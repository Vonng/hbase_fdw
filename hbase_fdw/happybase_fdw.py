#!/usr/bin/env python
# -*- coding: utf-8 -*- #
__author__ = 'Vonng (fengruohang@outlook.com)'

import json
import happybase
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log


class HappyBaseFdw(ForeignDataWrapper):
    def __init__(self, fdw_options, fdw_columns):
        super(HappyBaseFdw, self).__init__(fdw_options, fdw_columns)
        self.fdw_columns = fdw_columns
        self.fdw_options = fdw_options

        self.mode = fdw_options.get('mode', 'dev')
        self.host = fdw_options.get('host')
        self.debug = fdw_options.get('debug', None) == 'True'
        self.table_name = fdw_options.get('table')
        self.prefix = fdw_options.get('prefix')

        if not self.table_name or not self.host:
            raise ValueError('host and table should be specified!')

        self.qualifier = {}
        self.serializer = {}
        for col_name, col_def in fdw_columns.iteritems():
            if col_name == 'rowkey': continue
            qualifier = col_def.options.get('qualifier')

            # Column family already specified
            if self.prefix:
                if qualifier:
                    qualifier = self.prefix + ':' + qualifier
                else:
                    qualifier = self.prefix + ':' + col_name
            else:
                if not qualifier:
                    qualifier = col_name.replace('_', ':', 1)

            self.qualifier[col_name] = qualifier
            serializer = col_def.options.get('serializer')
            self.serializer[col_name] = serializer

        if self.debug:
            log("-- Columns ------------------------------")
            log(fdw_options)
            for col_name, cd in fdw_columns.iteritems():
                log("%-12s\t[%4d :%-30s(%s:%s)] Opt:%s" % (
                    cd.column_name, cd.type_oid, cd.type_name, cd.base_type_name, cd.typmod, cd.options))
            log(self.qualifier)

        self.conn = None
        self.table = None

        self.conn = happybase.Connection(self.host)
        self.table = self.conn.table(self.table_name)

    def get_rel_size(self, quals, columns):
        """
        Estimate result size by conditions
        """
        for qual in quals:
            if qual.field_name == 'rowkey':
                # single rowkey
                if qual.operator == '=':
                    return (1, len(columns) * 100)

                # multiple rowkey
                elif qual.is_list_operator:
                    return (len(qual.value), len(columns) * 100)

                # range scan
                elif qual.operator == '<=' or qual.operator == '>=':
                    return (100000, len(columns) * 100)

        # Full table scan
        return (100000000, len(columns) * 100)

    @property
    def rowid_column(self):
        return 'rowkey'

    def wrap(self, payload):
        buf = {}
        if payload:
            for col_name, value in payload.iteritems():
                if col_name == 'rowkey': continue
                qualifier = self.qualifier.get(col_name)
                buf[qualifier] = str(value)
        return buf

    def unwrap(self, rowkey, response):
        '''unwrap hbase result into postgres format'''
        buf = {"rowkey": rowkey}
        if response:
            for col_name, qualifier in self.qualifier.iteritems():
                value = response.get(qualifier)
                buf[col_name] = value
        return buf

    def execute(self, quals, columns, sortkeys=None):
        '''Query hbase: Invoked by executor
            Usage:

        -- Single selection
        SELECT * FROM hbtest WHERE rowkey = '9c9e_2016-02-02_56444370e7e12af0561e221c';

        -- Multiple selection
        SELECT * FROM hbtest WHERE rowkey IN (
          '9c9e_2016-02-02_56444370e7e12af0561e221c',
          'd58c_2015-12-03_548935a4fd98c5d3510008bc',
          'b50d_2015-12-03_5506905ffd98c5ae1b0000de',
          'e18d_2015-12-03_559e9b1067e58e2cdd002509',
          '8545_2015-12-03_563b1f8f67e58e55580014d1',
          '1516_2015-12-03_56430770cc3e5975ca000012');

        -- Range Scan
        SELECT rowkey,active,install,launch FROM hbtest
        WHERE rowkey BETWEEN '9c9a' AND '9c9f' AND active > 0 and rowkey ~ '^.{4}_.{10}_\w{24}';
        '''
        if self.debug:
            log("-- Exec begin ------------------------------")
            log("-- Cols & Quals ------------------------------")
            log(columns)
            log(quals)
            for qual in quals:
                log("%s %s %s" % (qual.field_name, qual.operator, qual.value))

        # Build rowkey: type of rowkey could be str, list, dict
        rowkey = None

        filter_str = None
        for qual in quals:
            if qual.field_name == 'rowkey':

                # single rowkey
                if qual.operator == '=':
                    rowkey = qual.value

                # multiple rowkey
                elif qual.is_list_operator:
                    rowkey = qual.value

                # range low bound
                elif qual.operator == '<=':
                    if isinstance(rowkey, dict):
                        rowkey['until'] = qual.value.encode('utf-8')
                    else:
                        rowkey = {'until': qual.value.encode('utf-8')}

                # range high bound
                elif qual.operator == '>=':
                    if isinstance(rowkey, dict):
                        rowkey['since'] = qual.value
                    else:
                        rowkey = {'since': qual.value}
                # Regex like
                elif qual.operator == '~':
                    filter_str = "RowFilter(%s, 'regexstring:%s')" % ('=', qual.value)
                elif qual.operator == '!~':
                    filter_str = "RowFilter(%s, 'regexstring:%s')" % ('!=', qual.value)
                else:
                    log(qual)
                    raise ValueError("Supported operators on rowkey : =,<=,>=,in,any,between")

        # Build columns
        qualifiers = [self.qualifier[k] for k in columns if k != 'rowkey']

        # full table scan
        if not rowkey:
            for rk, response in self.table.scan(columns=qualifiers, filter=filter_str):
                yield self.unwrap(rk, response)

        # single rowkey
        elif isinstance(rowkey, basestring):
            yield self.unwrap(rowkey, self.table.row(rowkey, qualifiers))

        # multiple rowkey
        elif isinstance(rowkey, list):
            for rk, response in self.table.rows(rowkey, qualifiers):
                yield self.unwrap(rk, response)

        # range rowkey
        elif isinstance(rowkey, dict):
            for rk, response in self.table.scan(rowkey.get('since'), rowkey.get('until'), columns=qualifiers,
                                                filter=filter_str):
                yield self.unwrap(rk, response)
        else:
            raise ValueError('Invalid rowkey')

    def update(self, rowkey, newvalues):
        if not rowkey: raise ValueError('rowkey should be specified!')
        self.table.put(rowkey, self.wrap(newvalues))

    def insert(self, values):
        rowkey = values.get('rowkey')
        if not rowkey: raise ValueError('rowkey should be specified!')
        self.table.delete(rowkey)
        self.table.put(rowkey, self.wrap(values))

    def delete(self, rowkey):
        if not rowkey: raise ValueError('rowkey should be specified!')
        self.table.delete(rowkey)
