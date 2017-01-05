#!/usr/bin/env python
# -*- coding: utf-8 -*- #
__author__ = 'Vonng (fengruohang@outlook.com)'

import json
import time, datetime
from dateutil.parser import parse
import happybase
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log

TS_CONVERTER = {
    20: lambda t: int(t),  # BIGINT -> int
    1082: lambda t: int(time.mktime(t.timetuple())) * 1000,  # DATE -> datetime.date
    1114: lambda t: int(time.mktime(t.timetuple())) * 1000,  # TIMESTAMP -> datetime.datetime
    1184: lambda t: int(time.mktime(parse(t).timetuple()) * 1000),  # TIMESTAMPTZ -> iso8601 unicode (WTF)
}

TS_RECONVERTER = {
    20: lambda t: int(t),  # BIGINT -> int
    1082: lambda t: datetime.date.fromtimestamp(t / 1000),  # DATE -> datetime.date
    1114: lambda t: datetime.date.fromtimestamp(t / 1000),  # TIMESTAMP -> datetime.datetime
    1184: lambda t: datetime.date.fromtimestamp(t / 1000),  # TIMESTAMPTZ -> iso8601 unicode (WTF)
}


class HappyBaseFdw(ForeignDataWrapper):
    def __init__(self, fdw_options, fdw_columns):
        super(HappyBaseFdw, self).__init__(fdw_options, fdw_columns)
        self.fdw_columns = fdw_columns
        self.fdw_options = fdw_options

        self.mode = fdw_options.get('mode', 'dev')
        self.host = fdw_options.get('host', 'localhost')
        self.port = int(fdw_options.get('port', '9090'))
        self.debug = fdw_options.get('debug', None) == 'True'
        self.table_name = fdw_options.get('table')
        self.prefix = fdw_options.get('prefix')

        if not self.table_name or not self.host:
            raise ValueError('[HB-FDW] Host and table should be specified!')

        self.qualifier = {}
        self.serializer = {}
        self.include_timestamp = False
        self.ts_converter = None
        self.ts_reconverter = None

        for col_name, col_def in fdw_columns.iteritems():
            if col_name == 'rowkey': continue
            if col_name == 'timestamp':
                self.include_timestamp = True
                self.ts_converter = TS_CONVERTER.get(col_def.type_oid)
                self.ts_reconverter = TS_RECONVERTER.get(col_def.type_oid)
                continue

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
            log("[HB-FDW] FDW Column Define ========================")
            for col_name, cd in fdw_columns.iteritems():
                log("[HB-FDW] %-12s\t[%d :%-s(%s:%s)] Opt:%s" % (
                    cd.column_name, cd.type_oid, cd.type_name, cd.base_type_name, cd.typmod, cd.options))

            log("[HB-FDW] FDW Options ===============================")
            for k, v in fdw_options.iteritems():
                log("[HB-FDW] %s:%s" % (k, v))

            log("[HB-FDW] Column Alias ==============================")
            for k, v in self.qualifier.iteritems():
                log("[HB-FDW] %12s\t%s" % (k, v))

        self.conn = happybase.Connection(self.host, self.port)
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

    def wrap(self, rowkey, response):
        '''wrap hbase result into postgres format'''
        buf = {"rowkey": rowkey}
        max_ts = 0
        if response:
            if self.include_timestamp:
                for col_name, qualifier in self.qualifier.iteritems():
                    value = response.get(qualifier)
                    if value:
                        value, ts = value
                        if ts > max_ts: max_ts = ts
                    buf[col_name] = value
                    buf["timestamp"] = self.ts_reconverter(max_ts)
            else:
                for col_name, qualifier in self.qualifier.iteritems():
                    value = response.get(qualifier)
                    if value:
                        buf[col_name] = value
        return buf

    def execute(self, quals, columns, sortkeys=None):
        """
        Query hbase
        :param quals:       list of qualification
        :param columns:     set of required columns
        :param sortkeys:    keys to be sored
        :return:            data dict
        """
        if self.debug:
            log("[HB-FDW] Query Begin ================================")
            log("[HB-FDW] Columns : %s" % columns)
            log("[HB-FDW] Quals   : %s" % quals)

        # Build rowkey: type of rowkey could be str, list, dict
        rowkey = None
        ts_max = None
        filter_str = None
        for qual in quals:
            if qual.field_name == 'rowkey':

                # single rowkey
                if qual.operator == '=':
                    rowkey = qual.value
                    if len(columns) == 1:  # rowkey only
                        yield {"rowkey": rowkey}
                        return

                # multiple rowkey
                elif qual.is_list_operator:
                    rowkey = qual.value
                    if len(columns) == 1:
                        for rk in rowkey:
                            yield {"rowkey": rowkey}

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
                    raise ValueError("[HB-FDW] Supported operators on rowkey : =,<=,>=,in,any,between")

            elif qual.field_name == 'timestamp':
                # lots of timestamp related function is not supported by happybase
                # Supported qual: <
                if qual.operator == '<':
                    timestamp = qual.value
                    if self.ts_converter:
                        timestamp = self.ts_converter(timestamp)
                    ts_max = timestamp

        # Build columns
        qualifiers = [self.qualifier[k] for k in columns if k != 'rowkey' and k != 'timestamp']

        # full table scan
        if not rowkey:
            for rk, response in self.table.scan(columns=qualifiers, filter=filter_str,
                                                include_timestamp=self.include_timestamp, timestamp=ts_max):
                yield self.wrap(rk, response)

        # single rowkey
        elif isinstance(rowkey, basestring):
            yield self.wrap(rowkey, self.table.row(rowkey, qualifiers, include_timestamp=self.include_timestamp,
                                                   timestamp=ts_max))

        # multiple rowkey
        elif isinstance(rowkey, list):
            for rk, response in self.table.rows(rowkey, qualifiers, include_timestamp=self.include_timestamp,
                                                timestamp=ts_max):
                yield self.wrap(rk, response)

        # range rowkey
        elif isinstance(rowkey, dict):
            for rk, response in self.table.scan(rowkey.get('since'), rowkey.get('until'), columns=qualifiers,
                                                filter=filter_str, include_timestamp=self.include_timestamp,
                                                timestamp=ts_max):
                yield self.wrap(rk, response)
        else:
            raise ValueError('[HB-FDW] Invalid rowkey')

    def update(self, rowkey, newvalues):
        if not rowkey: raise ValueError('[HB-FDW] rowkey should be specified! ')
        if self.debug:
            log('[HB-FDW] Update Begin: %s ================================' % rowkey)
            log(newvalues)

        payload = {self.qualifier.get(col_name): str(value)
                   for col_name, value in newvalues.iteritems() if
                   col_name != 'rowkey' and col_name != 'timestamp'}

        # Given new rowkey in update statement will make a new copy with new rowkey
        rowkey = newvalues.get('rowkey') or rowkey
        self.table.put(rowkey, payload)

    def insert(self, values):
        if self.debug:
            log('[HB-FDW] Insert Begin: ================================')
            log(values)

        rowkey = values.get('rowkey')
        if not rowkey:
            raise ValueError('[HB-FDW] rowkey should be specified!')
        timestamp = values.get('timestamp')
        if timestamp and self.ts_converter:
            timestamp = self.ts_converter(timestamp)

        payload = {self.qualifier.get(col_name): str(value)
                   for col_name, value in values.iteritems() if
                   col_name != 'rowkey' and col_name != 'timestamp'}

        self.table.delete(rowkey)
        self.table.put(rowkey, payload, timestamp=timestamp)

    def delete(self, rowkey):
        if not rowkey: raise ValueError('[HB-FDW] rowkey should be specified!')
        if self.debug:
            log("[HB-FDW] Delete Begin: %s ================================" % rowkey)
        self.table.delete(rowkey)
