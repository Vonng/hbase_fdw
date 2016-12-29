# HBase FDW

### 简介
PostgreSQL FDW非常强大，可以以SQL的方式访问外部数据源。
Multicorn是一个允许使用Python编写FDW的Postgres扩展，极大的提高了FDW的开发效率，降低了FDW的开发门槛。

不过HBase本身是非常蛋疼的，有多种接口。
0.96以前的版本有thrift, thrift2, rest, java native接口。node.js也有一个原生的驱动。
0.96及以后HBase采用Protobuf序列化协议。


## 项目结构
* hbase_fdw里放着相关代码
* resource里有相关sql,主要是配置PostgreSQL的脚本.包括DDL等.
* test文件夹里放着单元测试.包括wrapper和api以及DB的测试.

### 快捷操作
在本目录下,可执行以下命令.
	sudo make link		链接当前目录至Python Path,便于调试
	sudo make install 	拷贝本程序至Python Path
	sudo make uninstall 卸载本程序

	sudo make setup		设置开发环境的PostgreSQL数据库
	sudo make test;		进行几项基本的pg查询,验证功能能否正常使用


