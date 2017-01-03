TARGET_DIR=/usr/local/anaconda/lib/python2.7/site-packages
MODULE_NAME=hbase_fdw

PROJECT_PATH=`pwd`
MODULE_PATH=$(PROJECT_PATH)/$(MODULE_NAME)
TARGET_PATH=$(TARGET_DIR)/$(MODULE_NAME)

link :
	ln -s $(MODULE_PATH) $(TARGET_PATH)

unlink :
	rm -rf $(TARGET_PATH)


install :
	cp -r $(MODULE_PATH) $(TARGET_PATH)

uninstall :
	rm -rf $(TARGET_PATH)

show :
	ls -ald $(TARGET_PATH)
	ls -al $(TARGET_PATH)

ls : show

setup :
	psql elysium < resource/hbase_fdw_setup.sql --echo-all

test :
	psql elysium < resource/hbase_fdw_test.sql

clean :
	rm -rf *.pyc
	rm -rf hbase_fdw/*.pyc

.PHONY: link unlink uninstall install show run clean setup test
