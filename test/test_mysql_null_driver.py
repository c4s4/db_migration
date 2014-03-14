#!/usr/bin/env python
# encoding: UTF-8

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import unittest
import datetime
import db_migration


class TestMysqlNullDriver(unittest.TestCase):

    CONFIG = {
        'hostname': 'localhost',
        'database': 'test',
        'username': 'test',
        'password': 'test',
    }
    SCRIPT_DIR = os.path.dirname(__file__)

    def test_run_query_nominal(self):
        mysql = db_migration.MysqlNullDriver(configuration=self.CONFIG)
        result = mysql.run_query("SHOW DATABASES;")
        self.assertTrue('information_schema' in [entry['Database'] for entry in result])

    def test_run_query_error(self):
        mysql = db_migration.MysqlNullDriver(configuration=self.CONFIG)
        try:
            mysql.run_query("BAD SQL QUERY;")
            self.fail('Should have failed')
        except Exception, e:
            self.assertTrue("You have an error in your SQL syntax" in e.message)

    def test_run_script_nominal(self):
        script = os.path.join(self.SCRIPT_DIR, 'test_mysql_null_driver.sql')
        mysql = db_migration.MysqlNullDriver(configuration=self.CONFIG)
        result = mysql.run_script(script)
        self.assertTrue('information_schema' in [entry['Database'] for entry in result])

    def test_run_script_error(self):
        mysql = db_migration.MysqlNullDriver(configuration=self.CONFIG)
        try:
            mysql.run_script("script_that_doesnt_exist.sql")
            self.fail('Should have failed')
        except Exception, e:
            self.assertTrue("No such file or directory: 'script_that_doesnt_exist.sql'" in str(e))

    def test_run_script_syntax_error(self):
        script = os.path.join(self.SCRIPT_DIR, 'test_mysql_null_driver_error.sql')
        mysql = db_migration.MysqlNullDriver(configuration=self.CONFIG)
        try:
            mysql.run_script(script)
            self.fail('Should have failed')
        except Exception, e:
            self.assertTrue("You have an error in your SQL syntax" in e.message)

    def test_process_parameters(self):
        query = "%s %s %s"
        parameters = [1, 'deux', datetime.datetime(2014, 01, 22, 13, 10, 33)]
        expected = "1 'deux' '2014-01-22 13:10:33'"
        actual = db_migration.MysqlNullDriver.process_parameters(query, parameters)
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
