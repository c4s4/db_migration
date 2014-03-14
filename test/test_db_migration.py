#!/usr/bin/env python
# encoding: UTF-8

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import unittest
import db_migration


class TestDBMigration(unittest.TestCase):

    DB_CONFIG = {
        'hostname': 'localhost',
        'database': 'test',
        'username': 'test',
        'password': 'test',
    }
    MYSQL = db_migration.MysqlNullDriver(configuration=DB_CONFIG)
    SCRIPT_DIR = os.path.dirname(__file__)
    ROOT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, '..'))
    CONFIG_FILE = os.path.join(SCRIPT_DIR, '..', 'sql', 'db_configuration.py')

    ###########################################################################
    #                                UTILITIES                                #
    ###########################################################################

    def dump_actual(self):
        #pylint: disable=E1121
        output_file = os.path.join(self.ROOT_DIR, 'build', 'actual.sql')
        return db_migration.DBMigration.execute("mysqldump -d -h%s -u%s -p%s test pet > %s" %
               (self.DB_CONFIG['hostname'], self.DB_CONFIG['username'], self.DB_CONFIG['password'], output_file))

    @staticmethod
    def strip_query(query):
        stripped = ''
        margin = None
        for line in query.split('\n'):
            if len(line.strip()) > 0:
                if not margin:
                    margin = 0
                    while line[0] == ' ':
                        margin += 1
                        line = line[1:]
                    stripped += line + '\n'
                else:
                    stripped += line[margin:] + '\n'
        return stripped.strip()

    def dump_expected(self, expected):
        dump_file = os.path.join(self.ROOT_DIR, 'build', 'expected.sql')
        f = open(dump_file, 'w')
        try:
            f.write(self.strip_query(expected))
        finally:
            f.close()

    @staticmethod
    def run_db_migration(options):
        db_migration.DBMigration.parse_command_line(options).run()
        # script = os.path.join(self.ROOT_DIR, 'src', 'db_migration.py')
        # command = "python %s %s" % (script, options)
        # db_migration.DBMigration.execute(command)

    def assert_schema(self, expected):
        self.dump_expected(expected)
        self.dump_actual()
        script = os.path.join(self.ROOT_DIR, 'test', 'compdb.py')
        expected_file = os.path.join(self.ROOT_DIR, 'build', 'expected.sql')
        actual_file = os.path.join(self.ROOT_DIR, 'build', 'actual.sql')
        diff_file = os.path.join(self.ROOT_DIR, 'build', 'diff.sql')
        command = "python %s %s %s > %s" % (script, expected_file, actual_file, diff_file)
        db_migration.DBMigration.execute(command)
        if os.path.getsize(diff_file) > 0:
            raise Exception("Schema is not as expected")

    def assert_data(self, expected):
        actual = self.MYSQL.run_query("SELECT * FROM test.pet ORDER BY id")
        if expected != actual:
            print "%s != %s" % (repr(expected), repr(actual))
            raise Exception("Data are not as expected")

    ###########################################################################
    #                                 TESTS                                   #
    ###########################################################################

    def setUp(self):
        try:
            os.makedirs(os.path.join(self.ROOT_DIR, 'build'))
        except Exception:
            pass
        self.MYSQL.run_query("DROP TABLE IF EXISTS test._scripts")
        self.MYSQL.run_query("DROP TABLE IF EXISTS test._install")
        self.MYSQL.run_query("DROP TABLE IF EXISTS test.pet")

    def test_init_nominal(self):
        self.run_db_migration(['-ilu',
                               '-c', '%s/sql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/sql' % self.ROOT_DIR,
                               'itg', '0.1'])
        EXPECTED_SCHEMA = """
        DROP TABLE IF EXISTS `pet`;
        CREATE TABLE `pet` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(20) NOT NULL,
            `age` int(11) NOT NULL,
            `species` varchar(10) NOT NULL,
            `tatoo` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        self.assert_schema(EXPECTED_SCHEMA)
        EXPECTED_DATA = (
            {'species': 'dog', 'tatoo': '2-GKB-951', 'age': '14', 'id': '1', 'name': 'Réglisse'},
            {'species': 'cat', 'tatoo': 'NULL', 'age': '13', 'id': '2', 'name': 'Mignonne'},
            {'species': 'cat', 'tatoo': 'NULL', 'age': '19', 'id': '3', 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_init_version(self):
        self.run_db_migration(['-ilu',
                               '-c', '%s/sql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/sql' % self.ROOT_DIR,
                               'itg', '0.0'])
        EXPECTED_SCHEMA = """
        DROP TABLE IF EXISTS `pet`;
        CREATE TABLE `pet` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(20) NOT NULL,
            `age` int(11) NOT NULL,
            `species` varchar(10) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        self.assert_schema(EXPECTED_SCHEMA)
        EXPECTED_DATA = (
            {'species': 'dog', 'age': '14', 'id': '1', 'name': 'Réglisse'},
            {'species': 'cat', 'age': '13', 'id': '2', 'name': 'Mignonne'},
            {'species': 'cat', 'age': '19', 'id': '3', 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_init_prod(self):
        self.run_db_migration(['-ilu',
                               '-c', '%s/sql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/sql' % self.ROOT_DIR,
                               'prod', '0.1'])
        EXPECTED_SCHEMA = """
        DROP TABLE IF EXISTS `pet`;
        CREATE TABLE `pet` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(20) NOT NULL,
            `age` int(11) NOT NULL,
            `species` varchar(10) NOT NULL,
            `tatoo` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        self.assert_schema(EXPECTED_SCHEMA)
        EXPECTED_DATA = (
            {'species': 'dog', 'tatoo': 'NULL', 'age': '6', 'id': '1', 'name': 'Milou'},
            {'species': 'dog', 'tatoo': 'NULL', 'age': '11', 'id': '2', 'name': 'Médor'},
            {'species': 'cat', 'tatoo': 'NULL', 'age': '10', 'id': '3', 'name': 'Félix'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_migrate_nominal(self):
        # initialize database in version 0.0
        self.run_db_migration(['-ilu',
                               '-c', '%s/sql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/sql' % self.ROOT_DIR,
                               'itg', '0.0'])
        EXPECTED_SCHEMA = """
        DROP TABLE IF EXISTS `pet`;
        CREATE TABLE `pet` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(20) NOT NULL,
            `age` int(11) NOT NULL,
            `species` varchar(10) NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        self.assert_schema(EXPECTED_SCHEMA)
        EXPECTED_DATA = (
            {'species': 'dog', 'age': '14', 'id': '1', 'name': 'Réglisse'},
            {'species': 'cat', 'age': '13', 'id': '2', 'name': 'Mignonne'},
            {'species': 'cat', 'age': '19', 'id': '3', 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)
        # migrate database to version 1.0
        self.run_db_migration(['-lu',
                               '-c', '%s/sql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/sql' % self.ROOT_DIR,
                               'itg', '1.0'])
        EXPECTED_SCHEMA = """
        DROP TABLE IF EXISTS `pet`;
        CREATE TABLE `pet` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(20) NOT NULL,
            `age` int(11) NOT NULL,
            `species` varchar(10) NOT NULL,
            `tatoo` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        self.assert_schema(EXPECTED_SCHEMA)
        EXPECTED_DATA = (
            {'species': 'dog', 'tatoo': '2-GKB-951', 'age': '14', 'id': '1', 'name': 'Réglisse'},
            {'species': 'cat', 'tatoo': 'NULL', 'age': '13', 'id': '2', 'name': 'Mignonne'},
            {'species': 'cat', 'tatoo': 'NULL', 'age': '19', 'id': '3', 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_command_line_options(self):
        try:
            db_migration.DBMigration.parse_command_line(('-c', self.CONFIG_FILE))
            self.fail('Should have failed')
        except db_migration.AppException, e:
            self.assertTrue('Must pass platform on command line' in e.message)
        try:
            db_migration.DBMigration.parse_command_line(('foo', 'bar', 'spam'))
            self.fail('Should have failed')
        except db_migration.AppException, e:
            self.assertTrue('Too many arguments on command line' in e.message)
        try:
            db_migration.DBMigration.parse_command_line(('-c', self.CONFIG_FILE, '-a', 'itg', '1.2.3'))
            self.fail('Should have failed')
        except db_migration.AppException, e:
            self.assertTrue("You can't give a version with -a option" in e.message)
        try:
            db_migration.DBMigration.parse_command_line(('-c', self.CONFIG_FILE, 'foo'))
            self.fail('Should have failed')
        except db_migration.AppException, e:
            self.assertTrue("Platform must be one of" in e.message)
        try:
            db_migration.DBMigration.parse_command_line(('-c', self.CONFIG_FILE, '-d', '-m', '1.0.0', 'itg', '1.2.3'))
            self.fail('Should have failed')
        except db_migration.AppException, e:
            self.assertTrue("Migration script generation is incompatible with options dry_run local and dump" in e.message)

    def test_split_version(self):
        self.assertEqual([1, 2, 3], db_migration.DBMigration.split_version('1.2.3'))
        self.assertEqual([1, 2, 0], db_migration.DBMigration.split_version('1.2'))
        self.assertEqual([1, 0, 0], db_migration.DBMigration.split_version('1'))
        self.assertEqual(None, db_migration.DBMigration.split_version('init'))


if __name__ == '__main__':
    unittest.main()
