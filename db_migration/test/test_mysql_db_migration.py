#!/usr/bin/env python
# encoding: UTF-8

import os
import unittest

import sys
from StringIO import StringIO

import db_migration


class TestDBMigration(unittest.TestCase):

    DB_CONFIG = {
        'hostname': 'localhost',
        'database': 'test',
        'username': 'test',
        'password': 'test',
    }
    ENCODING = 'utf8'
    MYSQL = db_migration.MysqlCommando(configuration=DB_CONFIG, encoding=ENCODING)
    SCRIPT_DIR = os.path.dirname(__file__)
    ROOT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, '..', '..'))
    CONFIG_FILE = os.path.join(SCRIPT_DIR, 'sql', 'mysql', 'db_configuration.py')

    ###########################################################################
    #                                UTILITIES                                #
    ###########################################################################

    @staticmethod
    def run_db_migration(options):
        old_stdout = sys.stdout
        sys.stdout = output = StringIO()
        try:
            db_migration.DBMigration.parse_command_line(options).run()
            return output.getvalue()
        finally:
            sys.stdout = old_stdout

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

    def dump_actual(self):
        #pylint: disable=E1121
        output_file = os.path.join(self.ROOT_DIR, 'build', 'actual.sql')
        return db_migration.DBMigration.\
            execute("mysqldump -d -h%s -u%s -p%s test pet > %s" %
                    (self.DB_CONFIG['hostname'],
                     self.DB_CONFIG['username'],
                     self.DB_CONFIG['password'],
                     output_file))

    def assert_schema(self, expected):
        self.dump_expected(expected)
        self.dump_actual()
        script = os.path.join(self.ROOT_DIR, 'db_migration', 'test', 'compdb.py')
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

    def test_split_version(self):
        self.assertEqual([1, 2, 3, 4], db_migration.Script.split_version('1.2.3.4'))
        self.assertEqual([1, 2, 3, 4], db_migration.Script.split_version('01.02.03.04'))
        self.assertEqual(db_migration.Script.VERSION_INIT, db_migration.Script.split_version('init'))
        self.assertEqual(db_migration.Script.VERSION_NEXT, db_migration.Script.split_version('next'))

    def test_init_nominal(self):
        self.run_db_migration(['-ilu',
                               '-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
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
            {'species': 'dog', 'tatoo': '2-GKB-951', 'age': 14, 'id': 1, 'name': 'Réglisse'},
            {'species': 'cat', 'tatoo': None,        'age': 13, 'id': 2, 'name': 'Mignonne'},
            {'species': 'cat', 'tatoo': None,        'age': 19, 'id': 3, 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_init_version(self):
        self.run_db_migration(['-ilu',
                               '-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
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
            {'species': 'dog', 'age': 14, 'id': 1, 'name': 'Réglisse'},
            {'species': 'cat', 'age': 13, 'id': 2, 'name': 'Mignonne'},
            {'species': 'cat', 'age': 19, 'id': 3, 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_init_prod(self):
        self.run_db_migration(['-ilu',
                               '-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
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
            {'species': 'dog', 'tatoo': None, 'age': 6,  'id': 1, 'name': 'Milou'},
            {'species': 'dog', 'tatoo': None, 'age': 11, 'id': 2, 'name': 'Médor'},
            {'species': 'cat', 'tatoo': None, 'age': 10, 'id': 3, 'name': 'Félix'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_migrate_nominal(self):
        # initialize database in version 0.0
        self.run_db_migration(['-ilu',
                               '-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
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
            {'species': 'dog', 'age': 14, 'id': 1, 'name': 'Réglisse'},
            {'species': 'cat', 'age': 13, 'id': 2, 'name': 'Mignonne'},
            {'species': 'cat', 'age': 19, 'id': 3, 'name': 'Ophélie'},
        )
        self.assert_data(EXPECTED_DATA)
        # migrate database to version 1.0
        self.run_db_migration(['-lu',
                               '-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
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
            {'species': 'dog',    'tatoo': '2-GKB-951', 'age': 14, 'id': 1, 'name': 'Réglisse'},
            {'species': 'cat',    'tatoo': None,        'age': 13, 'id': 2, 'name': 'Mignonne'},
            {'species': 'cat',    'tatoo': None,        'age': 19, 'id': 3, 'name': 'Ophélie'},
            {'species': 'beaver', 'tatoo': None,        'age': 7, 'id': 4, 'name': 'Nico'},
        )
        self.assert_data(EXPECTED_DATA)

    def test_migration_script_mysql(self):
        # nominal case
        expected = '''-- Migration base 'test' on platform 'itg'
-- From version '0.1' to '1.0'
USE `test`;

-- Script '1.0/all.sql'
INSERT INTO pet
  (name, age, species)
VALUES
  ('Nico', 7, 'beaver');

COMMIT;
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-m', '0.1', 'itg', '1.0'])
        self.assertEquals(expected, actual)
        # another nominal case
        expected = '''-- Migration base 'test' on platform 'itg'
-- From version '0' to '1.0'
USE `test`;

-- Script '0.1/all.sql'
ALTER TABLE pet ADD tatoo VARCHAR(20);

-- Script '0.1/itg.sql'
UPDATE pet SET tatoo='2-GKB-951' WHERE NAME='Réglisse';

-- Script '1.0/all.sql'
INSERT INTO pet
  (name, age, species)
VALUES
  ('Nico', 7, 'beaver');

COMMIT;
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-m', '0', 'itg', '1.0'])
        self.assertEquals(expected, actual)
        # nominal case from init
        expected = '''-- Migration base 'test' on platform 'itg'
-- From version 'init' to '1.0'
USE `test`;

-- Script 'init/all.sql'
DROP TABLE IF EXISTS pet;

CREATE TABLE pet (
  id INTEGER NOT NULL AUTO_INCREMENT,
  name VARCHAR(20) NOT NULL,
  age INTEGER NOT NULL,
  species VARCHAR(10) NOT NULL,
  PRIMARY KEY  (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Script 'init/itg.sql'
INSERT INTO pet
  (name, age, species)
VALUES
  ('Réglisse', 14, 'dog'),
  ('Mignonne', 13, 'cat'),
  ('Ophélie', 19, 'cat');

-- Script '0.1/all.sql'
ALTER TABLE pet ADD tatoo VARCHAR(20);

-- Script '0.1/itg.sql'
UPDATE pet SET tatoo='2-GKB-951' WHERE NAME='Réglisse';

-- Script '1.0/all.sql'
INSERT INTO pet
  (name, age, species)
VALUES
  ('Nico', 7, 'beaver');

COMMIT;
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-m', 'init', 'itg', '1.0'])
        self.assertEquals(expected, actual)

    def test_migration_script_oracle(self):
        # nominal case
        expected = '''-- Migration base 'orcl' on platform 'itg'
-- From version '0.1' to '1.0'
WHENEVER SQLERROR EXIT SQL.SQLCODE;
WHENEVER OSERROR EXIT 9;

-- Script '1.0/all.sql'
INSERT INTO pet (ID, NAME, AGE, SPECIES) VALUES (PET_SEQUENCE.nextval, 'Nico', 7, 'beaver');

COMMIT;
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/oracle/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/oracle' % self.ROOT_DIR,
                                        '-m', '0.1', 'itg', '1.0'])
        self.assertEquals(expected, actual)
        # another nominal case
        expected = '''-- Migration base 'orcl' on platform 'itg'
-- From version '0' to '1.0'
WHENEVER SQLERROR EXIT SQL.SQLCODE;
WHENEVER OSERROR EXIT 9;

-- Script '0.1/all.sql'
ALTER TABLE PET ADD TATOO VARCHAR(20);

-- Script '0.1/itg.sql'
UPDATE PET SET TATOO='2-GKB-951' WHERE NAME='Réglisse';

-- Script '1.0/all.sql'
INSERT INTO pet (ID, NAME, AGE, SPECIES) VALUES (PET_SEQUENCE.nextval, 'Nico', 7, 'beaver');

COMMIT;
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/oracle/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/oracle' % self.ROOT_DIR,
                                        '-m', '0', 'itg', '1.0'])
        self.assertEquals(expected, actual)
        # nominal case from init
        expected = '''-- Migration base 'orcl' on platform 'itg'
-- From version 'init' to '1.0'
WHENEVER SQLERROR EXIT SQL.SQLCODE;
WHENEVER OSERROR EXIT 9;

-- Script 'init/all.sql'
-- clean schema
BEGIN
  FOR cur_rec IN (SELECT object_name, object_type
                  FROM   user_objects
                  WHERE  object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'PROCEDURE', 'FUNCTION', 'SEQUENCE')) LOOP
    BEGIN
      IF cur_rec.object_name = 'INSTALL_' OR cur_rec.object_name = 'SCRIPTS_' OR
         cur_rec.object_name = 'INSTALL_SEQUENCE' THEN
        CONTINUE;
      end IF;
      IF cur_rec.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '" CASCADE CONSTRAINTS';
      ELSE
        EXECUTE IMMEDIATE 'DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '"';
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.put_line('FAILED: DROP ' || cur_rec.object_type || ' "' || cur_rec.object_name || '"');
    END;
  END LOOP;
END;
/
-- create table PET
CREATE TABLE PET (
  ID NUMBER(10) NOT NULL,
  NAME VARCHAR(20) NOT NULL,
  AGE NUMBER(2) NOT NULL,
  SPECIES VARCHAR(10) NOT NULL,
  PRIMARY KEY  (id)
);
CREATE SEQUENCE PET_SEQUENCE
  START WITH 1
  INCREMENT BY 1
  CACHE 100;

-- Script 'init/itg.sql'
INSERT INTO pet (ID, NAME, AGE, SPECIES) VALUES (PET_SEQUENCE.nextval, 'Réglisse', 14, 'dog');
INSERT INTO pet (ID, NAME, AGE, SPECIES) VALUES (PET_SEQUENCE.nextval, 'Mignonne', 13, 'cat');
INSERT INTO pet (ID, NAME, AGE, SPECIES) VALUES (PET_SEQUENCE.nextval, 'Ophélie', 19, 'cat');

-- Script '0.1/all.sql'
ALTER TABLE PET ADD TATOO VARCHAR(20);

-- Script '0.1/itg.sql'
UPDATE PET SET TATOO='2-GKB-951' WHERE NAME='Réglisse';

-- Script '1.0/all.sql'
INSERT INTO pet (ID, NAME, AGE, SPECIES) VALUES (PET_SEQUENCE.nextval, 'Nico', 7, 'beaver');

COMMIT;
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/oracle/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/oracle' % self.ROOT_DIR,
                                        '-m', 'init', 'itg', '1.0'])
        self.assertEquals(expected, actual)

    def test_dry_run(self):
        expected = '''Version 'all' on platform 'localhost'
Using base 'test' as user 'test'
Writing meta tables... OK
SQL scripts to run:
- init/all.sql
- init/itg.sql
- 0.1/all.sql
- 0.1/itg.sql
- 1.0/all.sql
- next/all.sql
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-d', '-i', '-a', 'itg'])
        self.assertEquals(expected, actual)
        expected = '''Version '1.0' on platform 'localhost'
Using base 'test' as user 'test'
Writing meta tables... OK
SQL scripts to run:
- init/all.sql
- init/itg.sql
- 0.1/all.sql
- 0.1/itg.sql
- 1.0/all.sql
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-d', '-i', 'itg', '1.0'])
        self.assertEquals(expected, actual)
        expected = '''Version '0.1' on platform 'localhost'
Using base 'test' as user 'test'
Writing meta tables... OK
SQL scripts to run:
- init/all.sql
- init/itg.sql
- 0.1/all.sql
- 0.1/itg.sql
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-d', '-i', 'itg', '0.1'])
        self.assertEquals(expected, actual)
        self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                               '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                               '-i', 'itg', '0.1'])
        expected = '''Version '1.0' on platform 'localhost'
Using base 'test' as user 'test'
Writing meta tables... OK
SQL scripts to run:
- 1.0/all.sql
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-d', 'itg', '1.0'])
        self.assertEquals(expected, actual)
        expected = '''Version 'all' on platform 'localhost'
Using base 'test' as user 'test'
Writing meta tables... OK
SQL scripts to run:
- 1.0/all.sql
- next/all.sql
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-d', '-a', 'itg'])
        self.assertEquals(expected, actual)
        expected = '''Version 'all' on platform 'localhost'
Using base 'test' as user 'test'
Writing meta tables... OK
SQL scripts to run:
- init/all.sql
- init/itg.sql
- 0.1/all.sql
- 0.1/itg.sql
- 1.0/all.sql
- next/all.sql
'''
        actual = self.run_db_migration(['-c', '%s/db_migration/test/sql/mysql/db_configuration.py' % self.ROOT_DIR,
                                        '-s', '%s/db_migration/test/sql/mysql' % self.ROOT_DIR,
                                        '-d', '-i', '-a', 'itg'])
        self.assertEquals(expected, actual)

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
            self.assertTrue("Migration script generation is incompatible with options dry_run and local" in e.message)


if __name__ == '__main__':
    unittest.main()
