#!/usr/bin/env python
# encoding: UTF-8

from __future__ import with_statement
import os
import re
import sys
import glob
import getopt
import getpass
import datetime
import subprocess
import HTMLParser


#pylint: disable=E1103
class MysqlCommando(object):

    ISO_FORMAT = '%Y-%m-%d %H:%M:%S'
    CASTS = (
        (r'-?\d+', int),
        (r'-?\d*\.?\d*([Ee][+-]?\d+)?', float),
        (r'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d', lambda d: datetime.datetime.strptime(d, MysqlCommando.ISO_FORMAT)),
        (r'NULL', lambda d: None),
    )
    QUERY_LAST_INSERT_ID = """
    ;SELECT last_insert_id() as last_insert_id;
    """

    def __init__(self, configuration=None,
                 hostname=None, database=None,
                 username=None, password=None,
                 encoding=None, cast=True):
        if hostname and database and username and password:
            self.hostname = hostname
            self.database = database
            self.username = username
            self.password = password
            if encoding:
                self.encoding = encoding
            else:
                self.encoding = None
        elif configuration:
            self.hostname = configuration['hostname']
            self.database = configuration['database']
            self.username = configuration['username']
            self.password = configuration['password']
            if 'encoding' in configuration:
                self.encoding = configuration['encoding']
            else:
                self.encoding = None
        else:
            raise MysqlException('Missing database configuration')
        self.cast = cast

    def run_query(self, query, parameters=None, cast=None,
                  last_insert_id=False):
        query = self._process_parameters(query, parameters)
        if last_insert_id:
            query += self.QUERY_LAST_INSERT_ID
        if self.encoding:
            command = ['mysql',
                       '-u%s' % self.username,
                       '-p%s' % self.password,
                       '-h%s' % self.hostname,
                       '--default-character-set=%s' % self.encoding,
                       '-B', '-e', query, self.database]
        else:
            command = ['mysql',
                       '-u%s' % self.username,
                       '-p%s' % self.password,
                       '-h%s' % self.hostname,
                       '-B', '-e', query, self.database]
        output = self._execute_with_output(command)
        if cast is None:
            cast = self.cast
        if output:
            result = self._output_to_result(output, cast=cast)
            if last_insert_id:
                return int(result[0]['last_insert_id'])
            else:
                return result

    def run_script(self, script, cast=None):
        if self.encoding:
            command = ['mysql',
                       '-u%s' % self.username,
                       '-p%s' % self.password,
                       '-h%s' % self.hostname,
                       '--default-character-set=%s' % self.encoding,
                       '-B', self.database]
        else:
            command = ['mysql',
                       '-u%s' % self.username,
                       '-p%s' % self.password,
                       '-h%s' % self.hostname,
                       '-B', self.database]
        if cast is None:
            cast = self.cast
        with open(script) as stdin:
            output = self._execute_with_output(command, stdin=stdin)
        if output:
            return self._output_to_result(output, cast=cast)

    def _output_to_result(self, output, cast):
        result = []
        lines = output.strip().split('\n')
        fields = lines[0].split('\t')
        for line in lines[1:]:
            values = line.split('\t')
            if cast:
                values = MysqlCommando._cast_list(values)
            result.append(dict(zip(fields, values)))
        return tuple(result)

    @staticmethod
    def _cast_list(values):
        return [MysqlCommando._cast(value) for value in values]

    @staticmethod
    def _cast(value):
        for regexp, function in MysqlCommando.CASTS:
            if re.match("^%s$" % regexp, value):
                return function(value)
        return value

    @staticmethod
    def _execute_with_output(command, stdin=None):
        if stdin:
            process = subprocess.Popen(command, stdout=subprocess.PIPE,  stderr=subprocess.PIPE, stdin=stdin)
        else:
            process = subprocess.Popen(command, stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
        output, errput = process.communicate()
        if process.returncode != 0:
            raise MysqlException(errput.strip())
        return output

    @staticmethod
    def _process_parameters(query, parameters):
        if not parameters:
            return query
        if isinstance(parameters, (list, tuple)):
            parameters = tuple(MysqlCommando._format_parameters(parameters))
        elif isinstance(parameters, dict):
            parameters = dict(zip(parameters.keys(), MysqlCommando._format_parameters(parameters.values())))
        return query % parameters

    @staticmethod
    def _format_parameters(parameters):
        return [MysqlCommando._format_parameter(param) for param in parameters]

    @staticmethod
    def _format_parameter(parameter):
        if isinstance(parameter, (int, long, float)):
            return str(parameter)
        elif isinstance(parameter, (str, unicode)):
            return "'%s'" % MysqlCommando._escape_string(parameter)
        elif isinstance(parameter, datetime.datetime):
            return "'%s'" % parameter.strftime(MysqlCommando.ISO_FORMAT)
        elif isinstance(parameter, list):
            return "(%s)" % ', '.join([MysqlCommando._format_parameter(e) for e in parameter])
        elif parameter is None:
            return "NULL"
        else:
            raise MysqlException("Type '%s' is not managed as a query parameter" % parameter.__class__.__name__)

    @staticmethod
    def _escape_string(string):
        return string.replace("'", "''")


# pylint: disable=W0231
class MysqlException(Exception):

    def __init__(self, message, query=None):
        self.message = message
        self.query = query

    def __str__(self):
        return self.message#pylint: disable=E1103


class SqlplusCommando(object):

    CATCH_ERRORS = "WHENEVER SQLERROR EXIT SQL.SQLCODE;\nWHENEVER OSERROR EXIT 9;\n"
    EXIT_COMMAND = "\ncommit;\nexit;\n"
    ISO_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, configuration=None,
                 hostname=None, database=None,
                 username=None, password=None,
                 cast=True):
        if hostname and database and username and password:
            self.hostname = hostname
            self.database = database
            self.username = username
            self.password = password
        elif configuration:
            self.hostname = configuration['hostname']
            self.database = configuration['database']
            self.username = configuration['username']
            self.password = configuration['password']
        else:
            raise SqlplusException('Missing database configuration')
        self.cast = cast

    def run_query(self, query, parameters={}, cast=True,
                  check_unknown_command=True):
        if parameters:
            query = self._process_parameters(query, parameters)
        query = self.CATCH_ERRORS + query
        session = subprocess.Popen(['sqlplus', '-S', '-L', '-M', 'HTML ON',
                                    self._get_connection_url()],
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        session.stdin.write(query)
        output, _ = session.communicate(self.EXIT_COMMAND)
        code = session.returncode
        if code != 0:
            raise SqlplusException(SqlplusErrorParser.parse(output), query)
        else:
            if output:
                result = SqlplusResultParser.parse(output, cast=cast,
                                                  check_unknown_command=check_unknown_command)
                return result

    def run_script(self, script, cast=True, check_unknown_command=True):
        if not os.path.isfile(script):
            raise SqlplusException("Script '%s' was not found" % script)
        with open(script) as stream:
            source = stream.read()
        return self.run_query(query=source, cast=cast, check_unknown_command=check_unknown_command)

    def _get_connection_url(self):
        return "%s/%s@%s/%s" % \
               (self.username, self.password, self.hostname, self.database)

    @staticmethod
    def _process_parameters(query, parameters):
        if not parameters:
            return query
        if isinstance(parameters, (list, tuple)):
            parameters = tuple(SqlplusCommando._format_parameters(parameters))
        elif isinstance(parameters, dict):
            values = SqlplusCommando._format_parameters(parameters.values())
            parameters = dict(zip(parameters.keys(), values))
        return query % parameters

    @staticmethod
    def _format_parameters(parameters):
        return [SqlplusCommando._format_parameter(param) for
                param in parameters]

    @staticmethod
    def _format_parameter(parameter):
        if isinstance(parameter, (int, long, float)):
            return str(parameter)
        elif isinstance(parameter, (str, unicode)):
            return "'%s'" % SqlplusCommando._escape_string(parameter)
        elif isinstance(parameter, datetime.datetime):
            return "'%s'" % parameter.strftime(SqlplusCommando.ISO_FORMAT)
        elif isinstance(parameter, list):
            return "(%s)" % ', '.join([SqlplusCommando._format_parameter(e)
                                       for e in parameter])
        elif parameter is None:
            return "NULL"
        else:
            raise SqlplusException("Type '%s' is not managed as a query parameter" %
                                   parameter.__class__.__name__)

    @staticmethod
    def _escape_string(string):
        return string.replace("'", "''")


class SqlplusResultParser(HTMLParser.HTMLParser):

    DATE_FORMAT = '%d/%m/%y %H:%M:%S'
    UNKNOWN_COMMAND = 'SP2-0734: unknown command'
    CASTS = (
        (r'-?\d+', int),
        (r'-?\d*,?\d*([Ee][+-]?\d+)?', lambda f: float(f.replace(',', '.'))),
        (r'\d\d/\d\d/\d\d \d\d:\d\d:\d\d,\d*',
         lambda d: datetime.datetime.strptime(d[:17],
                                              SqlplusResultParser.DATE_FORMAT)),
        (r'NULL', lambda d: None),
    )

    def __init__(self, cast):
        HTMLParser.HTMLParser.__init__(self)
        self.cast = cast
        self.active = False
        self.result = []
        self.fields = []
        self.values = []
        self.header = True
        self.data = ''

    @staticmethod
    def parse(source, cast, check_unknown_command):
        if not source.strip():
            return ()
        if SqlplusResultParser.UNKNOWN_COMMAND in source and check_unknown_command:
            raise SqlplusException(SqlplusErrorParser.parse(source))
        parser = SqlplusResultParser(cast)
        parser.feed(source)
        return tuple(parser.result)

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.active = True
        elif self.active:
            if tag == 'th':
                self.header = True
            elif tag == 'td':
                self.header = False

    def handle_endtag(self, tag):
        if tag == 'table':
            self.active = False
        elif self.active:
            if tag == 'tr' and not self.header:
                row = dict(zip(self.fields, self.values))
                self.result.append(row)
                self.values = []
            elif tag == 'th':
                self.fields.append(self.data.strip())
                self.data = ''
            elif tag == 'td':
                data = self.data.strip()
                if self.cast:
                    data = self._cast(data)
                self.values.append(data)
                self.data = ''

    def handle_data(self, data):
        if self.active:
            self.data += data

    @staticmethod
    def _cast(value):
        for regexp, function in SqlplusResultParser.CASTS:
            if re.match("^%s$" % regexp, value):
                return function(value)
        return value


class SqlplusErrorParser(HTMLParser.HTMLParser):

    UNKNOWN_COMMAND = 'SP2-0734: unknown command'

    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self.active = False
        self.message = ''

    @staticmethod
    def parse(source):
        parser = SqlplusErrorParser()
        parser.feed(source)
        return '\n'.join([l for l in parser.message.split('\n') if l.strip() != ''])

    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.active = True

    def handle_endtag(self, tag):
        if tag == 'body':
            self.active = False

    def handle_data(self, data):
        if self.active:
            self.message += data


# pylint: disable=W0231
class SqlplusException(Exception):

    def __init__(self, message, query=None):
        self.message = message
        self.query = query

    def __str__(self):
        return self.message


class MysqlMetaManager(object):

    SQL_CREATE_META_INSTALL = """CREATE TABLE IF NOT EXISTS _install (
      id integer NOT NULL AUTO_INCREMENT,
      version varchar(20) NOT NULL,
      start_date datetime NOT NULL,
      end_date datetime,
      success tinyint NOT NULL,
      PRIMARY KEY (id)
    )"""
    SQL_CREATE_META_SCRIPTS = """CREATE TABLE IF NOT EXISTS _scripts (
      filename varchar(255) NOT NULL,
      install_date datetime NOT NULL,
      success tinyint NOT NULL,
      install_id integer NOT NULL,
      error_message text,
      CONSTRAINT fk_install_id
        FOREIGN KEY (install_id)
        REFERENCES _install(id)
    )"""
    SQL_DROP_META_INSTALL = """DROP TABLE IF EXISTS _install"""
    SQL_DROP_META_SCRIPTS = """DROP TABLE IF EXISTS _scripts"""
    SQL_INSTALL_BEGIN = """INSERT INTO _install
    (version, start_date, end_date, success)
    VALUES (%(version)s, now(), null, 0);
    SELECT last_insert_id() AS ID;"""
    SQL_INSTALL_DONE = """UPDATE _install
    SET end_date = now(), success = %(success)s WHERE id = %(install_id)s"""
    SQL_SCRIPT_INSTALL = """INSERT INTO _scripts
    (filename, install_date, success, install_id, error_message)
    VALUES (%(script)s, now(), %(success)s, %(install_id)s, %(message)s)"""
    SQL_SCRIPT_INSTALLED = """SELECT COUNT(*) AS installed FROM _scripts
    WHERE filename = %(script)s AND success = 1"""
    SQL_TEST_META = """SELECT COUNT(*) FROM _install"""

    def __init__(self, mysql):
        self.mysql = mysql
        self.install_id = None

    def run_script(self, script, cast=None):
        return self.mysql.run_script(script=script, cast=cast)

    def meta_create(self, init):
        if init:
            self.mysql.run_query(query=self.SQL_DROP_META_SCRIPTS)
            self.mysql.run_query(query=self.SQL_DROP_META_INSTALL)
        self.mysql.run_query(query=self.SQL_CREATE_META_INSTALL)
        self.mysql.run_query(query=self.SQL_CREATE_META_SCRIPTS)

    def database_test(self):
        try:
            self.mysql.run_query(query=self.SQL_TEST_META)
        except:
            raise AppException("Error accessing database")

    def install_begin(self, version):
        parameters = {'version': version}
        self.install_id = int(self.mysql.run_query(query=self.SQL_INSTALL_BEGIN, parameters=parameters)[0]['ID'])

    def install_done(self, success):
        parameters = {
            'success': 1 if success else 0,
            'install_id': self.install_id,
        }
        self.mysql.run_query(query=self.SQL_INSTALL_DONE, parameters=parameters)

    def script_run(self, script, success, message):
        parameters = {
            'script': script,
            'success': 1 if success else 0,
            'message': message if message else '',
            'install_id': self.install_id,
        }
        self.mysql.run_query(query=self.SQL_SCRIPT_INSTALL, parameters=parameters)

    def script_passed(self, script):
        parameters = {
            'script': script,
        }
        return int(self.mysql.run_query(query=self.SQL_SCRIPT_INSTALLED, parameters=parameters)[0]['installed']) > 0

    def script_header(self, db_config):
        return "USE `%(database)s`;" % db_config


class SqlplusMetaManager(object):

    SQL_CLEAR_DATABASE = """
BEGIN
  FOR cur_rec IN (SELECT object_name, object_type
                  FROM   user_objects
                  WHERE  object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'PROCEDURE', 'FUNCTION', 'SEQUENCE')) LOOP
    BEGIN
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
"""
    SQL_TABLE_EXIST = """
SELECT count(*) AS EXIST FROM USER_TABLES
WHERE TABLE_NAME = %(table)s;
"""
    SQL_CREATE_META_INSTALL = """
CREATE TABLE INSTALL_ (
  ID NUMBER(10) NOT NULL,
  VERSION VARCHAR(20) NOT NULL,
  START_DATE TIMESTAMP NOT NULL,
  END_DATE TIMESTAMP,
  SUCCESS NUMBER(1) NOT NULL,
  PRIMARY KEY (ID)
);
CREATE SEQUENCE INSTALL_SEQUENCE
  START WITH 1
  INCREMENT BY 1
  CACHE 100;
"""
    SQL_CREATE_META_SCRIPTS = """
CREATE TABLE SCRIPTS_ (
  FILENAME VARCHAR(255) NOT NULL,
  INSTALL_DATE TIMESTAMP NOT NULL,
  SUCCESS NUMBER(1) NOT NULL,
  INSTALL_ID NUMBER(10) NOT NULL,
  ERROR_MESSAGE VARCHAR(4000),
  CONSTRAINT FK_INSTALL_ID
    FOREIGN KEY (INSTALL_ID)
    REFERENCES INSTALL_(ID)
);
"""
    SQL_DROP_META_INSTALL = """
DROP TABLE INSTALL_;
DROP SEQUENCE INSTALL_SEQUENCE;
"""
    SQL_DROP_META_SCRIPTS = """
DROP TABLE SCRIPTS_;
"""
    SQL_INSTALL_BEGIN = """
INSERT INTO INSTALL_
  (ID, VERSION, START_DATE, END_DATE, SUCCESS)
VALUES (INSTALL_SEQUENCE.nextval, %(version)s, CURRENT_TIMESTAMP, null, 0);
SELECT max(id) AS ID FROM INSTALL_;
"""
    SQL_INSTALL_DONE = """
UPDATE INSTALL_
   SET END_DATE = CURRENT_TIMESTAMP, SUCCESS = %(success)s
 WHERE ID = %(install_id)s;
"""
    SQL_SCRIPT_INSTALL = """
INSERT INTO SCRIPTS_
  (FILENAME, INSTALL_DATE, SUCCESS, INSTALL_ID, ERROR_MESSAGE)
VALUES
  (%(script)s, CURRENT_TIMESTAMP, %(success)s, %(install_id)s, %(message)s);
"""
    SQL_SCRIPT_INSTALLED = """
SELECT COUNT(*) AS INSTALLED FROM SCRIPTS_
WHERE FILENAME = %(script)s AND SUCCESS = 1;
"""
    SQL_TEST_META = """
SELECT 42 FROM DUAL;
"""

    def __init__(self, sqlplus):
        self.sqlplus = sqlplus
        self.install_id = None

    def run_script(self, script, cast=None):
        return self.sqlplus.run_script(script=script, cast=cast)

    def meta_create(self, init):
        if init:
            self.sqlplus.run_query(query=self.SQL_CLEAR_DATABASE)
            if self.sqlplus.run_query(query=self.SQL_TABLE_EXIST, parameters={'table': 'SCRIPTS_'})[0]['EXIST']:
                self.sqlplus.run_query(query=self.SQL_DROP_META_SCRIPTS)
            if self.sqlplus.run_query(query=self.SQL_TABLE_EXIST, parameters={'table': 'INSTALL_'})[0]['EXIST']:
                self.sqlplus.run_query(query=self.SQL_DROP_META_INSTALL)
        if not self.sqlplus.run_query(query=self.SQL_TABLE_EXIST, parameters={'table': 'INSTALL_'})[0]['EXIST']:
            self.sqlplus.run_query(query=self.SQL_CREATE_META_INSTALL)
        if not self.sqlplus.run_query(query=self.SQL_TABLE_EXIST, parameters={'table': 'SCRIPTS_'})[0]['EXIST']:
            self.sqlplus.run_query(query=self.SQL_CREATE_META_SCRIPTS)

    def database_test(self):
        try:
            self.sqlplus.run_query(query=self.SQL_TEST_META)
        except:
            raise AppException("Error accessing database")

    def install_begin(self, version):
        parameters = {'version': version}
        self.install_id = int(self.sqlplus.run_query(query=self.SQL_INSTALL_BEGIN, parameters=parameters)[0]['ID'])

    def install_done(self, success):
        parameters = {
            'success': 1 if success else 0,
            'install_id': self.install_id,
        }
        self.sqlplus.run_query(query=self.SQL_INSTALL_DONE, parameters=parameters)

    def script_run(self, script, success, message):
        parameters = {
            'script': script,
            'success': 1 if success else 0,
            'message': message if message else '',
            'install_id': self.install_id,
        }
        self.sqlplus.run_query(query=self.SQL_SCRIPT_INSTALL, parameters=parameters)

    def script_passed(self, script):
        parameters = {
            'script': script,
        }
        return int(self.sqlplus.run_query(query=self.SQL_SCRIPT_INSTALLED, parameters=parameters)[0]['INSTALLED']) > 0

    def script_header(self, db_config): # pylint: disable=W0613
        return ''


class AppException(Exception):

    pass


class Config(object):

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def __repr__(self):
        return repr(self.__dict__)


class DBMigration(object):

    VERSION_FILE = 'VERSION'
    SNAPSHOT_POSTFIX = '-SNAPSHOT'
    SCRIPTS_GLOB = '*/*.sql'
    LOCAL_DB_CONFIG = {
        'hostname': 'localhost',
        'username': 'test',
        'password': 'test',
    }
    HELP = """python db_migration.py [-h] [-d] [-i] [-a] [-l] [-u] [-s sql_dir] [-c config]
                       [-p fichier] [-m from] platform [version]
-h          Pour afficher cette page d'aide.
-d          Affiche les scripts a installer mais ne les execute pas.
-i          Initialisation de la base ATTENTION ! Efface toutes les donnees.
-a          Pour installer les scripts de toutes les versions du repertoire.
-l          Pour installer sur la base de donnees locale en mode test.
-u          Pour ne rien afficher sur la console (si tout se passe bien).
-s sql_dir  Le répertoire où se trouvent les fichiers SQL (répertoire du script
            par défaut).
-c config   Indique le fichier de configuration à utiliser (db_configuration.py
            dans le répertoire du script par défaut).
-m from     Ecrit le script de migration de la version 'from' vers 'version'
            sur la console. La valeur 'init' indique que tous les scripts de
            migration doivent être inclus.
platform    La plate-forme sur laquelle on doit installer (les valeurs
            possibles sont 'itg', 'prp' et 'prod'). La valeur par defaut est 'itg'.
version     La version a installer (la version de l'archive par defaut)."""

    @staticmethod
    def run_command_line():
        try:
            DBMigration.parse_command_line(sys.argv[1:]).run()
        except AppException, e:
            print e.message
            sys.exit(1)

    @staticmethod
    def parse_command_line(arguments):
        dry_run = False
        init = False
        all_scripts = False
        local = False
        mute = False
        sql_dir = None
        configuration = None
        from_version = None
        platform = None
        version = None
        try:
            opts, args = getopt.getopt(arguments,
                                       "hdialus:c:p:m:",
                                       ["help", "dry-run", "init", "all", "local", "mute",
                                        "sql-dir=", "config=", "migration="])
        except getopt.GetoptError, exception:
            raise AppException("%s\n%s" % (exception.message, DBMigration.HELP))
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print DBMigration.HELP
                sys.exit(0)
            elif opt in ("-d", "--dry-run"):
                dry_run = True
            elif opt in ("-i", "--init"):
                init = True
            elif opt in ("-a", "--all"):
                all_scripts = True
            elif opt in ("-l", "--local"):
                local = True
            elif opt in ("-u", "--mute"):
                mute = True
            elif opt in ("-s", "--sql-dir"):
                sql_dir = arg
            elif opt in ("-c", "--config"):
                configuration = arg
            elif opt in ("-m", "--migration"):
                from_version = arg
            else:
                raise AppException("Unhandled option: %s\n%s" % (opt, DBMigration.HELP))
        if len(args) == 0:
            raise AppException("Must pass platform on command line:\n%s" % DBMigration.HELP)
        platform = args[0]
        if len(args) > 1:
            version = args[1]
        if len(args) > 2:
            raise AppException("Too many arguments on command line:\n%s" % DBMigration.HELP)
        return DBMigration(dry_run=dry_run, init=init, all_scripts=all_scripts, local=local, mute=mute,
                           platform=platform, version=version, from_version=from_version,
                           sql_dir=sql_dir, configuration=configuration)

    def __init__(self, dry_run, init, all_scripts, local, mute, platform, version, from_version, sql_dir, configuration):
        self.dry_run = dry_run
        self.init = init
        self.all_scripts = all_scripts
        self.local = local
        self.mute = mute
        self.platform = platform
        self.version = version
        self.from_version = from_version
        self.sql_dir = sql_dir
        self.db_config = None
        self.meta_manager = None
        self.version_array = None
        self.config = self.load_configuration(configuration)
        self.check_options()
        self.initialize()

    ###########################################################################
    #                                INIT STUFF                               #
    ###########################################################################

    @staticmethod
    def load_configuration(configuration):
        if not configuration:
            configuration = os.path.join(os.path.dirname(__file__), 'db_configuration.py')
        if not os.path.isfile(configuration):
            raise AppException("Configuration file '%s' not found" % configuration)
        config = {'CONFIG_PATH': os.path.abspath(configuration)}
        execfile(configuration, {}, config)
        return Config(**config)

    def check_options(self):
        if self.version and self.all_scripts:
            raise AppException("You can't give a version with -a option")
        if self.platform not in self.config.PLATFORMS:
            raise AppException('Platform must be one of %s' % ', '.join(sorted(self.config.PLATFORMS)))
        if self.from_version and (self.dry_run or self.local):
            raise AppException("Migration script generation is incompatible with options dry_run and local")
        if self.init and self.platform in self.config.CRITICAL_PLATFORMS and not self.local:
            raise AppException("You can't initialize critical platforms (%s)" % ' and '.join(sorted(self.config.CRITICAL_PLATFORMS)))

    def initialize(self):
        # set database configuration in db_config
        self.db_config = self.config.CONFIGURATION[self.platform]
        if self.local:
            self.db_config.update(self.LOCAL_DB_CONFIG)
        if not self.db_config['password']:
            self.db_config['password'] = getpass.getpass("Database password for user '%s': " % self.db_config['username'])
        if self.config.DATABASE == 'mysql':
            mysql = MysqlCommando(configuration=self.db_config, encoding=self.config.CHARSET)
            self.meta_manager = MysqlMetaManager(mysql)
        elif self.config.DATABASE == 'oracle':
            sqlplus = SqlplusCommando(configuration=self.db_config)
            self.meta_manager = SqlplusMetaManager(sqlplus)
        else:
            raise AppException("DATABASE must be 'mysql' or 'oracle'")
        # set default SQL directory
        if not self.sql_dir:
            if self.config.SQL_DIR:
                if os.path.isabs(self.config.SQL_DIR):
                    self.sql_dir = self.config.SQL_DIR
                else:
                    self.sql_dir = os.path.join(os.path.dirname(self.config.CONFIG_PATH), self.config.SQL_DIR)
            else:
                self.sql_dir = os.path.abspath(os.path.dirname(__file__))
        # manage version
        if not self.version and not self.all_scripts:
            self.version = self.get_version_from_file()
        if not self.version:
            self.version = 'all'
            self.version_array = 0, 0, 0
        else:
            self.version_array = self.split_version(self.version)

    ###########################################################################
    #                              RUNTIME                                    #
    ###########################################################################

    def run(self):
        if self.from_version:
            self.print_migration_script()
        else:
            if not self.mute:
                print "Version '%s' on platform '%s'" % (self.version, self.db_config['hostname'])
                print "Using base '%(database)s' as user '%(username)s'" % self.db_config
            if self.dry_run:
                self.migrate_dry()
            else:
                self.migrate()

    def print_migration_script(self):
        print "-- Migration base '%s' on platform '%s'" % (self.db_config['database'], self.platform)
        print "-- From version '%s' to '%s'" % (self.from_version, self.version)
        print self.meta_manager.script_header(self.db_config)
        print
        for script in self.select_scripts():
            print "-- Script '%s'" % script
            print open(os.path.join(self.sql_dir, script)).read().strip()
            print

    def migrate_dry(self):
        print "Testing database connection...",
        sys.stdout.flush()
        self.meta_manager.database_test()
        print "OK"
        print "SQL scripts to run:"
        for script in self.filter_scripts():
            print "- %s" % script

    def migrate(self):
        if not self.mute:
            print "Writing meta tables...",
            sys.stdout.flush()
        self.meta_manager.meta_create(self.init)
        self.meta_manager.install_begin(self.version)
        if not self.mute:
            print "OK"
        install_success = True
        errors = []
        try:
            scripts = self.filter_scripts()
            for script in scripts:
                success = True
                message = None
                try:
                    if not self.mute:
                        print "Running script '%s'... " % script,
                        sys.stdout.flush()
                    self.meta_manager.run_script(os.path.join(self.sql_dir, script))
                    if not self.mute:
                        print "OK"
                except Exception, e:
                    if not self.mute:
                        print "ERROR"
                    success = False
                    install_success = False
                    if e.message:
                        message = e.message
                    else:
                        message = str(e)
                    errors.append((script, message))
                    break
                finally:
                    self.meta_manager.script_run(script, success, message)
        finally:
            self.meta_manager.install_done(install_success)
        if install_success:
            if not self.mute:
                print "OK"
        else:
            print '-'*80
            print "Error running following migration scripts:"
            for error in errors:
                print "- %s: %s" % error
            print '-'*80
            raise AppException("ERROR")

    ###########################################################################
    #                              UTILITY METHODS                            #
    ###########################################################################

    @staticmethod
    def split_version(version):
        if version == 'init':
            return None
        elif re.match('\\d+(\\.\\d+)*', version):
            return [int(i) for i in version.split('.')]
        else:
            raise AppException("Unknown version '%s'" % version)

    @staticmethod
    def get_version_from_file():
        if os.path.exists(DBMigration.VERSION_FILE):
            version = open(DBMigration.VERSION_FILE).read().strip()
            # remove trailing '-SNAPSHOT' on version
            if version.endswith(DBMigration.SNAPSHOT_POSTFIX):
                version = version[:-len(DBMigration.SNAPSHOT_POSTFIX)]
            return version
        else:
            raise AppException("Version file not found, please set version on command line")

    @staticmethod
    def execute(command):
        result = os.system(command)
        if result != 0:
            raise AppException("Error running command '%s'" % command)

    @staticmethod
    def _script_platform_version_name(script):
        platform = os.path.basename(script)
        if '.' in platform:
            platform = platform[:platform.index('.')]
        if '-' in platform:
            platform = platform[:platform.index('-')]
        dirname = os.path.dirname(script)
        if os.path.sep in dirname:
            v = dirname[dirname.rindex(os.path.sep)+1:]
        else:
            v = dirname
        version = DBMigration.split_version(v)
        name = v + os.path.sep + os.path.basename(script)
        return platform, version, name

    def select_scripts(self):
        if self.from_version != 'init':
            self.from_version = self.split_version(self.from_version)
        scripts_list = glob.glob(os.path.join(self.sql_dir, self.SCRIPTS_GLOB))
        version_script_directory_list = []
        init_script_directory_list = []
        for script in scripts_list:
            script_platform, script_version, script_name = self._script_platform_version_name(script)
            if script_platform == 'all' or script_platform == self.platform:
                if script_version:
                    if self.from_version == 'init' or script_version > self.from_version:
                        if script_version <= self.version_array:
                            version_script_directory_list.append(script_name)
                else:
                    if self.from_version == 'init':
                        init_script_directory_list.append(script_name)
        return sorted(init_script_directory_list) + \
               sorted(version_script_directory_list, key=self.script_file_sorter)

    def filter_scripts(self):
        scripts_list = glob.glob(os.path.join(self.sql_dir, self.SCRIPTS_GLOB))
        version_script_directory_list = []
        init_script_directory_list = []
        for script in scripts_list:
            script_platform, script_version, script_name = self._script_platform_version_name(script)
            if script_platform == 'all' or script_platform == self.platform:
                if script_version:
                    if self.all_scripts or self.version_array >= script_version:
                        if not self.meta_manager.script_passed(script_name) or self.init:
                            version_script_directory_list.append(script_name)
                else:
                    if self.init:
                        init_script_directory_list.append(script_name)
        return sorted(init_script_directory_list) + \
               sorted(version_script_directory_list, key=self.script_file_sorter)

    def script_file_sorter(self, filename):
        platform, version, name = self._script_platform_version_name(filename)
        base = os.path.basename(name)
        if platform == 'all':
            platform_index = 0
        else:
            platform_index = 1
        return (version,  platform_index, base)


def main():
    DBMigration.run_command_line()


if __name__ == '__main__':
    main()
