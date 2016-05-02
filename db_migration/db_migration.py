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


#pylint: disable=E1103
class MysqlCommando(object):

    ISO_FORMAT = '%Y-%m-%d %H:%M:%S'
    CASTS = (
        (r'-?\d+', int),
        (r'-?\d*\.?\d*([Ee][+-]?\d+)?', float),
        (r'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d', lambda d: datetime.datetime.strptime(d, MysqlCommando.ISO_FORMAT)),
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
            raise Exception('Missing database configuration')
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
            raise Exception(errput.strip())
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
        else:
            raise Exception("Type '%s' is not managed as a query parameter" % parameter.__class__.__name__)

    @staticmethod
    def _escape_string(string):
        return string.replace("'", "''")


class MetaManager(object):

    SQL_CREATE_META_INSTALL = """CREATE TABLE IF NOT EXISTS _install (
      id integer NOT NULL AUTO_INCREMENT,
      major integer NOT NULL,
      minor integer NOT NULL,
      debug integer NOT NULL,
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
    (major, minor, debug, start_date, end_date, success)
    VALUES (%(major)s, %(minor)s, %(debug)s, now(), null, 0);
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

    def install_begin(self, version_array):
        parameters = {
            'major': version_array[0],
            'minor': version_array[1],
            'debug': version_array[2],
        }
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
-p fichier  Realise un dump de la base de donnees dans le fichier avant
            d'effectuer la migration.
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
        dump = None
        from_version = None
        platform = None
        version = None
        try:
            opts, args = getopt.getopt(arguments,
                                       "hdialus:c:p:m:",
                                       ["help", "dry-run", "init", "all", "local", "mute",
                                        "sql-dir=", "config=", "dump=", "migration="])
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
            elif opt in ("-p", "--dump"):
                dump = arg
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
                           platform=platform, version=version, dump=dump, from_version=from_version,
                           sql_dir=sql_dir, configuration=configuration)

    def __init__(self, dry_run, init, all_scripts, local, mute, platform, version, dump, from_version, sql_dir, configuration):
        self.dry_run = dry_run
        self.init = init
        self.all_scripts = all_scripts
        self.local = local
        self.mute = mute
        self.platform = platform
        self.version = version
        self.dump = dump
        self.from_version = from_version
        self.sql_dir = sql_dir
        self.db_config = None
        self.mysql = None
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
        config = {}
        execfile(configuration, {}, config)
        return Config(**config)

    def check_options(self):
        if self.version and self.all_scripts:
            raise AppException("You can't give a version with -a option")
        if not self.platform in self.config.PLATFORMS:
            raise AppException('Platform must be one of %s' % ', '.join(sorted(self.config.PLATFORMS)))
        if self.from_version and (self.dry_run or self.local or self.dump):
            raise AppException("Migration script generation is incompatible with options dry_run local and dump")
        if self.init and self.platform in self.config.CRITICAL_PLATFORMS and not self.local:
            raise AppException("You can't initialize critical platforms (%s)" % ' and '.join(sorted(self.config.CRITICAL_PLATFORMS)))

    def initialize(self):
        # set database configuration in db_config
        self.db_config = self.config.CONFIGURATION[self.platform]
        if self.local:
            self.db_config.update(self.LOCAL_DB_CONFIG)
        if not self.db_config['password']:
            self.db_config['password'] = getpass.getpass("Database password for user '%s': " % self.db_config['username'])
        self.mysql = MysqlCommando(configuration=self.db_config, encoding=self.config.CHARSET)
        self.meta_manager = MetaManager(self.mysql)
        # set default SQL directory
        if not self.sql_dir:
            if self.config.SQL_DIR:
                self.sql_dir = self.SQL_DIR
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
            if self.dump:
                self.dump_database()
            if self.dry_run:
                self.migrate_dry()
            else:
                self.migrate()

    def print_migration_script(self):
        print "-- Migration base '%s' on platform '%s'" % (self.db_config['database'], self.platform)
        print "-- From version '%s' to '%s'" % (self.from_version, self.version)
        print "USE `%(database)s`;" % self.db_config
        print
        for script in self.select_scripts():
            print "-- Script '%s'" % script
            print open(os.path.join(self.sql_dir, script)).read().strip()
            print

    def dump_database(self):
        if not self.mute:
            print "Dumping database in file '%s'..." % self.dump,
            sys.stdout.flush()
        self.execute("mysqldump -u%s -p%s -h%s --complete-insert --opt -r%s %s" %
                     (self.db_config['username'], self.db_config['password'], self.db_config['hostname'],
                      self.dump, self.db_config['database']))
        if not self.mute:
            print "OK"

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
        self.meta_manager.install_begin(self.version_array)
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
                    self.mysql.run_script(os.path.join(self.sql_dir, script))
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
        if re.match('\\d+(\\.\\d+(\\.\\d+)?)?', version):
            version_array = [int(i) for i in version.split('.')]
            while len(version_array) < 3:
                version_array.append(0)
            return version_array
        elif version == 'init':
            return None
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

    def select_scripts(self):
        if self.from_version != 'init':
            self.from_version = self.split_version(self.from_version)
        scripts_list = glob.glob(os.path.join(self.sql_dir, self.SCRIPTS_GLOB))
        version_script_directory_list = []
        init_script_directory_list = []
        for script in scripts_list:
            dir_name = os.path.dirname(script)
            script_name = dir_name[dir_name.rindex('/')+1:] + os.path.sep + os.path.basename(script)
            script_version = self.split_version(dir_name[dir_name.rindex('/')+1:])
            script_platform = os.path.basename(script)[:-4]
            if script_platform == 'all' or script_platform == self.platform:
                if script_version:
                    if self.from_version == 'init' or script_version > self.from_version:
                        if script_version <= self.version:
                            version_script_directory_list.append(script_name)
                else:
                    if self.from_version == 'init':
                        init_script_directory_list.append(script_name)
        return sorted(init_script_directory_list) + sorted(version_script_directory_list, key=self.script_file_sorter)

    def filter_scripts(self):
        scripts_list = glob.glob(os.path.join(self.sql_dir, self.SCRIPTS_GLOB))
        version_script_directory_list = []
        init_script_directory_list = []
        for script in scripts_list:
            dir_name = os.path.dirname(script)
            script_name = dir_name[dir_name.rindex('/')+1:] + os.path.sep + os.path.basename(script)
            script_version = self.split_version(dir_name[dir_name.rindex('/')+1:])
            script_platform = os.path.basename(script)[:-4]
            if script_platform == 'all' or script_platform == self.platform:
                if script_version:
                    if self.all_scripts or self.version_array >= script_version:
                        if not self.meta_manager.script_passed(script_name) or self.init:
                            version_script_directory_list.append(script_name)
                else:
                    if self.init:
                        init_script_directory_list.append(script_name)
        return sorted(init_script_directory_list) + sorted(version_script_directory_list, key=self.script_file_sorter)

    def script_file_sorter(self, filename):
        platform = os.path.basename(filename)[:-4]
        if platform == 'all':
            platform_index = 0
        else:
            platform_index = 1
        version = self.split_version(filename[:filename.index('/')])
        return version + [platform_index]


def main():
    DBMigration.run_command_line()


if __name__ == '__main__':
    main()
