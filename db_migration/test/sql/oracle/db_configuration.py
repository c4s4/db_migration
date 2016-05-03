# encoding: UTF-8

# platform list
PLATFORMS = ['itg', 'prp', 'prod']
# default platform
DEFAULT_PLATFORM = PLATFORMS[0]
# platform where init is forbidden
CRITICAL_PLATFORMS = PLATFORMS[1:]
# directory for SQL scripts (relative to this configuration file)
SQL_DIR = "."

# Database name ('mysql' or 'oracle')
DATABASE = 'oracle'
# Database configuration for environments
CONFIGURATION = {
    'itg': {
        'hostname': 'localhost:1521',
        'database': 'xe',
        'password': 'test',
        'username': 'test',
    },
    'prp': {
        'hostname': 'localhost:1521',
        'database': 'xe',
        'password': 'test',
        'username': 'test',
    },
    'prod': {
        'hostname': 'localhost:1521',
        'database': 'xe',
        'password': 'test',
        'username': 'test',
    },
}