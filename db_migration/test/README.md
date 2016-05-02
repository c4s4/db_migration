Prerequisites
-------------

To run tests, you need to configure a local MySQL database:

- database: `test`
- username: `test`
- password: `test`

To create this database and user, you might run following script:

```sql
drop database if exists test;
create database test;
grant all on test.* to 'test'@'localhost' identified by 'test';
```

Run tests
---------

```shell
$ python test_db_migration.py
```
