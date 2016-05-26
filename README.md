Stork
=====

Stork is a database migration tool for **MySQL** and **Oracle** databases. It
calls *mysql* or *sqlplus* on command line to run your migration scripts. This
script is an elegant way to automate your database migration: Stork knows which
scripts have already run so you don't have to produce a dedicated set of 
migration scripts from a given version. Your scripts will adapt to migrate to a
target version.

Scripts organization
--------------------

Migration scripts are organized by version: initialization scripts live in
*init* directory and scripts for version *x.y.z* in directory *x.y.z*. In each
directory, scripts starting with *all* will apply on all platforms, while those
starting with *foo* will apply on this platform.

Thus, with following migration scripts:

```
sql
├── 0.1.0
│   ├── all.sql
│   └── itg.sql
├── 0.2.0
│   └── all.sql
└── init
    ├── all.sql
    ├── itg.sql
    └── prod.sql
```


