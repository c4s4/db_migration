Stork
=====

Stork is a database migration tool for **MySQL** and **Oracle** databases. It
calls *mysql* or *sqlplus* on command line to run migration scripts. This way,
this tool might replace an existing *by hand* migration procedure.

Scripts structure
-----------------

Migration scripts are organized by version: initialization scripts live in
*init* directory and scripts for version *x.y.z* in directory *x.y.z*. In each
directory, scripts starting with *all* will apply on all platforms, while those
starting with *foo* will apply on this platform. Thus, you can customize
platforms to add specific data in development platform for instance.

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


