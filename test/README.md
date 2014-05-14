Prérequis
---------

Pour dérouler les tests, vous avez besoin d'une base de données en local :
- nom de la base : `test`
- utilisateur : `test`
- mot de passe : `test`

Pour créer cette base
    drop database if exists test;
    create database test;
    grant all on test.* to 'test'@'localhost' identified by 'test';

Rouler les tests
----------------

    python test_db_migration.py
