# pg_auto_shard
Shard your PostgreSQL database automatically

Author: Sachin Kotwal

This tool is developed to automatically distribute all your table to specified shards while creating it into your database. This tool works with dblink and postgres_fdw contrib modules of PostgreSQL. Those must be installed before going ahead. This tool gives you behavior like (not exactly same) Greenplum database.

NOTE: Make sure you have installed dblink and postgres_fdw contrib modules.

Steps to install this tool as below:

1. Just create dblink and postgres_fdw extensions and run provided install.sql into tour targate database as below:
$ $psql -d postgres
postgres=# CREATE EXTENSION dblink;
postgres=# CREATE EXTENSION postgres_fdw;

$ psql -d postgres -f install.sql

2. Then insert your shard machines configuration into shard.shard_config table for each shard as below:
postgres=# INSERT INTO shard.shard_config values(1,'server_0','127.0.0.1',5433,'postgres');

3. Then create table which you want to create in same database as below:

postgres=# CREATE TABLE test(id int, name varchar);

4. Confirm your related foreign tables has been created with your actual table as below:
postgres=# \det

postgres=# \dt

5. Also connect to shard Machines and confirm table is replicated with same definition on shard machines using below commands:

postgres=# \dt

postgres=# \d+ your_table_name

6. Now do insert, update , delete and select on your table and see it works well.

NOTE: 
A. while dropping your table you must need to you cascade like below:
postgres=# DROP TABLE test cascade;

B. To remove server configurations use below command, It will remove related
foreign servers and user mappings:
postgres=# DELETE FROM shard.shard_config;
OR
postgres=# DELETE FROM shard.shard_config WHERE sid=1;


