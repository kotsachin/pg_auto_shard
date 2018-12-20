
DROP EVENT TRIGGER ddl_trigger_create;
DROP EVENT TRIGGER ddl_trigger_drop;
DROP TRIGGER foreign_server_trigger ON shard.shard_config;

DROP FUNCTION shard.fn_ddl_trigger_create();
DROP FUNCTION shard.pg_get_tabledef(text,text);
DROP FUNCTION shard.create_partition_insert_func(character varying,text,text,integer);
DROP FUNCTION shard.fn_ddl_trigger_drop();

DROP FUNCTION shard.foreign_server_trigger();

