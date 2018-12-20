
CREATE SCHEMA IF NOT EXISTS shard;
CREATE TABLE IF NOT EXISTS shard.shard_config(sid int primary key,sname varchar not null, host varchar, port int not null, dbname varchar not null, password varchar default '' ,unique(sname,host,port,dbname));
  
CREATE OR REPLACE FUNCTION shard.foreign_server_trigger()
RETURNS TRIGGER AS $$
DECLARE
cmd text;
server_name varchar;
sid_temp int;
BEGIN
 IF (TG_OP = 'DELETE') THEN
 sid_temp := OLD.sid;
  server_name := (SELECT sname FROM shard.shard_config WHERE sid=OLD.sid);
 	cmd := 'DROP USER MAPPING IF EXISTS FOR public SERVER ' || server_name;
 	EXECUTE cmd ; 
 	cmd := 'DROP SERVER IF EXISTS ' || server_name;
 	EXECUTE cmd; 
 	RETURN OLD;
 ELSIF (TG_OP = 'UPDATE') THEN 
 sid_temp := OLD.sid;
   server_name := (SELECT sname FROM shard.shard_config WHERE sid=sid_temp);
 	cmd := 'DROP USER MAPPING IF EXISTS FOR public SERVER ' || server_name;
 	EXECUTE cmd; 
 	cmd := 'DROP SERVER IF EXISTS ' || server_name;
 	EXECUTE cmd; 
 	cmd := 'CREATE SERVER ' || NEW.sname || ' FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host ''';
 	cmd := cmd || NEW.host || ''', port ''' || NEW.port;
 	cmd := cmd || ''', dbname ''' || NEW.dbname ||  ''')';
 	EXECUTE cmd; 
 	cmd := 'CREATE USER MAPPING FOR PUBLIC SERVER ' || NEW.sname;
 	cmd := cmd || ' OPTIONS (password ''' || NEW.password || ''')';
 	EXECUTE cmd;
 	RETURN NEW;
 ELSIF (TG_OP = 'INSERT') THEN
 	cmd := 'CREATE SERVER ' || NEW.sname || ' FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host ''';
 	cmd := cmd || NEW.host || ''', port ''' || NEW.port;
 	cmd := cmd || ''', dbname ''' || NEW.dbname ||  ''')';
 	EXECUTE cmd; 
 	cmd := 'CREATE USER MAPPING FOR PUBLIC SERVER ' || NEW.sname;
 	cmd := cmd || ' OPTIONS (password ''' || NEW.password || ''')';
 	EXECUTE cmd;
 	RETURN NEW;
 END IF;
 RETURN NULL;
END;
$$
LANGUAGE plpgsql;
ALTER FUNCTION shard.foreign_server_trigger() OWNER TO postgres;


CREATE TRIGGER foreign_server_trigger
 BEFORE UPDATE OR INSERT OR DELETE
 ON shard.shard_config
 FOR EACH ROW
 EXECUTE PROCEDURE shard.foreign_server_trigger();

CREATE OR REPLACE FUNCTION shard.fn_ddl_trigger_create()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
DECLARE
    shard record;
    tablename text;
    schemaname text;
    temp text;
    cmd text;
    tbl_def text;
    part_col varchar;
    shard_count int;
    conn varchar;
    i int;
BEGIN
  temp := ( SELECT object_identity FROM pg_event_trigger_ddl_commands() limit 1);
  tablename := (SELECT split_part(temp,'.',2));
  schemaname := (SELECT split_part(temp,'.',1));
  tbl_def := (SELECT shard.pg_get_tabledef(schemaname,tablename));
  part_col := (SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = tablename::regclass AND i.indisprimary limit 1);
  IF part_col IS NULL OR part_col = ''
  THEN
    part_col := (SELECT column_name from information_schema.columns where table_schema = schemaname and table_name = tablename limit 1);
  END IF;
  shard_count := (SELECT count(1) FROM shard.shard_config);
  i := 0;  
  FOR shard IN SELECT * FROM shard.shard_config 
  LOOP
  
	cmd := 'CREATE FOREIGN TABLE IF NOT EXISTS ' || tablename || '_f_' || i;
	cmd := cmd || '(CHECK (' || part_col || ' % ' || shard_count || ' = ' || i;
	cmd := cmd || ')) INHERITS (' || schemaname ||'.' || tablename;
	cmd := cmd || ') SERVER ' || shard.sname || ' OPTIONS (table_name ''' || tablename || '_' || i || ''')';
	EXECUTE cmd ;

	conn := 'host=' || shard.host || ' dbname=' || shard.dbname || ' port=' || shard.port;
	cmd := 'CREATE TABLE IF NOT EXISTS ' || schemaname || '.' || tablename || '_' || i || tbl_def;
	PERFORM dblink_connect('dbconn',conn);
	PERFORM * FROM dblink_exec('dbconn',cmd);
	PERFORM dblink_disconnect('dbconn');
	i := i + 1;
  END LOOP;
  
  PERFORM shard.create_partition_insert_func(part_col,schemaname,tablename,shard_count); 
  cmd := 'CREATE TRIGGER ' || tablename || '_insert_trigger';
  cmd := cmd || ' BEFORE INSERT ON ' || schemaname || '.' || tablename || ' FOR EACH ROW';
  cmd := cmd || ' EXECUTE PROCEDURE shard.' || tablename || '_partition_insert()';
  EXECUTE cmd ;
END;
$$;
ALTER FUNCTION shard.fn_ddl_trigger_create() OWNER TO postgres;
  

CREATE OR REPLACE FUNCTION shard.pg_get_tabledef(text,text) RETURNS text
AS
$$
  DECLARE
     tabledef TEXT;
     dotpos integer;
     tablename text;
     schemaname text;
     prevcol text;
     coltype text;
     notnull1 boolean;
     rec record;
     oidcheck boolean;
  BEGIN
  schemaname := $1;
  tablename := $2;
  select relhasoids into oidcheck from pg_class,pg_namespace where pg_class.relnamespace=pg_namespace.oid and pg_namespace.nspname=schemaname and pg_class.relname=tablename and pg_class.relkind='r';
   if not found then
     tabledef:='Table Does not exists!';
     return tabledef;
   end if;
  --tabledef:= 'CREATE TABLE '|| schemaname||'.'||tablename;
   for rec in SELECT a.attname as columnname ,pg_catalog.format_type(a.atttypid, a.atttypmod) as coltype, (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),a.attnotnull as notnull, a.attnum FROM pg_catalog.pg_attribute a WHERE a.attrelid = (select pg_class.oid from pg_class,pg_namespace where relname=tablename and pg_class.relnamespace=pg_namespace.oid and pg_namespace.nspname=schemaname) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum  
  loop
      if prevcol is null  then
 tabledef:=' (';
        prevcol:=rec.columnname;
        coltype:=rec.coltype;
        notnull1:=rec.notnull;
      elsif notnull1 then
        tabledef:=tabledef||' '||E'\n'||prevcol||' '||coltype||' NOT NULL ,';
        prevcol:=rec.columnname;
        coltype:=rec.coltype;
        notnull1:=rec.notnull;
     else
        tabledef:=tabledef||' '||E'\n'||prevcol||' '||coltype||' ,';
        prevcol:=rec.columnname;
        coltype:=rec.coltype;
        notnull1:=rec.notnull;
     end if;
   end loop;
      if oidcheck = true and notnull1 = true then
        tabledef:=tabledef||E'\n'||prevcol||' '||coltype||' NOT NULL ) WITH OIDS;';
      elsif oidcheck = true and notnull1 = false then
        tabledef:=tabledef||E'\n'||prevcol||' '||coltype||' NOT NULL ) WITH OIDS;';
      elsif oidcheck=false and notnull1=true then
        tabledef:=tabledef||E'\n'|| prevcol||' '||coltype||' NOT NULL ) WITHOUT OIDS;';
      else
        tabledef:=tabledef||E'\n'||prevcol||' '||coltype||' ) WITHOUT OIDS;';
      end if;
   
   return tabledef;
   end;
$$ language plpgsql;   
ALTER FUNCTION shard.pg_get_tabledef(text,text) OWNER TO postgres;
    
CREATE OR REPLACE FUNCTION shard.create_partition_insert_func(part_col varchar, schemaname text, tablename text, shard_count int)
  RETURNS VOID AS
$func$
DECLARE
cmd text;
BEGIN
  cmd := 'CREATE OR REPLACE FUNCTION shard.' || tablename || '_partition_insert()'; 
  cmd := cmd || ' RETURNS trigger AS $$ DECLARE v_destination VARCHAR; sql VARCHAR;';
  cmd := cmd || ' BEGIN SELECT mod(new.' || part_col || ', ' || shard_count || ') into v_destination;';
  cmd := cmd || ' v_destination := ''' || schemaname  || '.' || tablename || ''' || ''_f_'' || v_destination' || ';';
  cmd := cmd || ' sql := ''INSERT INTO  '' || v_destination ||  '' VALUES ( ($1).*)''' || ';'; 
  cmd := cmd || ' BEGIN EXECUTE sql USING NEW; END; RETURN NULL; END; $$ LANGUAGE ''plpgsql''' || ';';
  EXECUTE cmd;  
  cmd := 'ALTER FUNCTION shard.' || tablename || '_partition_insert() OWNER TO postgres';
  EXECUTE cmd; 
END
$func$ LANGUAGE plpgsql;
ALTER FUNCTION shard.create_partition_insert_func(part_col varchar, schemaname text, tablename text, shard_count int) OWNER TO postgres; 
  

CREATE OR REPLACE FUNCTION shard.fn_ddl_trigger_drop()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
DECLARE
    shard record;
    tablename text;
    schemaname text;
    temp text;
    cmd text;
    shard_count int;
    conn varchar;
    i int;
BEGIN
  temp := (SELECT object_identity FROM pg_event_trigger_dropped_objects() limit 1);
  tablename := (SELECT split_part(temp,'.',2));
  schemaname := (SELECT split_part(temp,'.',1));

  shard_count := (SELECT count(1) FROM shard.shard_config);
  i := 0;  
  FOR shard IN SELECT * FROM shard.shard_config 
  LOOP
    conn := 'host=' || shard.host || ' dbname=' || shard.dbname || ' port=' || shard.port;
	cmd := 'DROP TABLE IF EXISTS ' || schemaname || '.' || tablename || '_' || i;
	PERFORM dblink_connect('dbconn',conn);
	PERFORM * FROM dblink_exec('dbconn',cmd);
	PERFORM dblink_disconnect('dbconn');
	i := i + 1;
  END LOOP;
  
  cmd := 'DROP FUNCTION shard.' || tablename || '_partition_insert()';
  EXECUTE cmd;
END;
$$;
ALTER FUNCTION shard.fn_ddl_trigger_drop() OWNER TO postgres;

 
CREATE EVENT TRIGGER ddl_trigger_create ON ddl_command_end WHEN TAG in ('CREATE TABLE') EXECUTE PROCEDURE shard.fn_ddl_trigger_create();
CREATE EVENT TRIGGER ddl_trigger_drop ON sql_drop WHEN TAG in ('DROP TABLE') EXECUTE PROCEDURE shard.fn_ddl_trigger_drop();



