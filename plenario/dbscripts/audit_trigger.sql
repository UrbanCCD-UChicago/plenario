CREATE SCHEMA audit;

COMMENT ON SCHEMA audit IS 'Out-of-table audit/history logging tables and trigger functions. Basic';

CREATE OR REPLACE FUNCTION audit.if_modified() RETURNS TRIGGER AS $function$
DECLARE
    temp_row RECORD; -- a temporary variable used on updates/deletes
    v_count int;
    v_sql text;
    v_table text;
BEGIN
    IF TG_WHEN <> 'AFTER' THEN
        RAISE EXCEPTION 'audit.if_modified() may only run as an AFTER trigger';
    END IF;

    --if not exists history table create it
    v_table = TG_TABLE_NAME::regclass || '_history';
    v_sql = 'SELECT ' || '  count(1) ' || 'FROM ' || '  information_schema.tables ' || 'WHERE ' || E'  table_name=\'' || v_table || E'\'';
    
    EXECUTE v_sql INTO v_count;
    IF (v_count = 0) THEN
        v_sql = '
	CREATE TABLE ' || TG_TABLE_NAME::regclass || '_history' || '(
	    action_tstamp_tx TIMESTAMP WITH TIME ZONE NOT NULL,
	    action CHAR(1) NOT NULL CHECK (action IN (''''I'''',''''D'''',''''U'''', ''''T'''')),
	    row_data ' || TG_TABLE_NAME::regclass ||
	')';

       EXECUTE v_sql;
    END IF;
    --else

     temp_row = ROW(
        CURRENT_TIMESTAMP,                            -- action_tstamp_tx
        SUBSTRING(TG_OP,1,1),                         -- action
        NULL                                          -- row_data
        );
 
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        temp_row.row_data = OLD;
    ELSIF (TG_OP = 'DELETE' AND TG_LEVEL = 'ROW') THEN
        temp_row.row_data = OLD;
    ELSIF (TG_OP = 'INSERT' AND TG_LEVEL = 'ROW') THEN
        temp_row.row_data = NEW;
    --for future if we need some STMT level logging
    --ELSIF (TG_LEVEL = 'STATEMENT' AND TG_OP IN ('INSERT','UPDATE','DELETE','TRUNCATE')) THEN
    --    temp_row.statement_only = 't';
    ELSE
        RAISE EXCEPTION '[audit.if_modified] - Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;
    EXECUTE 'INSERT INTO ' || TG_TABLE_NAME::regclass || '_history VALUES (temp_row.*)'; 
    
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        temp_row.row_data = NEW;
        EXECUTE 'INSERT INTO ' || TG_TABLE_NAME::regclass || '_history VALUES (temp_row.*)'; 
    END IF; 
    RETURN NULL;
END;
$function$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public;
