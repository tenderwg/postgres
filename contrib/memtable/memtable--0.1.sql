CREATE FUNCTION memtable_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD memtable TYPE TABLE HANDLER memtable.memtable_handler;