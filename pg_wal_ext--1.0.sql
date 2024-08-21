/* contrib/pg_wal_ext/pg_wal_ext--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_wal_ext" to load this file. \quit

--
-- pg_xlog_records()
--

CREATE FUNCTION pg_xlog_records(IN xlog_file_path text, IN relOid oid, OUT page_num int4, OUT txn_id xid, OUT xlog_type text, OUT commit_ts timestamptz, OUT generated_sql text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_xlog_records'
LANGUAGE C STRICT PARALLEL SAFE;

-- GRANT SELECT ON pg_xlog_records(text) TO PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_xlog_records(TEXT, OID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_xlog_records(TEXT, OID) TO pg_read_server_files;

-- Query example
-- select * from pg_xlog_records(path/to/wal/file, oid_of_the_table); => where `path/to/wal/file` is the full path to the WAL file and
-- `oid_of_the_table` is the oid of the specific table to decode. The oid is from pg_class: `select * from pg_class;`

-- select * from pg_xlog_records('/usr/local/pgsql/data/pg_wal/000000010000000000000001', 24576);
