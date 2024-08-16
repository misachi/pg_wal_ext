/* contrib/pg_wal_ext/pg_wal_ext--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_wal_ext" to load this file. \quit

--
-- pg_xlog_records()
--

CREATE FUNCTION pg_xlog_records(IN xlog_file_path text, OUT page_num int4, OUT txn_id xid, OUT xlog_type text, OUT commit_ts timestamptz, OUT max_decode_block_id int4)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_xlog_records'
LANGUAGE C STRICT PARALLEL SAFE;

-- GRANT SELECT ON pg_xlog_records(text) TO PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_xlog_records(TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_xlog_records(TEXT) TO pg_read_server_files;

-- Query example
-- select * from pg_xlog_records('/usr/local/pgsql/data/pg_wal/000000010000000000000001');
