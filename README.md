# pg_wal_ext(WIP)
Read WAL files and generate SQL from WAL

A Postgres extension to read logged WAL records and attempt to decode the records and generate SQL for DML operations. One possible use case would be for database restoration(has limitations) or audit purposes.

# Installation
```
CREATE EXTENSION pg_wal_ext;
```

# Usage
Pass in the WAL file(to replace "path/to/wal/file" as shown) to be decoded: `select * from pg_xlog_records("path/to/wal/file");`

# Demo
![Usage Demo](assets/sql-from-wal.gif)
