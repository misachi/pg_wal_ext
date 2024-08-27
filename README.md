# pg_wal_ext(WIP)
Read WAL files and possibly generate SQL from WAL

A Postgres extension to read logged WAL records and attempt to decode the records and generate SQL for DML operations(inserts supported for the moment). It currently works for the current database we are connected to only. One possible use case would be for database restoration(has limitations) or audit purposes
