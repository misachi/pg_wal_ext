# pg_wal_ext
Read WAL files and generate SQL from WAL

~~Developed and tested on Postgres v17 which was compiled from source -- I will be working on testing it on PG16 as well🤞🏾~~

The extension has been ran on Postgres v16(both compiled from source and pre-compiled binary from Ubuntu) and Postgres v17(compiled from source)

A Postgres extension to read logged WAL records and attempt to decode the records and generate SQL for DML operations. One possible use case would be for database restoration(has limitations) or audit purposes.

# Installation
By default, it uses PGXS for installation. Set `OLD_INSTALL` to any value if this is not desired.

### Postgres [Only applies if you don't have Postgres installed already]
Update APT repository
```
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo tee /etc/apt/trusted.gpg.d/pgdg.asc &>/dev/null
```

install Postgres with its headers
```
sudo apt update
sudo apt install postgresql postgresql-server-dev-XXX
```
Replace the XXX in `postgresql-server-dev-XXX` with your Postgres version

### Install Extension
Run these inside the directory you cloned the repository in:

```
make PG_CONFIG=/usr/bin/pg_config
make PG_CONFIG=/usr/bin/pg_config install
```
Note you might need to run above two commands using `sudo`

Next log in to your database and create the extension:
```
CREATE EXTENSION pg_wal_ext;
```

# Usage
Pass in the WAL file(to replace "path/to/wal/file" as shown) to be decoded: `select * from pg_xlog_records("path/to/wal/file");`

# Demo
![Usage Demo](assets/sql-from-wal.gif)
