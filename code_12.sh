#!/bin/bash

# Set the name of the Hive database
db_name="my_db"

# Set the name of the Hive table
table_name="my_table"

# Set the backup database and table name
backup_db_name="my_backup_db"
backup_table_name="my_backup_table"

# Set the backup location
backup_location="/path/to/backup/location"

# Create the backup database if it doesn't exist
hive -e "CREATE DATABASE IF NOT EXISTS ${backup_db_name};"

# Create the backup table using the schema of the source table
hive -e "USE ${backup_db_name}; \
CREATE EXTERNAL TABLE IF NOT EXISTS ${backup_table_name} LIKE ${db_name}.${table_name} LOCATION '${backup_location}';"

# Insert data into the backup table from the source table
hive -e "USE ${backup_db_name}; \
INSERT OVERWRITE TABLE ${backup_table_name} PARTITION(year, month) \
SELECT * FROM ${db_name}.${table_name} \
WHERE year = '2022' AND month = '03';"
