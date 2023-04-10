#!/bin/bash

# Set the name of the Hive database
db_name="my_db"

# Set the name of the Hive table
table_name="my_table"

# Set the name of the partition columns
partition_cols=("year" "month")

# Set the date to restore to (in yyyy-MM-dd format)
restore_date="2022-02-10"

# Derive the year and month from the restore date
restore_year=$(date -d "${restore_date}" "+%Y")
restore_month=$(date -d "${restore_date}" "+%m")

# Set the name and location of the backup table
backup_table_name="${table_name}_backup"
backup_table_location="/user/hive/warehouse/${db_name}.db/${backup_table_name}"

# Set the name and location of the rolled-back table
rolled_back_table_name="${table_name}_rolled_back"
rolled_back_table_location="/user/hive/warehouse/${db_name}.db/${rolled_back_table_name}"

# Backup the table to a temporary table
hive -e "CREATE TABLE ${backup_table_name} LIKE ${db_name}.${table_name} STORED AS ORC; \
INSERT INTO ${backup_table_name} PARTITION(year,month) SELECT * FROM ${db_name}.${table_name};"

# Rollback the table to the specified date
hive -e "CREATE TABLE ${rolled_back_table_name} LIKE ${db_name}.${table_name} STORED AS ORC; \
INSERT INTO ${rolled_back_table_name} PARTITION(year,month) \
SELECT * FROM ${backup_table_name} \
WHERE start_ts <= '${restore_date}' AND year = '${restore_year}' AND month = '${restore_month}';"

# Get the row counts for the original, backup, and rolled-back tables
original_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${db_name}.${table_name};")
backup_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${backup_table_name};")
rolled_back_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${rolled_back_table_name};")

# Print the row counts
echo "Original table row count: ${original_row_count}"
echo "Backup table row count: ${backup_row_count}"
echo "Rolled-back table row count: ${rolled_back_row_count}"

# Drop the temporary tables
hive -e "DROP TABLE ${backup_table_name};"
hive -e "DROP TABLE ${rolled_back_table_name};"
