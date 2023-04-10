#!/bin/bash

# Set the name of the Hive database
db_name="dev"

# Set the name of the Hive table
table_name="my_table"

# Set the partition columns
partition_cols=("year" "month")

# Set the rollback date (in yyyy-MM-dd format)
rollback_date="2022-03-01"

# Set the backup table name and location
backup_table_name="${table_name}_backup"
backup_table_location="/user/hive/warehouse/${db_name}.db/${backup_table_name}"

# Set the rolled-back table name and location
rolled_back_table_name="${table_name}_rolled_back"
rolled_back_table_location="/user/hive/warehouse/${db_name}.db/${rolled_back_table_name}"

# Backup the data in the table
hive -e "CREATE TABLE ${db_name}.${backup_table_name} LIKE ${db_name}.${table_name}; \
INSERT INTO ${db_name}.${backup_table_name} PARTITION(year,month) SELECT * FROM ${db_name}.${table_name};"

# Rollback the data in the table to the specified date
hive -e "CREATE TABLE ${db_name}.${rolled_back_table_name} LIKE ${db_name}.${table_name}; \
INSERT INTO ${db_name}.${rolled_back_table_name} PARTITION(year,month) \
SELECT * FROM ${db_name}.${backup_table_name} \
WHERE load_ts <= '${rollback_date}' AND year = '2022' AND month = '03';"

# Count the number of rows in the backup table and the rolled-back table
backup_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${db_name}.${backup_table_name};")
rolled_back_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${db_name}.${rolled_back_table_name};")

# Print the row counts
echo "Backup table row count: ${backup_row_count}"
echo "Rolled-back table row count: ${rolled_back_row_count}"
