#!/bin/bash

# Set the name of the Hive database
db_name="my_db"

# Set the name of the Hive table
table_name="my_table"

# Set the partition columns
partition_cols=("year" "month")

# Set the date to restore to (in yyyy-MM-dd format)
restore_date="2022-02-15"

# Convert the restore date to partition year and month
restore_year=$(date -d "$restore_date" +%Y)
restore_month=$(date -d "$restore_date" +%m)

# Create a backup of the table
hive -e "CREATE TABLE ${db_name}.${table_name}_backup LIKE ${db_name}.${table_name}; \
INSERT OVERWRITE TABLE ${db_name}.${table_name}_backup \
PARTITION(year,month) \
SELECT * FROM ${db_name}.${table_name};"

# Restore the table to the specified date
hive -e "CREATE TABLE ${db_name}.${table_name}_restore LIKE ${db_name}.${table_name}; \
INSERT OVERWRITE TABLE ${db_name}.${table_name}_restore \
PARTITION(year,month) \
SELECT * FROM ${db_name}.${table_name}_backup \
WHERE year*100+month >= ${restore_year}*100+${restore_month};"

# Drop the original table and rename the restore table
hive -e "DROP TABLE ${db_name}.${table_name}; \
ALTER TABLE ${db_name}.${table_name}_restore RENAME TO ${table_name};"

# Count the number of rows in the backup table and the restored table
backup_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${db_name}.${table_name}_backup;")
restored_row_count=$(hive -S -e "SELECT COUNT(*) FROM ${db_name}.${table_name};")

# Print the row counts
echo "Backup table row count: ${backup_row_count}"
echo "Restored table row count: ${restored_row_count}"
