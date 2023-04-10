#!/bin/bash

# Set the name of the Hive database
db_name="my_database"

# Set the name of the external table
table_name="my_table"

# Set the location of the table data
table_location="/user/hive/warehouse/${db_name}.db/${table_name}"

# Set the date to restore to (in yyyy-MM-dd format)
restore_date="2022-03-15"

# Set the year and month of the restore date
restore_year=$(date -d "$restore_date" +%Y)
restore_month=$(date -d "$restore_date" +%m)

# Backup the partitioned data for the restore month
echo "Backing up partition for month $restore_month of year $restore_year..."
hive -e "USE ${db_name}; \
CREATE EXTERNAL TABLE ${table_name}_backup_${restore_year}_${restore_month} LIKE ${table_name}; \
INSERT INTO ${table_name}_backup_${restore_year}_${restore_month} \
SELECT * FROM ${table_name} WHERE start_ts >= '${restore_year}-${restore_month}-01' \
AND start_ts <= '${restore_year}-${restore_month}-31';"

# Drop the partition for the restore month
echo "Dropping partition for month $restore_month of year $restore_year..."
hive -e "USE ${db_name}; \
ALTER TABLE ${table_name} DROP IF EXISTS PARTITION (year=${restore_year},month=${restore_month});"

# Restore the backup data for the restore month
echo "Restoring partition for month $restore_month of year $restore_year..."
hive -e "USE ${db_name}; \
ALTER TABLE ${table_name} ADD PARTITION (year=${restore_year},month=${restore_month}) \
LOCATION '${table_location}/year=${restore_year}/month=${restore_month}'; \
INSERT INTO ${table_name} PARTITION (year=${restore_year},month=${restore_month}) \
SELECT * FROM ${table_name}_backup_${restore_year}_${restore_month};"

# Count the number of rows in the restored partition
restored_row_count=$(hive -S -e "USE ${db_name}; \
SELECT COUNT(*) FROM ${table_name} WHERE year=${restore_year} AND month=${restore_month};")

# Print the row count
echo "Restored partition row count: $restored_row_count"
