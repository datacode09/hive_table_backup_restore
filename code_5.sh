#!/bin/bash

# Set the name of the Hive database
db_name="dev"

# Set the name of the external Hive table
table_name="my_external_table"

# Set the partition columns
partition_cols=("year" "month")

# Set the restore date (in yyyy-MM-dd format)
restore_date="2022-03-01"

# Set the backup table name and location
backup_table_name="${table_name}_backup"
backup_table_location="/path/to/backup/table"

# Set the location of the table data to restore
restore_location="/path/to/restore/data"

# Set the list of partitions to restore
partitions_to_restore=("year=2022/month=03" "year=2022/month=02")

# Create the backup table if it doesn't exist
if ! hive -e "USE ${db_name}; DESCRIBE ${backup_table_name};" >/dev/null 2>&1; then
  hive -e "CREATE EXTERNAL TABLE ${db_name}.${backup_table_name} LIKE ${db_name}.${table_name} LOCATION '${backup_table_location}';"
fi

# Backup the data for the specified partitions
for partition in "${partitions_to_restore[@]}"; do
  hive -e "INSERT INTO ${db_name}.${backup_table_name} PARTITION(${partition}) SELECT * FROM ${db_name}.${table_name} WHERE ${partition};"
done

# Restore the data for the specified partitions to the restore location
for partition in "${partitions_to_restore[@]}"; do
  restore_partition_path="${restore_location}/${partition}"
  backup_partition_path="${backup_table_location}/${partition}"
  hdfs dfs -rm -r -skipTrash "${restore_partition_path}"
  hdfs dfs -cp "${backup_partition_path}" "${restore_partition_path}"
done

echo "Table restored successfully!"
