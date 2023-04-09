#!/bin/bash

# Define the table name and Hive database
TABLE_NAME="my_table"
DATABASE_NAME="my_database"

# Define the backup location for the table
BACKUP_LOCATION="/mnt/backups/hive_tables"

# Check if the backup location exists and is writable
if [ ! -w "$BACKUP_LOCATION" ]; then
  echo "Error: Backup location $BACKUP_LOCATION does not exist or is not writable"
  exit 1
fi

# Stop the Hive metastore service to prevent any updates to the table
sudo service hive-metastore stop

# Create a backup of the table data and metadata
hive -e "use $DATABASE_NAME; show create table $TABLE_NAME" > "$BACKUP_LOCATION/$TABLE_NAME.ddl"
hive -e "use $DATABASE_NAME; show partitions $TABLE_NAME" | while read partition; do
  hive -e "use $DATABASE_NAME; set hive.exec.dynamic.partition.mode=nonstrict; show create table $TABLE_NAME partition($partition)" > "$BACKUP_LOCATION/$TABLE_NAME.$partition.ddl"
  hive -e "use $DATABASE_NAME; set hive.exec.dynamic.partition.mode=nonstrict; insert overwrite local directory '$BACKUP_LOCATION/$TABLE_NAME/$partition' select * from $TABLE_NAME where $partition"
done

# Start the Hive metastore service
sudo service hive-metastore start

# Verify the backup
if [ -f "$BACKUP_LOCATION/$TABLE_NAME.ddl" ]; then
  echo "Table backup created successfully"
else
  echo "Error: Failed to create table backup"
  exit 1
fi
