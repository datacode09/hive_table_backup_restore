#!/bin/bash

# Set the name of the source table and the backup table
SRC_TABLE=my_table
BKUP_TABLE=my_table_bkup

# Set the HDFS path where the backup will be stored
BKUP_PATH=/path/to/backup/dir

# Set the date to which you want to restore the table
RESTORE_DATE=2022-03-01

# Set the path and name of the log file
LOG_FILE=/path/to/log/file.log

# Start logging
echo "$(date): Starting backup of ${SRC_TABLE} to ${BKUP_TABLE}" >> ${LOG_FILE}

# Take backup of the source table
hive -e "CREATE TABLE ${BKUP_TABLE} LIKE ${SRC_TABLE}; INSERT INTO ${BKUP_TABLE} SELECT * FROM ${SRC_TABLE};"

# Verify backup success
if [ $? -eq 0 ]; then
    echo "$(date): Backup of ${SRC_TABLE} to ${BKUP_TABLE} completed successfully" >> ${LOG_FILE}
else
    echo "$(date): Backup of ${SRC_TABLE} to ${BKUP_TABLE} failed" >> ${LOG_FILE}
fi

# Start logging
echo "$(date): Starting restore of ${BKUP_TABLE} to ${SRC_TABLE} for date ${RESTORE_DATE}" >> ${LOG_FILE}

# Restore the backup to an earlier date
hive -e "USE default; ALTER TABLE ${BKUP_TABLE} SET LOCATION '${BKUP_PATH}/${SRC_TABLE}/date=${RESTORE_DATE}';"

# Verify restore success
if [ $? -eq 0 ]; then
    echo "$(date): Restore of ${BKUP_TABLE} to ${SRC_TABLE} for date ${RESTORE_DATE} completed successfully" >> ${LOG_FILE}
else
    echo "$(date): Restore of ${BKUP_TABLE} to ${SRC_TABLE} for date ${RESTORE_DATE} failed" >> ${LOG_FILE}
fi

# Verify that the table has been restored to the correct date
hive -e "USE default; SELECT COUNT(*) FROM ${SRC_TABLE} WHERE date='${RESTORE_DATE}';"

# Verify data restore success
if [ $? -eq 0 ]; then
    echo "$(date): Data for ${SRC_TABLE} has been restored to date ${RESTORE_DATE}" >> ${LOG_FILE}
else
    echo "$(date): Data for ${SRC_TABLE} could not be restored to date ${RESTORE_DATE}" >> ${LOG_FILE}
fi
