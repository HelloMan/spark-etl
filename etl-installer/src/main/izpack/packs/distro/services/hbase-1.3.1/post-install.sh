#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

config(){
   #create backup-masters file
   echo > ${HBASE_HOME}/conf/backup-masters
   echo > ${HBASE_HOME}/conf/regionservers
   if [ "${DISTRIBUTED}" == "true" ]; then
       for host in $HBASE_BACKUP_MASTER_HOSTS; do
         echo $host >> ${HBASE_HOME}/conf/backup-masters
       done

       #create regionservers file
       for host in $HBASE_REGION_SERVER_HOSTS; do
         echo $host >>${HBASE_HOME}/conf/regionservers
       done
   else
       echo localhost > ${HBASE_HOME}/conf/regionservers
   fi
}



echo "Starting config hbase, log to ${APP_HOME}/logs/install.log"
config  >> ${APP_HOME}/logs/install.log 2>&1
