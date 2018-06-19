#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

usage(){
  echo "Usage: hbase.sh start|stop|status"
}



start() {
  if [ "${DISTRIBUTED}" == "true" ]; then
       #check hdfs
       ${APP_HOME}/bin/ext/check-services.sh -m hdfsMaster -c
       ssh ${SSH_OPTS} ${HBASE_MASTER_HOST} bash -c "'
           . ${APP_HOME}/conf/env.sh
           ${HBASE_HOME}/bin/start-hbase.sh
       '" 2>&1 | sed "s/^/${HBASE_MASTER_HOST}: /" &
   else
     ${HBASE_HOME}/bin/start-hbase.sh
   fi
}
stop() {
  if [ "${DISTRIBUTED}" == "true" ]; then
       ssh ${SSH_OPTS} ${HBASE_MASTER_HOST} bash -c "'
          . ${APP_HOME}/conf/env.sh
          ${HBASE_HOME}/bin/stop-hbase.sh
       '" 2>&1 | sed "s/^/${HBASE_MASTER_HOST}: /"
   else
      ${HBASE_HOME}/bin/stop-hbase.sh
   fi
}


case "$1" in
  start)
     start;
     ;;
  stop)
     stop;
     ;;
  status)
     ${APP_HOME}/bin/ext/check-services.sh -m hbase;
     ;;
  *)
    usage;
     ;;
esac