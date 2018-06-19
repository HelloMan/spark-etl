#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

usage(){
  echo "Usage: phoenix.sh start|stop|status"
}

daemon() {
   commandToRun=$1
   if [ "${DISTRIBUTED}" == "true" ]; then
       for qsHost in ${PHOENIX_QUERY_SERVER_HOSTS}; do
           ssh ${SSH_OPTS} ${qsHost} bash -c "'
              . ${APP_HOME}/conf/env.sh
              ${PHOENIX_HOME}/bin/queryserver.py $commandToRun
           '" 2>&1 | sed "s/^/${qsHost}: /" &
       done
   else
     ${PHOENIX_HOME}/bin/queryserver.py $commandToRun
   fi
}

case "$1" in
  start)
     ${APP_HOME}/bin/ext/check-services.sh -m hbaseMaster -c
     daemon start;
     ;;
  stop)
     daemon stop;
     ;;
  status)
     ${APP_HOME}/bin/ext/check-services.sh -m phoenix;
     ;;
  *)
     usage;
     ;;
esac