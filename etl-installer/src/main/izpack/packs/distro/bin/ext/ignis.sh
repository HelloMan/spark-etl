#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh
usage(){
   echo "Usage: etl.sh start|stop|status"
}

daemon() {
   commandToRun=$1
   if [ "${DISTRIBUTED}" == "true" ]; then
      ssh ${SSH_OPTS} ${IGNIS_HOST} bash -c "'
         . ${APP_HOME}/conf/env.sh
         ${IGNIS_HOME}/bin/etl.sh $commandToRun
      '" 2>&1 | sed "s/^/${IGNIS_HOST}: /" &
   else
      ${IGNIS_HOME}/bin/etl.sh $commandToRun
   fi
}

case "$1" in
  start)
     daemon start;
     ;;
  stop)
     daemon stop;
     ;;
  status)
     ${APP_HOME}/bin/ext/check-services.sh -m etl;
     ;;
  *)
     usage;
     ;;
esac