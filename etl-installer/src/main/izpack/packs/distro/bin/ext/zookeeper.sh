#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

usage(){
  echo "Usage: zookeeper.sh start|stop|status"
}


daemon() {
  commandToRun=$1
  if [ "${DISTRIBUTED}" == "true" ]; then
       for zkHost in ${ZOOKEEPER_HOSTS}; do
           ssh ${SSH_OPTS} ${zkHost} bash -c "'
              . ${APP_HOME}/conf/env.sh
              ${ZOOKEEPER_HOME}/bin/zkServer.sh $commandToRun
           '" 2>&1 | sed "s/^/${zkHost}: /" &
       done
   else
     ${ZOOKEEPER_HOME}/bin/zkServer.sh $commandToRun
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
     ${APP_HOME}/bin/ext/check-services.sh -m zookeeper;
     ;;
  *)
    usage;
    ;;
esac