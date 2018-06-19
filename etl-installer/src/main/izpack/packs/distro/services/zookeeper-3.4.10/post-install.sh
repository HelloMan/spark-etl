#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

config(){
  # append "server.id=ip:2888:3888" to the end of  file of zoo.cfg
   if [ "${DISTRIBUTED}" == "true" ]; then
       zk_hosts=($ZOOKEEPER_HOSTS)
       for index in ${!zk_hosts[@]}; do
          host=${zk_hosts[$index]}
          serverId=$(($index+1))
          echo "server.$serverId=$host:2888:3888" >> ${ZOOKEEPER_HOME}/conf/zoo.cfg
       done
   fi
}


echo "Starting config zookeeper, log to  ${APP_HOME}/logs/install.log"
config >> ${APP_HOME}/logs/install.log 2>&1
