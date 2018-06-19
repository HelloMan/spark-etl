#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

formatHadoop(){
  if [ ! -d ${DATA_PATH}/hdfs/namenode/current ]; then
     ${HADOOP_PREFIX}/bin/hdfs namenode -format
  fi
}

config(){
  formatHadoop
}

echo "Starting config hadoop, log to ${APP_HOME}/logs/install.log"
config   >> ${APP_HOME}/logs/install.log 2>&1
