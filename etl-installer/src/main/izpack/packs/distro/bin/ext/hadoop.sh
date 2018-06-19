#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

historyServer(){
  commandToRun=$1
  ssh ${SSH_OPTS} "${JOB_HISTORY_SERVER_HOST}" bash -c "'
      . ${APP_HOME}/conf/env.sh
      ${HADOOP_PREFIX}/sbin/mr-jobhistory-daemon.sh $commandToRun historyserver
  '" \
  2>&1 | sed "s/^/${JOB_HISTORY_SERVER_HOST}: /" &
}
start(){
   if [ "${DISTRIBUTED}" == "true" ]; then
      ${HADOOP_PREFIX}/sbin/hadoop-daemons.sh --hostnames "${HDFS_NAME_NODE_HOST}"  start namenode
      ${HADOOP_PREFIX}/sbin/hadoop-daemons.sh --hostnames "${HDFS_SNAME_NODE_HOST}" start secondarynamenode
      ${HADOOP_PREFIX}/sbin/yarn-daemons.sh --hostnames "${YARN_RESOURCE_MANAGER_HOST}" start resourcemanager
      historyServer start

      ${APP_HOME}/bin/ext/check-services.sh -m  hdfsMaster -c
      ${HADOOP_PREFIX}/sbin/hadoop-daemons.sh --hostnames "${HDFS_DATA_NODE_HOSTS}" start datanode
      ${APP_HOME}/bin/ext/check-services.sh -m yarnMaster -c
      ${HADOOP_PREFIX}/sbin/yarn-daemons.sh --hostnames "${YARN_NODE_MANAGER_HOSTS}" start nodemanager
  else
      ${HADOOP_PREFIX}/sbin/hadoop-daemon.sh start namenode
      ${HADOOP_PREFIX}/sbin/hadoop-daemon.sh start secondarynamenode
      ${HADOOP_PREFIX}/sbin/yarn-daemon.sh start resourcemanager
      ${HADOOP_PREFIX}/sbin/mr-jobhistory-daemon.sh start historyserver
      ${HADOOP_PREFIX}/sbin/hadoop-daemon.sh start datanode
      ${HADOOP_PREFIX}/sbin/yarn-daemon.sh start nodemanager
  fi
}

stop(){
   if [ "${DISTRIBUTED}" == "true" ]; then
      ${HADOOP_PREFIX}/sbin/hadoop-daemons.sh --hostnames "${HDFS_DATA_NODE_HOSTS}" stop datanode
      ${HADOOP_PREFIX}/sbin/yarn-daemons.sh --hostnames "${YARN_NODE_MANAGER_HOSTS}" stop nodemanager
      historyServer stop
      ${HADOOP_PREFIX}/sbin/yarn-daemons.sh --hostnames "${YARN_RESOURCE_MANAGER_HOST}" stop resourcemanager
      ${HADOOP_PREFIX}/sbin/hadoop-daemons.sh --hostnames "${HDFS_SNAME_NODE_HOST}" stop secondarynamenode
      ${HADOOP_PREFIX}/sbin/hadoop-daemons.sh --hostnames "${HDFS_NAME_NODE_HOST}"  stop namenode
   else
      ${HADOOP_PREFIX}/sbin/hadoop-daemon.sh stop datanode
      ${HADOOP_PREFIX}/sbin/yarn-daemon.sh stop nodemanager
      ${HADOOP_PREFIX}/sbin/mr-jobhistory-daemon.sh stop historyserver
      ${HADOOP_PREFIX}/sbin/yarn-daemon.sh stop resourcemanager
      ${HADOOP_PREFIX}/sbin/hadoop-daemon.sh stop secondarynamenode
      ${HADOOP_PREFIX}/sbin/hadoop-daemon.sh stop namenode
   fi
}

usage(){
   echo "Usage: hadoop.sh start|stop|status"
}

case "$1" in
  start)
     start;
     ;;
  stop)
     stop;
     ;;
  status)
     ${APP_HOME}/bin/ext/check-services.sh -m hadoop;
     ;;
  *)
     usage;
     ;;
esac