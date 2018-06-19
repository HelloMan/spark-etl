#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "${bin}"/../conf/env.sh

commandToRun=$1

service=$2

usage(){
   echo "Usage: daemon.sh (start|stop|status) (hadoop|zookeeper|hbase|phoenix|etl|all)"
}

stopAll(){
  ${bin}/ext/etl.sh stop;
  ${bin}/ext/phoenix.sh stop;
  ${bin}/ext/hbase.sh stop;
  ${bin}/ext/hadoop.sh stop;
  ${bin}/ext/zookeeper.sh stop;
}

startAll(){
  ${bin}/ext/zookeeper.sh start;
  ${bin}/ext/hadoop.sh start;
  ${bin}/ext/hbase.sh start;
  ${bin}/ext/phoenix.sh start;
  ${bin}/ext/etl.sh start;
}
case "$service" in
  hadoop)
     ${bin}/ext/hadoop.sh $commandToRun
     ;;
  zookeeper)
     ${bin}/ext/zookeeper.sh $commandToRun;
     ;;
  hbase)
     ${bin}/ext/hbase.sh $commandToRun;
     ;;
  phoenix)
     ${bin}/ext/phoenix.sh $commandToRun;
     ;;
  etl)
     ${bin}/ext/etl.sh $commandToRun;
     ;;
  all)
     if [ "$commandToRun" == "status" ]; then
        ${bin}/ext/check-services.sh  -m all
     else
        if [ "$commandToRun" == "stop" ]; then
           stopAll
        else
           startAll
        fi
     fi
     ;;
  *)
     usage;
     ;;
esac

