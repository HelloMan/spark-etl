#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`
cmdname=$(basename $0)

. "${dir}"/../../conf/env.sh

usage()
{
    cat << USAGE >&2
Usage:
    $cmdname [-m module] [-p|-c] [-t timeout]
    -m MODULE
               Module=(hdfsMaster|yarnMaster|hbaseMaster|phoenix|zookeeper|etl|hadoop|hbase|all)
    -p         Print module status
    -c         Check module status
    -t TIMEOUT
              Timeout in seconds, zero for no timeout (only avaliable with -c option)
USAGE
    exit 1
}

lineFormat="|%-15s|%-30s|%-30s|%-6s|\n"

printLine(){
  if [ $PRINT -eq 1 ]; then
    awk 'BEGIN{OFS="-";NF=87;print}'
  fi
}

printHeader(){
   if [ $PRINT -eq 1 ]; then
       awk 'BEGIN{OFS="-";NF=87;print}'
       printf "${lineFormat}" "Service" "Component" "Host" "Status"
       printLine
   fi
}

printFooter(){
   return
}

checkService(){
    serviceName=$1
    serviceComponent=$2
    serviceHost=$3
    servicePort=$4
    if [ ${PRINT} -eq 1 ]; then
      "${dir}"/../common/wait-for-it.sh ${serviceHost}:${servicePort} -q -t 1
    else
      "${dir}"/../common/wait-for-it.sh ${serviceHost}:${servicePort} -q -t ${TIMEOUT}
    fi
    RESULT=$?
    if [[ ${PRINT} -eq 1 ]]; then
        if [ ${RESULT} -eq 0 ]; then
          printf "${lineFormat}" "${serviceName}" "${serviceComponent}" "${serviceHost}" "Up"
        else
          printf "${lineFormat}" "${serviceName}" "${serviceComponent}" "${serviceHost}" "Down"
        fi
    else
       if [ ${RESULT} -eq 0 ]; then
          echo "${serviceName}:${serviceComponent} on ${serviceHost}:${servicePort} is started"
       else
          echo "${serviceName}:${serviceComponent} on ${serviceHost}:${servicePort} not started"
       fi
    fi
    printLine
}

checkHdfsMaster(){
    checkService "Hdfs" "NameNode" ${HDFS_NAME_NODE_HOST}  50070
    checkService "Hdfs" "SecondaryNameNode" ${HDFS_SNAME_NODE_HOST}  50090
}

checkHdfsSlaves(){
  for dnHost in ${HDFS_DATA_NODE_HOSTS}; do
      checkService "Hdfs" "DataNode" ${dnHost} 50075
   done
}

checkYarnMaster(){
  checkService "Yarn"  "ResourceManager" ${YARN_RESOURCE_MANAGER_HOST} 8088
  checkService "Yarn"  "JobHistoryServer" ${JOB_HISTORY_SERVER_HOST} 19888
}

checkYarnSlaves(){
   for host in ${YARN_NODE_MANAGER_HOSTS}; do
      checkService "Yarn" "NodeManager" ${host} 8042
   done
}

checkHadoop(){
   checkHdfsMaster
   checkHdfsSlaves
   checkYarnMaster
   checkYarnSlaves
}

checkHBaseMaster(){
  hmasterPort=16010
  checkService "HBase" "HMaster" ${HBASE_MASTER_HOST} $hmasterPort
  if [ "${DISTRIBUTED}" == "true" ]; then
      for host in ${HBASE_BACKUP_MASTER_HOSTS}; do
        checkService "HBase" "HMaster" ${host} $hmasterPort
      done
  fi
}

checkHBaseSlaves(){
   regionServerPort=16030
   if [ "${DISTRIBUTED}" == "false" ]; then
      regionServerPort=16301
   fi
   for host in ${HBASE_REGION_SERVER_HOSTS}; do
     checkService "HBase" "HRegionServer" ${host} $regionServerPort
   done
}

checkHBase(){
   checkHBaseMaster
   checkHBaseSlaves
}

checkPhoenix(){
   for host in ${PHOENIX_QUERY_SERVER_HOSTS}; do
        checkService "Phoenix" "QueryServer" ${host} 8765
   done

}

checkZookeeper(){
  for host in ${ZOOKEEPER_HOSTS}; do
     checkService "Zookeeper" "Zookeeper" ${host} 2181
  done
}


checkIgnis(){
  checkService "Ignis" "Ignis" ${IGNIS_HOST} ${IGNIS_HTTP_PORT}
}


checkAll(){
  checkZookeeper
  checkHadoop
  checkHBase
  checkPhoenix
  checkIgnis

}


# process arguments
while [[ $# -gt 0 ]]
do
    case "$1" in
        -m)
        MODULE="$2"
        if [[ $MODULE == "" ]]; then break; fi
        shift 2
        ;;
        -p)
        PRINT=1
        CHECK=0
        shift 1
        ;;
        -c)
        CHECK=1
        PRINT=0
        shift 1
        ;;
        -t)
        TIMEOUT="$2"
        shift 2
        ;;
        -h)
        usage;
        exit 1
        ;;
        *)
        echo  "Error:Unknown argument: $1"
        usage
        exit 1
        ;;
    esac
done

MODULE=${MODULE:-"all"}
TIMEOUT=${TIMEOUT:-15}
CHECK=${CHECK:-0}
PRINT=${PRINT:-1}

case "${MODULE}" in
  hdfsMaster)
     printHeader;
     checkHdfsMaster;
     printFooter;
     ;;
  yarnMaster)
     printHeader;
     checkYarnMaster;
     printFooter;
     ;;
  hbaseMaster)
     printHeader;
     checkHBaseMaster;
     printFooter;
     ;;
  zookeeper)
     printHeader;
     checkZookeeper;
     printFooter;
     ;;
  hadoop)
     printHeader;
     checkHadoop;
     printFooter;
     ;;
  hbase)
     printHeader;
     checkHBase;
     printFooter;
     ;;
  phoenix)
     printHeader;
     checkPhoenix;
     printFooter;
     ;;
  etl)
     printHeader;
     checkIgnis;
     printFooter;
     ;;
  all)
     printHeader;
     checkAll;
     printFooter;
     ;;
  *)
     usage;
     ;;
esac