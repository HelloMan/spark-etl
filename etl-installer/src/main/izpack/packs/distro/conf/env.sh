#!/bin/bash
#export all services home directory
export JAVA_HOME=%{JAVA_HOME}
export APP_HOME="%{INSTALL_PATH}"
export APP_CONF_DIR="%{INSTALL_PATH}"/conf
export HADOOP_PREFIX="%{HADOOP_HOME}"
export HADOOP_CONF_DIR="%{HADOOP_HOME}"/etc/hadoop
export HADOOP_HOME="%{HADOOP_HOME}"
export HBASE_HOME="%{HBASE_HOME}"
export HBASE_CONF_DIR="%{HBASE_HOME}"/conf
export SPARK_HOME="%{SPARK_HOME}"
export PHOENIX_HOME="%{PHOENIX_HOME}"
export PHOENIX_CONF_DIR="%{PHOENIX_HOME}"/conf
export ZOOKEEPER_HOME="%{ZOOKEEPER_HOME}"
export ZOOKEEPER_CONF_DIR="%{ZOOKEEPER_HOME}"/conf
export FLYWAY_HOME="%{FLYWAY_HOME}"
export SSH_OPTS="%{SSH_OPTS}"

export IGNIS_HOME="%{IGNIS_HOME}"
export IGNIS_CONF_DIR="%{IGNIS_HOME}"/conf
export DATA_PATH="%{DATA_PATH}"
export DISTRIBUTED=%{distributed}
export IGNIS_HTTP_PORT=%{etl.http.port}
export IGNIS_HTTPS_PORT=%{etl.https.port}

#export all services's master  hosts
if [ "${DISTRIBUTED}" == "false" ]; then
    export HDFS_NAME_NODE_HOST=localhost
    export HDFS_DATA_NODE_HOSTS=localhost
    export HDFS_SNAME_NODE_HOST=localhost
    export YARN_RESOURCE_MANAGER_HOST=localhost
    export YARN_NODE_MANAGER_HOSTS=localhost
    export JOB_HISTORY_SERVER_HOST=localhost
    export HBASE_MASTER_HOST=localhost
    export HBASE_BACKUP_MASTER_HOSTS=localhost
    export HBASE_REGION_SERVER_HOSTS=localhost
    export IGNIS_HOST=localhost
    export ZOOKEEPER_HOSTS=localhost
    export PHOENIX_QUERY_SERVER_HOSTS=localhost
else
    export HDFS_NAME_NODE_HOST=%{hdfs.nn.host}
    export HDFS_SNAME_NODE_HOST=%{hdfs.sn.host}
    export YARN_RESOURCE_MANAGER_HOST=%{yarn.rm.host}
    export JOB_HISTORY_SERVER_HOST=%{history.server.host}
    export HBASE_MASTER_HOST=%{hbase.master.host}
    export IGNIS_HOST=%{etl.host}
    export HBASE_BACKUP_MASTER_HOSTS="%{hbase.backup.master.hosts}"
    export HBASE_BACKUP_MASTER_HOSTS="${HBASE_BACKUP_MASTER_HOSTS//,/ }"
    #export all services's slaves  hosts
    export HDFS_DATA_NODE_HOSTS="%{hdfs.dn.hosts}"
    export HDFS_DATA_NODE_HOSTS="${HDFS_DATA_NODE_HOSTS//,/ }"
    export YARN_NODE_MANAGER_HOSTS="%{yarn.nm.hosts}"
    export YARN_NODE_MANAGER_HOSTS="${YARN_NODE_MANAGER_HOSTS//,/ }"
    export HBASE_REGION_SERVER_HOSTS="%{hbase.regionserver.hosts}"
    export HBASE_REGION_SERVER_HOSTS="${HBASE_REGION_SERVER_HOSTS//,/ }"
    export ZOOKEEPER_HOSTS="%{zookeeper.hosts}"
    export ZOOKEEPER_HOSTS="${ZOOKEEPER_HOSTS//,/ }"
    export PHOENIX_QUERY_SERVER_HOSTS="%{phoenix.qs.hosts}"
    export PHOENIX_QUERY_SERVER_HOSTS="${PHOENIX_QUERY_SERVER_HOSTS//,/ }"
fi





