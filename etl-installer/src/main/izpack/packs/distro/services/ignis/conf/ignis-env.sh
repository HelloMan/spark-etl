#!/bin/bash
export JAVA_HOME=%{JAVA_HOME}
export HADOOP_CONF_DIR=%{HADOOP_HOME}/etc/hadoop
export SPARK_HOME=%{SPARK_HOME}
export FLYWAY_HOME=%{FLYWAY_HOME}
export IGNIS_PID_DIR="%{INSTALL_PATH}"/tmp