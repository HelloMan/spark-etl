#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh

config(){
  rm ${SPARK_HOME}/jars/guava-14.0.1.jar
}

echo "Starting config spark, log to ${APP_HOME}/logs/install.log"
config >> ${APP_HOME}/logs/install.log 2>&1