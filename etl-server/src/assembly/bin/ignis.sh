#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
IGNIS_HOME=`cd "$bin/.."; pwd`
. ${IGNIS_HOME}/conf/etl-env.sh

if [ "$IGNIS_PID_DIR" = "" ]; then
  IGNIS_PID_DIR=/tmp
fi
if [ "$IGNIS_LOG_DIR" = "" ]; then
  IGNIS_LOG_DIR=${bin}/../log
fi

if [ "${SPARK_HOME}" == "" ]; then
   echo "Error: SPARK_HOME is not set."
   exit 1
fi
if [ "${HADOOP_CONF_DIR}" == "" ]; then
   echo "Error: HADOOP_CONF_DIR is not set."
   exit 1
fi
if [ "${JAVA_HOME}" == "" ]; then
   echo "Error: JAVA_HOME is not set."
   exit 1
fi
if [ "${FLYWAY_HOME}" == "" ]; then
   echo "Error: FLYWAY_HOME is not set."
   exit 1
fi

PID="${IGNIS_PID_DIR}/etl-${USER}.pid"
LOG=$IGNIS_LOG_DIR/etl.log


usage() {
  echo "Usage: etl.sh {start|stop|restart|status}"
}

start() {
  if [ -f $PID ]; then
    if kill -0 `cat $PID` > /dev/null 2>&1; then
      echo etl running as process `cat $PID`.  Stop it first.
      exit 1
    fi
   fi
   JAVA=${JAVA_HOME}/bin/java
   JAR_FILE=`find ${IGNIS_HOME}/lib -maxdepth 1  -name "etl-*.jar" |xargs basename`
   JAR_FILE="${IGNIS_HOME}/lib/${JAR_FILE}"
   JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8787"
   JAVA_OPTS="$JAVA_OPTS -Xmx512m -Dspring.config.location=${IGNIS_HOME}/conf/application.properties"
   IGNIS_STOP_TIMEOUT=${IGNIS_STOP_TIMEOUT:-5}
   flywayScript="${FLYWAY_HOME}/flyway migrate"
   etlScript="${JAVA} ${JAVA_OPTS}  -jar ${JAR_FILE}"
   echo starting etl, logging to $LOG
   nohup ${etlScript} >> "$IGNIS_LOG_DIR/flyway.log" 2>&1 < /dev/null &
   nohup ${etlScript} >> "$LOG" 2>&1 < /dev/null &
   echo $! > $PID
}



status() {
  if [ -f $PID ]; then
    TARGET_PID=`cat $PID`
    if ps -p $TARGET_PID > /dev/null
    then
       echo "Ignis is running with pid $TARGET_PID"
    else
       echo "No etl is running with pid $TARGET_PID"
    fi
  else
    echo "No etl is running with pid $TARGET_PID"
  fi
}

stop() {
  if [ -f $PID ]; then
    TARGET_PID=`cat $PID`
    if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping etl
        kill $TARGET_PID
        sleep $IGNIS_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "etl did not stop gracefully after $IGNIS_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
    else
        echo no etl to stop
    fi
    rm -f $PID
  else
    echo no etl to stop
  fi
}

case "$1" in
  start)
        start;
        ;;
  restart)
        stop;
        start;
        ;;
  stop)
        stop
        ;;
  status)
        status
        ;;
  *)
        usage
        ;;
esac