#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`
cmdname=$(basename $0)

if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo found java executable in JAVA_HOME
    _java="$JAVA_HOME/bin/java"
else
    echo "no java"
    exit 1
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ "$version" < "1.8" ]]; then
        echo Java version can not be less than 1.8
        exit 1
    fi
fi


usage()
{
    cat << USAGE >&2
Usage:
    $cmdname [-p ] [- i ] [-d ]
      -p properties file
      -i install path
      -d debug port
USAGE
    exit 1
}

# process arguments
while [[ $# -gt 0 ]]
do
    case "$1" in
      -p)
      PROP_FILE="$2"
      if [[ "$PROP_FILE" == "" ]]; then
         echo "Error:Properties file can not be empty"
         exit 1
      fi
      shift 2
      ;;
      -d)
      DEBUG_PORT="$2"
       if [[ "$DEBUG_PORT" == "" ]]; then
          echo "Error:Debug port can not be empty"
         exit 1
      fi
      shift 2
      ;;
      -i)
      INSTALL_PATH="$2"
      if [[ "$INSTALL_PATH" == "" ]]; then
         echo "Install path can not be empty"
         exit 1
      fi
      shift 2
      ;;
      -h)
      usage;
      ;;
      *)
      usage
      ;;
    esac
done
#This line will be replace by maven resource plugin
ARTIFACT_NAME=${artifactId}-${project.version}
INSTALL_PATH=${INSTALL_PATH:-${dir}/${ARTIFACT_NAME}}
PROP_FILE=${PROP_FILE:-""}
JAR_FILE=${ARTIFACT_NAME}.jar
if [  ! -f ${JAR_FILE} ]; then
  echo "Error: No such file '$JAR_FILE'"
  exit 1
fi


JAVA_OPTS="-DSTACKTRACE=true"
if [ "${DEBUG_PORT}" != "" ]; then
  JAVA_OPTS="$JAVA_OPTS  -DDEBUG=true -DTRACE=true -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=${DEBUG_PORT}"
fi

if [ "${PROP_FILE}" != "" ]; then
  if [ -e ${PROP_FILE} ]; then
     echo "Starting install FCR Engine with silent mode..."
    JAVA_OPTS="$JAVA_OPTS -DuseDefaultsFile=true "
    $_java $JAVA_OPTS -jar $JAR_FILE -defaults-file $PROP_FILE -auto
  else
    PROP_FILE="${dir}/${PROP_FILE}"
     if [ -e ${PROP_FILE} ]; then
       JAVA_OPTS="$JAVA_OPTS -DuseDefaultsFile=true "
       $_java $JAVA_OPTS -jar $JAR_FILE -defaults-file $PROP_FILE -auto
     else
       echo "Error:No such file '$PROP_FILE'"
       exit 1
     fi
  fi
else
  PROP_FILE="${INSTALL_PATH}"/conf/system.properties
  if [ -e $PROP_FILE ]; then
    JAVA_OPTS="$JAVA_OPTS -DuseDefaultsFile=true"
    $_java $JAVA_OPTS -DINSTALL_PATH=$INSTALL_PATH -jar $JAR_FILE -defaults-file $PROP_FILE
  else
    $_java $JAVA_OPTS -DINSTALL_PATH=$INSTALL_PATH -jar $JAR_FILE
  fi
fi