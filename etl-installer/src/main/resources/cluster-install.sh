#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`
cmdname=$(basename $0)
ARTIFACT_NAME=${artifactId}-${project.version}
JAR_FILE=${ARTIFACT_NAME}.jar


usage()
{
    cat << USAGE >&2
Usage:
    $cmdname -p  [- w ] [-u ] [-o ]
      -p  Required: absolute path of Properties file  for install with silent mode
      -w  Optional: remote working directory,default to current directory
      -u  Optional: remote user ,default to current user
      -o  Optional: ssh options,default using  password-less ssh
USAGE
    exit 1
}


# process arguments
while [[ $# -gt 0 ]]
do
    case "$1" in
      -w)
      WORKING_PATH=$2
       if [[ "$WORKING_PATH" == "" ]]; then
         echo "Error:Remote working path can not be empty"
         exit 1
      fi
      shift 2
      ;;
      -p)
      PROP_FILE="$2"
      if [[ "$PROP_FILE" == "" ]]; then
         echo "Error:Properties file can not be empty"
         exit 1
      fi
      shift 2
      ;;
      -u)
      REMOTE_USER=$2
      if [[ "$REMOTE_USER" == "" ]]; then
         echo "Error:Remote user can not be empty"
         exit 1
      fi
      shift 2
      ;;
      -h)
      usage;
      ;;
      -o)
      SSH_OPTS=$2
       if [[ "$SSH_OPTS" == "" ]]; then
         echo "Error:SSH options can not be empty"
         exit 1
      fi
      shift 2
      ;;
      *)
      usage
      ;;
    esac
done
#if no use specify ,using same as current user
REMOTE_USER=${REMOTE_USER:-${USER}}
#if no working path specify, using current path
WORKING_PATH=${WORKING_PATH:-${dir}}
SSH_OPTS=${SSH_OPTS:-""}

if [[ "$PROP_FILE" == "" ]]; then
     echo "Error:Properties file can not be empty"
     exit 1
fi
PROP_FILE_NAME=$(basename ${PROP_FILE})
dos2unix $PROP_FILE

#load properties file as environment variable
hosts=`sed '/^\#/d' $PROP_FILE | grep 'hosts'  | tail -n 1 | cut -d "=" -f2-`
#replace "," with ""
hosts="${hosts//,/ }"

for host in ${hosts}; do
  echo "Synchronized directory from ${dir} to a remote host($host) directory ${WORKING_PATH} "
  ssh ${SSH_OPTS} ${REMOTE_USER}@${host}  bash -c "'
     if [ ! -d "${WORKING_PATH}" ]; then
       echo create directory ${WORKING_PATH}
       mkdir ${WORKING_PATH}
     fi
  '"  2>&1 | sed "s/^/${host}: /"

  rsync -avzopg --progress  "${dir}"/install.sh  "${dir}"/cluster-install.sh  "${dir}"/${JAR_FILE} "${PROP_FILE}" ${REMOTE_USER}@${host}:${WORKING_PATH}
done

echo ----------Starting Installing FCR Engine ------------------
for host in ${hosts}; do
  ssh ${SSH_OPTS} ${REMOTE_USER}@${host}  bash -c "'
       cd "${WORKING_PATH}"
       chmod u+x install.sh
       chmod u+x cluster-install.sh
       ./install.sh -p ./${PROP_FILE_NAME}
  '" 2>&1 | sed "s/^/${host}: /"
done