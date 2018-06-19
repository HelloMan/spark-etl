#!/bin/bash
dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`

. "${dir}"/../../conf/env.sh


case "$1" in
  start)
     docker run --name etl-mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=etl -e MYSQL_USER=etl -e MYSQL_PASSWORD=password -p 3306:3306 -v ${DATA_PATH}/mysql:/var/lib/mysql -d mysql:5.7
     ;;
  stop)
     docker stop etl-mysql;
     docker rm etl-mysql;
     ;;
  status)
     echo "TODO for MYSQL"
     ;;
  *)
     usage;
     ;;
esac


