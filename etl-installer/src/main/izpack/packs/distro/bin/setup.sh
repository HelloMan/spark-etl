#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

java -DINSTALL_PATH="${bin}"/.. \
     -jar "${bin}"/../lib/etl-setup.jar \
     -defaults-file  "${bin}"/../conf/system.properties