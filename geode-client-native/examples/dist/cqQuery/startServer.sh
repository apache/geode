#!/bin/bash

if [ -z ${GEMFIRE:-} ]; then
  echo GEMFIRE is not set.
  exit 1
fi

echo Running GemFire Server

export CLASSPATH="${CLASSPATH}:../javaobject.jar"
export PATH="${PATH}:${GEMFIRE}/bin"

if [ ! -d gfecs ]
then
  mkdir gfecs
fi

   cacheserver start cache-xml-file=../XMLs/serverCqQuery.xml mcast-port=35673 -dir=gfecs

