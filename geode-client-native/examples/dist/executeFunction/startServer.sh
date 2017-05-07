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

if [ ! -d gfecs2 ]
then
  mkdir gfecs2
fi

   cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions.xml mcast-port=35673 -dir=gfecs

   cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions2.xml mcast-port=35673 -dir=gfecs2
