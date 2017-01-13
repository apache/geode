#!/bin/sh

if [ -z ${GFCCLIENT:-} ]; then
    echo GFCCLIENT is not set.
    exit 1
fi
if [ -z ${GEMFIRE:-} ]; then
    echo GEMFIRE is not set.
    exit 1
fi


export LD_LIBRARY_PATH=$GFCCLIENT:$LD_LIBRARY_PATH
$GEMFIRE/bin/cacheserver start cache-xml-file=sampleserver.xml log-level=all
./sample
$GEMFIRE/bin/cacheserver stop

