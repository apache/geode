#!/bin/bash

TC=$1
PORT=`expr $UID + 2000`
TYPE=$2

echo "creating $TC for isolation: PORT=$PORT TYPE=$TYPE."

echo "context resolver_multicast_port $PORT" >$TC

