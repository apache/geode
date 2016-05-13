#!/bin/bash

## This script allows an optional -t {tag_label} argument, to allow separation between different sets of servers.
## It also allows an optional count of servers to be specified, -c count  with the default being 1.
## These arguments, if used, must be before any other arguments.
tag=""
numServers=1
while getopts ":t:c:" op
do
  case $op in
   ( "t" ) ((scnt++)) ; tag="$OPTARG" ;;
   ( "c" ) ((scnt++)) ; numServers="$OPTARG" ;;
    ( * ) echo "Unknown argument provided: -$OPTARG, ignoring." ; echo "" ;;
  esac
  ((scnt++))
done

while [ ${scnt:-0} -gt 0 ]
do
  shift
  ((scnt--))
done

## This script expects two arguments, the "number" of the server to kill
## And the signal to kill it with
## If no argument is given all servers will be killed with a 15.

## This script expects the environment to have the following variables set:
## GFE_COUNT -- the number of servers to use, default is one.
## GFE_DIR -- location of the GFE Java build

# Server to stop:
server=$1
shift

# Signal to use:
signal=$1
signal=${signal:-15}

# GFE Build location
gfeProd=${GFE_DIR:-""}

# Base for dir names
gfdb=GFECS${tag:+_}$tag

##set -x

if [ ${server:-0} -gt 0 ]
then
  tdir=${gfdb}_$server
  killJavaServer ${signal} ${tdir}
  exit 0
fi

# Number of servers to stop:
cnt=$numServers

while [ ${cnt:-0} -gt 0 ]
do
  tdir=${gfdb}_$cnt
  killJavaServer ${signal} ${tdir}
  ((cnt--))
done
