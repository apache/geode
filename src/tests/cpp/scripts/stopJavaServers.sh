#!/bin/bash

## This script allows an optional -t {tag_label} argument, to allow separation between different sets of servers.
## It also allows an optional count of servers to be specified, -c count  with the default being 1.
## These arguments, if used, must be before any other arguments.
## For example, if  "-t CS1" is provided, the script will store endpoints using 
## the name EndPoints_CS1, so tasks that need access to endpoints need the data value
## defined. <data name="TAG">CS1</data>
## The default tag is blank.
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

## This script expects one argument, the "number" of the server to stop
## If no argument is given all servers will be stopped.

## This script expects the environment to have the following variables set:
## GFE_COUNT -- the number of servers to use, default is one.
## GFE_DIR -- location of the GFE Java build

# Where are we now?
currDir=`pwd`

# Server to stop:
server=$1

# GFE Build location
gfeProd=${GFE_DIR:-""}

# Base for dir names
gfdb=GFECS${tag:+_}$tag

if [ ${server:-0} -gt 0 ]
then
  tdir=${gfdb}_$server
  if [ -d $tdir ]
  then
    cd $tdir
    stopCS
    cd $currDir
  fi
  exit
fi

# Number of servers to stop:
cnt=$numServers

while [ ${cnt:-0} -gt 0 ]
do
  tdir=${gfdb}_$cnt
  if [ -d $tdir ]
  then
    cd $tdir
    stopCS
    cd $currDir
  fi
  ((cnt--))
done
