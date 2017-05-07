#!/bin/bash
 
## This script allows an optional -t {tag_label} argument, to allow separation between different sets of clients.
## It also allows an optional count of clients to be specified, -c count  with the default being 1.
## These arguments, if used, must be before any other arguments.
## For example, if  "-t JC1" is provided, the script expects to find endpoints 
## using the name EndPoints_JC1, so tasks that need access to endpoints need the data value
## defined. <data name="TAG">JC1</data>
## The default tag is blank.
tag=""
numClients=1
while getopts ":t:c:" op
do
  case $op in
   ( "t" ) ((scnt++)) ; tag="$OPTARG" ;;
   ( "c" ) ((scnt++)) ; numClients="$OPTARG" ;;
    ( * ) echo "Unknown argument provided: -$OPTARG, ignoring." ; echo "" ;;
  esac
  ((scnt++))
done

while [ ${scnt:-0} -gt 0 ]
do
  shift
  ((scnt--))
done

## This script expects one argument, the "number" of the client to start
## If no argument is given all clients will be started.

## This script expects the environment to have the following variables set:
## GFE_COUNT -- the number of clients to use, default is one.
## GFE_DIR -- location of the GFE Java build

# Where are we now?
currDir=`pwd`

# Server to start:
server=$1

# GFE Build location
gfeProd=${GFE_DIR:-""}

# Base for dir names
gfdb=GFEJC${tag:+_}$tag

export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`

if [ ${server:-0} -gt 0 ]
then
  tdir=${gfdb}_$server
  if [ -d $tdir ]
  then
    cd $tdir
    ./startJC
    sleep 10
    pid=`grep "Process ID:" system.log | ${AWK:-awk} '{print $3}'`
    cd $currDir
    thisDir=`basename $currDir`
    mkdir -p ../pids/$GF_FQDN/$thisDir
    echo "$pid" > ../pids/$GF_FQDN/$thisDir/JavaClient_pid
  fi
  exit
fi

# cnt specified which client to setup:
cnt=$numClients

  tdir=${gfdb}_$cnt
  if [ -d $tdir ]
  then
    cd $tdir
    ./startJC
    cd $currDir
  fi
