#!/bin/bash

#set -x

scriptname=`basename $0`

startServer() {
  cnt=$1
  extraargs=$2
  tdir=${gfdb}_$cnt
  if [ ! -d $tdir ]
  then
    mkdir -p $tdir
    rsync -rLqe "ssh -A -x -q -o StrictHostKeyChecking=no " ${PROV_HOST}:${PROV_DIR}/$tdir/ $tdir
  fi
  if [ -d $tdir ]
  then
    cd $tdir
    export TD=$PWD
    echo "export TD=$TD" >> ssk.env
    echo "export PD=$PD" >> ssk.env
    if [ ${CLASSPATH:-none} != "none" ]
    then
      echo "export CLASSPATH=$CLASSPATH:\$CLASSPATH" >> ssk.env
    fi
    echo "export CS_HOST=\"$GF_FQDN\"" >> ssk.env
    source ssk.env
    rsync -rLqe "ssh -A -x -q -o StrictHostKeyChecking=no " ${PROV_HOST}:${cacheXml} before.xml
    sed 's/\$PORT_NUM/'$GFE_PORT'/;s/\$LRU_MEM/'$heap_lru'/' before.xml > cache.xml
    bbcntr=`FwkBB get $EPBB $tdir`
    if [ ${bbcntr:-NONE} == "NONE" ]
    then
      bbcntr=`FwkBB inc $EPBB $EPCNT`
      FwkBB set $EPBB $tdir $bbcntr
    fi
    FwkBB set $EPBB ${EPLABEL}_$bbcntr "${GF_FQDN}:$GFE_PORT"
    cd $PD
    source bin/setenv.sh
    cd $TD
      echo $PD/bin/${CACHESERVER} start cache-xml-file=cache.xml statistic-sampling-enabled=true statistic-archive-file=statArchive.gfs -J-DCacheClientProxy.MESSAGE_QUEUE_SIZE=500000 -dir=. $DEF4 mcast-port=0 $extraargs $DEF3 $JA
      $PD/bin/${CACHESERVER} start cache-xml-file=cache.xml statistic-sampling-enabled=true statistic-archive-file=statArchive.gfs -J-DCacheClientProxy.MESSAGE_QUEUE_SIZE=500000 -dir=. $DEF4 mcast-port=0 $extraargs $DEF3 $JA
    cd $currDir
  else
    echo "ERROR: directory $tdir not found."
  fi
}

## This script allows an optional -t {tag_label} argument, to allow separation between different sets of servers.
## It also allows an optional count of servers to be specified, -c count  with the default being 1.
## These arguments, if used, must be before any other arguments.
## For example, if  "-t CS1" is provided, the script will store endpoints using
## the name EndPoints_CS1, so tasks that need access to endpoints need the data value
## defined. <data name="TAG">CS1</data>
## The default tag is blank.
tag=""
numServers=1
minMemory=0
maxMemory=0
cmdargs=""
while getopts ":t:c:M:X:N:" op
do
  case $op in
   ( "t" ) ((scnt++)) ; tag="$OPTARG" ;;
   ( "c" ) ((scnt++)) ; numServers="$OPTARG" ;;
   ( "M" ) ((scnt++)) ; minMemory="$OPTARG" ;;
   ( "X" ) ((scnt++)) ; maxMemory="$OPTARG" ;;
   ( "N" ) ((scnt++)) ; cmdargs="$cmdargs $OPTARG" ;;
    ( * ) echo "Unknown argument provided: -$OPTARG, ignoring." ; echo "" ;;
  esac
  ((scnt++))
done

while [ ${scnt:-0} -gt 0 ]
do
  shift
  ((scnt--))
done
## This script expects one argument, the "number" of the server to start
## If no argument is given all servers will be started.

## This script expects the environment to have the following variables set:
## GFE_COUNT -- the number of servers to use, default is one.
## GFE_DIR -- location of the GFE Java build

AWK=`which nawk 2>/dev/null`
myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`
local=`hostname`
CACHESERVER=""
GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`

case $myOS in
  ( "SUN" ) myBits=64 ;;
  ( * )
    if [ $HOSTTYPE != `basename $HOSTTYPE 64` ]
    then
      myBits=64
    else
      myBits=32
    fi
  ;;
esac

sscript=`basename $scriptname 64`
if [ $scriptname != $sscript ]
then
  myBits=64
  noJava="NO_64_BIT_JAVA_INSTALL_SPECIFIED"
else
  noJava=""
fi

# Where are we now?
export currDir=`pwd`

export MCLABEL=`basename $currDir`
export PROV_HOST=`FwkBB get $MCLABEL PROV_HOST`
export PROV_DIR=`FwkBB get $MCLABEL PROV_DIR`
export DEF0=`FwkBB get $MCLABEL MCADDR`
export DEF2=`FwkBB get $MCLABEL MCPORT`
export DEF4=`FwkBB get $MCLABEL LOCPORT`

# setup GF_JAVA
var_name=GF_JAVA_${myOS}_${myBits}
if [ $myBits -eq 64 ]
then
  export GF_JAVA=${!var_name:-${noJava:-""}}
else
  export GF_JAVA=${!var_name:-""}
fi

# Server to start:
server=$1

# Base for dir names
gfdb=GFECS${tag:+_}$tag

# first some defaults values
export maxHeap=${GFE_HEAPSIZE:-""} # Default if it exists
export minHeap=${GFE_MINHEAPSIZE:-""} # Default if it exists
export heap_lru=${GFE_LRU_MEM:-""} # Default if it exists

# GFE Build location
export gfeProd=${GFE_DIR:-""}

# JavaObject and Antlr jar class Path
export CLASSPATH=${GFE_CLASS_PATH:-""}

export gfe_name=GFE_DIR_$myOS
export heap_name=GFE_HEAPSIZE_$myOS
export minheap_name=GFE_MINHEAPSIZE_$myOS
export lru_name=GFE_LRU_MEM_$myOS
export class_path=GFE_CLASS_PATH_$myOS

# now some os specific overrides of defaults
if [ $myOS == "CYG" ]
then
  maxHeap=1280
  heap_lru=1024
  dumpHeapOOM=" "
  CACHESERVER="cacheserver.bat"
else
  maxHeap=2048
  heap_lru=1536
  dumpHeapOOM="-J-XX:+HeapDumpOnOutOfMemoryError"
  CACHESERVER="cacheserver"
fi
minHeap=512
# and if os specific values have been passed in, they win
if [ ${!gfe_name:-none} != "none" ]
then
  gfeProd=${!gfe_name}
fi
if [ ${!heap_name:-none} != "none" ]
then
  maxHeap=${!heap_name}
fi
if [ ${!minheap_name:-none} != "none" ]
then
  minHeap=${!minheap_name}
fi
if [ ${!lru_name:-none} != "none" ]
then
  heap_lru=${!lru_name}
fi
if [ ${!class_path:-none} != "none" ]
then
  CLASSPATH=${!class_path}:${JAVAOBJECT}
fi
if [ ${minMemory:-0} -gt 0 ]
then
   minHeap=${minMemory}
fi
if [ ${maxMemory:-0} -gt 0 ]
then
   maxHeap=${maxMemory}
fi

export JA="-J-Xmx${maxHeap}m -J-Xms${minHeap}m $dumpHeapOOM "
#export JA="-J-Xmx600m -J-Xms600m $dumpHeapOOM "

export EPBB=GFE_BB
export EPCNT=EP_COUNT
export EPLABEL=EndPoints${tag:+_}$tag

export PD="$gfeProd"

if [ ${server:-0} -gt 0 ]
then
  startServer $server "$cmdargs"
else
  # Number of servers to start:
  cnt=$numServers
  while [ ${cnt:-0} -gt 0 ]
  do
    startServer $cnt "$cmdargs"
    ((cnt--))
  done
fi
