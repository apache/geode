#!/bin/bash

#The objective of this script is to create following files:
#startJC --- the script to run mixed mode java client
#killJC --- housekeeping script in case the java client is still running
#gemfire.properties

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

## This script requires as an argument, the xml file to use for the cache.xml for clients
## The script will replace 999999999 with the proper port number to use.

GFE_XML=$1

## This script expects the environment to have the following variables set:
## GFE_DIR -- location of the GFE Java build

# Where are we now?
currDir=`pwd`

# cnt specified which client to setup:
cnt=$numClients

EPBB=GFE_BB
EPLABEL=EndPoints${tag:+_}$tag

myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`
# GFE Build location
gfeProd=${GFE_DIR:-""}

export gfe_name=GFE_DIR_$myOS
export heap_name=GFE_HEAPSIZE_$myOS
export lru_name=GFE_LRU_MEM_$myOS

# now some os specific overrides of defaults
if [ $myOS == "CYG" ]
then
  maxHeap=1280
  heap_lru=1024
else
  maxHeap=2048
  heap_lru=1536
fi

# and if os specific values have been passed in, they win
if [ ${!gfe_name:-none} != "none" ]
then
  gfeProd=${!gfe_name}
fi
if [ ${!heap_name:-none} != "none" ]
then
  maxHeap=${!heap_name}
fi
if [ ${!lru_name:-none} != "none" ]
then
  heap_lru=${!lru_name}
fi

# cache.xml to use
cacheXml=$BUILDDIR/framework/xml/${GFE_XML:-NO_CACHE_XML_SPECIFIED}

# Base for dir names
gfdb=GFEJC${tag:+_}$tag

currEPs=`FwkBB get $EPBB $EPLABEL`
if [ $? -eq 0 ]; then
  endPoints="`echo $currEPs | awk '{print \$1}'`"
else
  echo "No endpoints are known."
    exit $?
fi

  tdir=${gfdb}_$cnt
  if [ -d $tdir ]
  then
    echo Skipping as work seems to have been done.
    break
  fi

  # Build temp dirs for them to call home
  mkdir $tdir

export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`
thisDir=`basename $PWD`
mkdir -p ../pids/$GF_FQDN/$thisDir
  
# Create gemfire.properties
##  echo "mcast-address=224.0.0.250" > $tdir/gemfire.properties
##  echo "mcast-port=5757" >> $tdir/gemfire.properties
  echo "locators=" > $tdir/gemfire.properties
  echo "license-file=gemfireLicense.zip" >> $tdir/gemfire.properties
  echo "license-type=evaluation" >> $tdir/gemfire.properties
  echo "statistic-sampling-enabled=true" >> $tdir/gemfire.properties
  echo "statistic-archive-file=statArchive.gfs" >> $tdir/gemfire.properties
  echo "log-level=config" >> $tdir/gemfire.properties
  echo "log-file=system.log" >> $tdir/gemfire.properties
  
  # Create bridge_client.xml files for them to call their own
  sed 's/\$ENDPOINT/'$endPoints'/;s/\$LRU_MEM/'$heap_lru'/' $cacheXml > $tdir/bridge_client.xml

  # create a JC${tag:+_}$tag${cnt:+_}$cnt.cmd
  echo "status" > $currDir/JC${tag:+_}$tag${cnt:+_}$cnt.cmd

  echo "TD=\"$currDir/$tdir\"" > $currDir/$tdir/ssk.env
  echo "PD=\"$gfeProd\"" >> $currDir/$tdir/ssk.env
  echo "JA=\"-Xmx${maxHeap}m -Xms256m\"" >> $currDir/$tdir/ssk.env
  
  # create script to start it
  echo "#!/bin/bash" > $tdir/startJC
  echo "##set -x" >> $tdir/startJC
  echo "source ssk.env" >> $tdir/startJC
  echo "cd \$PD" >> $tdir/startJC
  echo "source bin/setenv.sh" >> $tdir/startJC
  echo "cd \$TD" >> $tdir/startJC
  echo "export PATH=$gfeProd/jre/bin:$PATH " >> $tdir/startJC
  echo "export CLASSPATH=$BUILDDIR/framework/lib/mixed.jar:$gfeProd/lib/gemfire.jar:$gfeProd/lib/antlr.jar:\$CLASSPATH " >> $tdir/startJC
if [ $myOS == "CYG" ]
then
  echo "CLASSPATH=\`cygpath -p -w \$CLASSPATH\`" >> $tdir/startJC
fi
  echo "java \$JA javaclient.CacheRunner bridge_client.xml -batch file=../JC${tag:+_}$tag${cnt:+_}$cnt.cmd > JC${tag:+_}$tag${cnt:+_}$cnt.log &" >> $tdir/startJC
  echo "echo \$! > ../../pids/$GF_FQDN/$thisDir/JC${tag:+_}$tag${cnt:+_}$cnt_pid " >> $tdir/startJC
  echo "exit 0" >> $tdir/startJC

  chmod guo+rwx $tdir/startJC

