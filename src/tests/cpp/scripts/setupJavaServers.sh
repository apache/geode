#!/bin/bash

#set -x 

## This script allows an optional -t {tag_label} argument, to allow separation between different sets of servers.
## It also allows an optional count of servers to be specified, -c count  with the default being 1.
## These arguments, if used, must be before any other arguments.
## For example, if  "-t CS1" is provided, the script will store endpoints using 
## the name EndPoints_CS1, so tasks that need access to endpoints need the data value
## defined. <data name="TAG">CS1</data>
## The default tag is blank.
scriptname=`basename $0`
tag=""
numServers=1
while getopts ":t:c:p:e:N:" op
do
  case $op in
   ( "e" ) ((scnt++)) ; ev="export $OPTARG" ;;
   ( "t" ) ((scnt++)) ; tag="$OPTARG" ;;
   ( "c" ) ((scnt++)) ; numServers="$OPTARG" ;;
   ( "p" ) ((scnt++)) ; passthru="$passthru $OPTARG" ;;
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

## This script requires as an argument, the xml file to use for the cache.xml for servers
## The script will replace "$PORT_NUM" with the proper port number to use.
## The script will replace "$LRU_MEM" with the proper amount of memory to use for heap lru.

GFE_XML=$1

## This script may expect the environment to have the following variables set:

## GFE_DIR -- location of the GFE Java build, REQUIRED
## GFE_DIR_CYG -- allows setting a cygwin specific value
## GFE_DIR_LIN -- allows setting a linux specific value
## GFE_DIR_SOL -- allows setting a solaris specific value

## GFE_HEAPSIZE -- max size in meg the cacheserver should be allowed, for example 2048
## GFE_HEAPSIZE_CYG -- allows setting a cygwin specific value
## GFE_HEAPSIZE_LIN -- allows setting a linux specific value
## GFE_HEAPSIZE_SOL -- allows setting a solaris specific value

## GFE_LRU_MEM -- the heap lru size limit in meg, for example 1024
## GFE_LRU_MEM_CYG -- allows setting a cygwin specific value
## GFE_LRU_MEM_LIN -- allows setting a linux specific value
## GFE_LRU_MEM_SOL -- allows setting a solaris specific value

random() {
  if [ ${randval:--1} -eq -1 ] # First time we have called this function
  then  # So we seed the sequence 
    # the pipe to bc is required because date will sometimes return a number 
    # with a leading 0, but is not a valid octal number
    ((RANDOM=( ( `date +'%S' | bc` + 1 ) * $$ ) + `date +'%j' | bc` ))
  fi
  min=$1
  max=$2
  val=$3
  ## $RANDOM will be between 0 and 32767 inclusive
  ##randval=`echo "( ( $RANDOM * ( $max - $min ) ) / 32767 ) + $min" | bc`
  ((randval=( ( $RANDOM * ( $max - $min + 1 ) ) / ( 32767 + 1 ) ) + $min ))
  export $val=$randval
}

# Where are we now?
currDir=`pwd`

### Has the mcast properties been set yet?
### use BB instead of file for retaining and sharing mcast properties
MCLABEL=`basename $currDir`
DEF0=`FwkBB get $MCLABEL MCADDR`
if [ ${DEF0:-none} == "none" ]
then
  random 1 254 madd
  random 2431 31123 mport
  DEF0="mcast-address=224.10.11.$madd"
  DEF2="mcast-port=$mport"
  FwkBB set $MCLABEL MCADDR "$DEF0"
  FwkBB set $MCLABEL MCPORT "$DEF2"
else
  DEF2=`FwkBB get $MCLABEL MCPORT`
fi

# Number of servers to setup:
cnt=$numServers

# Base to use for port numbers
random 21321 29789 basePort

# cache.xml to use
cacheXml=$BUILDDIR/framework/xml/${GFE_XML:-NO_CACHE_XML_SPECIFIED}
if type -p cygpath >/dev/null 2>/dev/null; then
  cacheXml="`cygpath -p -u "$cacheXml"`"
fi

# Base for dir names
gfdb=GFECS${tag:+_}$tag

AWK=`which nawk 2>/dev/null`
myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`
local=`hostname`
GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`

MCLABEL=`basename $currDir`
export GFEBB=GFE_BB
export LOCCNT=LOC_CNT
FwkBB set $MCLABEL PROV_HOST "$GF_FQDN"
FwkBB set $MCLABEL PROV_DIR "$currDir"
if [ "${POOLOPT:-none}" != "none" ]
then
 if [ "$POOLOPT" == "poolwithendpoints" ]
 then
   FwkBB set GFE_BB testScheme "poolwithendpoints"
 elif [ "$POOLOPT" == "poolwithlocator" ]
 then
   FwkBB set GFE_BB testScheme "poolwithlocator"
 fi
 if [ "$TESTSTICKY" == "ON" ]
 then
   FwkBB set GFE_BB teststicky "ON"
 fi
fi
                                  

if [ "${passthru:-NONE}" != "NONE" ]
then
  DEF3="$passthru"
else
  DEF3=" "
fi
  
GEMFIRE=""
# setup GF_JAVA
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
  var_name=GF_JAVA_${myOS}_${myBits}
  if [ $myBits -eq 64 ]
  then
    export GF_JAVA=${!var_name:-${noJava:-""}}
  else
    export GF_JAVA=${!var_name:-""}
  fi

 if [ $myOS == "CYG" ]
 then
   GEMFIRE="gemfire.bat"   
 else
   GEMFIRE="gemfire"
 fi
 
# GFE Build location
export gfeProd=${GFE_DIR:-""}

export gfe_name=GFE_DIR_$myOS
# and if os specific values have been passed in, they win
if [ ${!gfe_name:-none} != "none" ]
then
  gfeProd=${!gfe_name}
fi
SECURITY_JAR=${gfeProd}/lib/gfSecurityImpl.jar
DEF1=`FwkBB get $MCLABEL LOCPORT`
if [ ${DEF1:-none} == "none" ]
then
  random 31124 54343 locPort
  bbcntr=`FwkBB inc $GFEBB $LOCCNT`
  FwkBB set $MCLABEL LOCPORT "locators=${GF_FQDN}:${locPort}"
  FwkBB set $GFEBB LOCPORTS_$bbcntr "${GF_FQDN}:${locPort}"
  # Start the gemfire locator
  cd "${gfeProd}"
  source bin/setenv.sh
  cd "${currDir}"
  echo "Starting locator in directory ${currDir}: ${gfeProd}/bin/gemfire start-locator -port=${locPort} -dir=. $cmdargs"
  "${gfeProd}/bin/${GEMFIRE}" start-locator -port=${locPort} -dir=. $cmdargs
  # Record the PID for stopAll
  locatorPID="`grep "Process ID:" locator.log | ${AWK:-awk} '{print $3}'`"
  locatorPIDDir="${currDir}/../pids/${GF_FQDN}/`basename "${currDir}"`"
  singleLocPid="`echo $locatorPID | awk '{print $1;}'`"
  mkdir -p "${locatorPIDDir}"
  echo "${singleLocPid}" > "${locatorPIDDir}/999999_pid"
fi
while [ ${cnt:-0} -gt 0 ]
do
  tdir=${gfdb}_$cnt
  if [ -d $tdir ]
  then
    echo $tdir directory exist, Skipping as work seems to have been done for this directory.
    nextcnt=`expr $cnt + 1`
    tdir=${gfdb}_$nextcnt
    #break
  fi

  # Build temp dirs for them to call home
  mkdir -p $tdir/system
  if [ -f $currDir/gfcpp.gfe.properties ]
  then
    cat $currDir/gfcpp.gfe.properties >> $currDir/$tdir/gemfire.properties
  fi
  
  # Create cache.xml files for them to call their own
  ((pnum=$basePort+$cnt))
  echo "export cacheXml=\"$cacheXml\"" >> $currDir/$tdir/ssk.env
  echo "export GFE_PORT=\"$pnum\"" >> $currDir/$tdir/ssk.env
  if [ "${ev:-none}" != "none" ]
  then
    if [ -z "${CYGWIN}" ]; then
      echo "${ev:-}:${SECURITY_JAR}" >> $currDir/$tdir/ssk.env
    else
      echo "export CLASSPATH=\"$BUILDDIR/framework/lib/javaobject.jar;$CLASSPATH;${SECURITY_JAR}\"" >> $currDir/$tdir/ssk.env
    fi
  else  
    if [ -z "${CYGWIN}" ]; then
      echo "export CLASSPATH=${CLASSPATH}:${SECURITY_JAR}" >> $currDir/$tdir/ssk.env
    else
      echo "export CLASSPATH=${CLASSPATH};${SECURITY_JAR}" >> $currDir/$tdir/ssk.env
    fi 
  fi  
  echo "export DEF3=\"$DEF3\"" >> $currDir/$tdir/ssk.env
  
  ((cnt--))
done
