#!/bin/bash

source genericFunctions

export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`
export GF_IPADDR=`nslookup $local 2>/dev/null | ${AWK:-awk} '{if ($1 ~ /Address:/) ip=$2}END{print ip}'`

suff=`date +'%m.%d.%H.%M.%S'`
cnum=$1
shift
xmlfile=$1
shift
logDir=$1
shift
port=$1
shift

program=${1:-none}
if [ $program != 'none' ]
then
  shift
  arguments="$@"
fi

((bbport=$port+4))
export GF_BBADDR=${driverHost}:$bbport
echo "export GF_BBADDR=${driverHost}:$bbport" >> $logDir/gfcpp.env

vcmd=
valLoc=$VALGRIND_INSTALL/$HOSTTYPE
argCount $toolClients
if [ $RETVAL -gt 0 ]
then
  contains $cnum $toolClients
else
  RETVAL=1  ## If no clients are specified, all clients are assumed
fi

if [ -d $valLoc -a $RETVAL == 1 ]  ## Use valgrind on Client
then
  PATH=$valLoc/bin:$PATH
  LD_LIBRARY_PATH=$valLoc/lib:$LD_LIBRARY_PATH
  vcmd="$VALCMD"
fi

cd $logDir
LOG=Client_${cnum}_$suff.log
ulimit -a &> $LOG
echo "  " >> $LOG 2>&1
uname -a >> $LOG

if [ $program != 'none' ]
then
  $program ${arguments:- } >> $LOG 2>&1 &
else
  $vcmd Client $cnum $xmlfile ${driverHost}:$port ${driverHost}:$bbport $ne >> $LOG 2>&1 &
fi
myPid=$!

thisDir=`basename $PWD`
mkdir -p ../pids/$GF_FQDN/$thisDir
echo $myPid > ../pids/$GF_FQDN/$thisDir/${cnum}_pid


