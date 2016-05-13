#!/bin/bash

#set -x
echo "start stopClient.sh"
source genericFunctions
source runDriverFunctions
host=${1:-all}
shift
cnt=$*
hostCnt=1

if [ "$host" == "all" ]
then
for clnt in $cnt
do
  for hst in $UHOSTS
  do
     goHost $hst stopClient $hst $clnt
  done
done    
exit 0
fi

export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`


PID_BASE="$BASEDIR/pids/$GF_FQDN"
if [ ! -d $PID_BASE ]
then
  exit
fi
findCmd="/usr/bin/find"
for dnam in `$findCmd $PID_BASE -type d ! -name "." ! -name ".." -print`
do
  for pfil in `$findCmd $dnam -type f -name ${1}_pid`
  do
    id=`cat $pfil`
    echo "about to stop process .. found $dnam $pfil for $hst $cnt"
    stopProcess $id -p
    rm -f $pfil
    break
  done
done  
echo "end stopClient.sh"
