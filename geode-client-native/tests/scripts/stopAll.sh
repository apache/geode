#!/bin/bash

source genericFunctions
source runDriverFunctions

##set -x
host=${1:-all}
shift
dirn=${1:-""}

if [ "$host" == "all" ]
then
  for hst in $UHOSTS
  do
    goHost $hst stopAll local $dirn
  done
  exit 0
fi

export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`


if [ ${dirn:-none} == "none" ]
then
  PID_BASE="$BASEDIR/pids/$GF_FQDN"
else
  PID_BASE="$BASEDIR/pids/$GF_FQDN/`basename "$dirn"`"
fi


if [ ! -d $PID_BASE ]
then
  exit
fi

## for each dir under $PID_BASE, stop all procs whose pids are recorded
findCmd="/usr/bin/find"
for dnam in `$findCmd $PID_BASE -type d ! -name "." ! -name ".." -print`
do
  ## Stop Driver
  pfil=$dnam/pid_Driver
  if [ -f $pfil ]
  then
    id=`cat $pfil`
    stopProcess $id -p
    rm -f $pfil
  fi

  ## Stop Clients
  for pfil in `$findCmd $dnam -type f -name "*_pid"`
  do
    id=`cat $pfil`
    stopProcess $id -p
    rm -f $pfil
  done

  random 1 5 seconds

  sleep $seconds

  if [ -d $dnam ]
  then
    ( rmdir $dnam > /dev/null 2>&1 || sleep 60 && rmdir $dnam > /dev/null 2>&1 ) &
  fi
done 

cdir=$PWD
for gdir in $BASEDIR/*/GFECS*
do
  if [ -d $gdir ]
  then
    cd $gdir
    eval `grep CS_HOST ssk.env 2>/dev/null`
    if [ $GF_FQDN == ${CS_HOST:-none} ]
    then
      stopCS > /dev/null 2>&1
      mv ssk.env ssk.env.hide 2>/dev/null
    fi
    cd $cdir
  fi
done
