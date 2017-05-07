#!/bin/bash

export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`

cnum=$1
id=0
if [ ${2:-x} == -p ]
then
  id=$1
else
  dirn="${2:-none}"
  if [ "$dirn" == "none" ]
  then
    exit
  fi
  PID_BASE=$BASEDIR/pids/$GF_FQDN/$dirn
  if [ -f $PID_BASE/${cnum}_pid ]
  then
    id=`cat $PID_BASE/${cnum}_pid`
  else
    if [ -f $PID_BASE/pid_$cnum ]
    then
      id=`cat $PID_BASE/pid_$cnum`
    fi
  fi
fi

if [ ${id:-0} -eq 0 ]
then
  exit
fi

myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`
killCmd="kill"
if [ $myOS == "CYG" ]
then
  killCmd="/bin/kill -f"
fi

##ls /proc/${id}/status > /dev/null 2>&1
##if [ $? -ne 0 ] ; then exit ; fi
#if [ -d /proc/$id ] ; then $killCmd $id >/dev/null 2>&1 ; sleep 1 ; fi
#if [ -d /proc/$id ] ; then $killCmd -9 $id >/dev/null 2>&1 ; fi
if $killCmd -0 $id >/dev/null 2>&1; then
  $killCmd $id >/dev/null 2>&1 && sleep 1 && $killCmd -9 $id >/dev/null 2>&1
fi
