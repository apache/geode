#!/bin/bash

source ssk.env
cd $PD
source bin/setenv.sh
cd $TD
scriptname=`basename $0`
myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`
if [ ! -f .cacheserver.ser ]
then
  exit
fi

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
  $PD/bin/cacheserver.bat stop -dir=.
else
  $PD/bin/cacheserver stop -dir=.
fi
stat=$?
sleep 10

if [ $stat -ne 0 ]
then
  AWK=`which nawk 2>/dev/null`

  killCmd="kill"
  if [ $myOS == "CYG" ]
  then
    killCmd="/bin/kill -f"
  fi

  cspid=`grep 'Process ID:' cacheserver.log | ${AWK:-awk} '{print $NF}'`

  waitCnt=1
  kill -0 $cspid 2>/dev/null
  stillRunning=$?
  while [ $stillRunning -eq 0 ] 
  do
    if [ $waitCnt -gt 4 ]
    then
      $killCmd -9 $cspid >/dev/null 2>&1
      break
    else
      $killCmd $cspid >/dev/null 2>&1
    fi
    ((waitCnt++))
    sleep 5
    kill -0 $cspid 2>/dev/null
    stillRunning=$?
  done
  rm -f .cacheserver.ser
fi
