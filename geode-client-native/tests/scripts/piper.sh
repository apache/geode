#!/bin/bash


AWK=`which nawk 2>/dev/null`
lname=`hostname`
hst=`nslookup $lname 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`

source $BASEDIR/envs/${hst}_env
source $BASEDIR/gfcpp.env

goHost() {
  ##  set -x
  local host=$1
  shift
  local cmd="$*"
  ##  echo "In piper:goHost cmd is:: $cmd ::"
  if [ "${cmd:-none}" == "none" ]
  then
    echo "Empty command passed to goHost"
    return
  fi
  local ev="`cat $BASEDIR/envs/extra_env $BASEDIR/envs/${host}_env`"
  ##  echo "piper:goHost: ssh -A -x -q -f $host \"$ev $cmd \" "
  ssh -o "NumberOfPasswordPrompts 0" -A -x -f $host "$ev $cmd &" 
  ##  echo "piper:goHost complete"
  ##  set +x
}

while [ "${cmd:-NULL}" != "exit" ]
do
  if [ "${cmd:-NULL}" != "NULL" ]
  then
    $cmd &
  fi
  date | tr -d '\n' ; echo "   Waiting..."
  read cmd
  res=$?
  if [ $res -ne 0 ]
  then
    cmd=exit
  fi
  date | tr -d '\n' ; echo "   Read ::$cmd::"
done
echo "Pipe process exiting."
