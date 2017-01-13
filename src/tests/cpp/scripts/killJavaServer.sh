#!/bin/bash

checkRunning() {
  if [ "${myOS}" = "CYG" ]; then
    $killCmd -0 $1 2>&1 | grep -q 'kill:'
  else
    $killCmd -0 $1 2>/dev/null
  fi
  echo $?
}

killIt() {
  sig=$1
  kpid=$2

  waitCnt=1
  stillRunning="`checkRunning $kpid`"
  while [ $stillRunning -eq 0 ] 
  do
    if [ $waitCnt -gt 4 ]
    then
      $killCmd -9 $kpid >/dev/null 2>&1
      break
    else
      $killCmd $sig $kpid >/dev/null 2>&1
    fi
    ((waitCnt++))
    sleep 5
    stillRunning="`checkRunning $kpid`"
  done
}

# Signal to use:
signal=$1
signal=${signal:-15}

# Server start directory
tdir="$2"

AWK=`which nawk 2>/dev/null`
myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`

killCmd="kill"
if [ "${myOS}" = "CYG" ]
then
  killCmd="/bin/kill -f"
fi

if [ -d "${tdir}" ]; then
  killpid="`grep "Process ID:" "${tdir}/cacheserver.log" | ${AWK:-awk} '{print $3}'`"
  killIt -${signal} ${killpid}
  rm -f "${tdir}/.cacheserver.ser"
fi
