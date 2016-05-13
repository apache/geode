#!/bin/bash
 
export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`

tag=""
numClients=1
while getopts ":t:c:p:m:" op
do
  case $op in
   ( "m" ) ((scnt++)) ; waitMinutes="$OPTARG" ;;
   ( "t" ) ((scnt++)) ; tag="$OPTARG" ;;
   ( "c" ) ((scnt++)) ; numClients="$OPTARG" ;;
   ( "p" ) ((scnt++)) ; seqid="$OPTARG" ;;
    ( * ) echo "Unknown argument provided: -$OPTARG, ignoring." ; echo "" ;;
  esac
  ((scnt++))
done

while [ ${scnt:-0} -gt 0 ]
do
  shift
  ((scnt--))
done

# Where are we now?
currDir=`pwd`
bnam=`basename $currDir`

mkdir -p ../pids/$GF_FQDN/$bnam
echo "${$}" > ../pids/$GF_FQDN/$bnam/waitForTask_pid

# cnt specified which client to setup:
cnt=$numClients

# Base for dir names
gfdb=GFEJC${tag:+_}$tag

tdir=${gfdb}_$cnt
if [ ! -d $tdir ]
then
  echo $tdir does not exist.
  exit 0
fi

grep "seqid $seqid finished" $currDir/$tdir/JC${tag:+_}$tag${cnt:+_}$cnt.log
status=$?
num=0
sleepTime=10  ## Note: changing this impacts the calculation used to set sleepsPerMinute
sleepsPerMinute=6
((limit=$waitMinutes*$sleepsPerMinute))
((logLimit=$limit/3))
while [ $status -ne 0 ]
do 
  sleep $sleepTime
  ((num++))
  if [ $num -gt $limit ]
  then
    echo "ERROR:  Wait for task timed out after $waitMinutes minutes."
    exit 1
  fi
  ((val=$num%$logLimit))
  if [ $val -eq 0 ]
  then
    ((minutes=$num/$sleepsPerMinute))
    echo "Wait for task has waited for $minutes minutes."
  fi
  grep "seqid $seqid finished" $currDir/$tdir/JC${tag:+_}$tag${cnt:+_}$cnt.log
  status=$?
done

exit 0
