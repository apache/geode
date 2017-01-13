#!/bin/bash
## Let's take care of some basic needs
#set -x
fullscript=$0
fullcmdline="$0 $*"
cmdline=$*

script=`basename $0`
scriptLocation=`dirname $0`

source $scriptLocation/genericFunctions
source $scriptLocation/runDriverFunctions
export AWK=`which nawk 2>/dev/null` ## Which awk to use
export HOSTS=     ## Hosts given on commandline
export UHOSTS=    ## unique hosts ( $HOSTS piped thru sort -u )
export PHOSTS=    ## Hosts that will need provisioned/unprovisioned
# Get our host name, ip address, and os
export local=`hostname`
export GF_FQDN=`nslookup $local 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`
export GF_IPADDR=`nslookup $local 2>/dev/null | ${AWK:-awk} '{if ($1 ~ /Address:/) ip=$2}END{print ip}'`
export myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`
       


# Let's bail if we have a command return an error while we are getting set up
trap badcmd SIGINT ERR

# Process the command line
if [ $# -lt 2 ]
then
  usage "Too few arguments were specified."
fi

TSTIME=$SECONDS

clearVars
processArgs $*
setLocations

LOG "$fullcmdline"

handleRestart

processGroups 

export SA_RUNNING=0
if [ ${SSH_AGENT_PID:--1} -ne -1 ]
then 
  SA_RUNNING=`ps -fp $SSH_AGENT_PID 2>/dev/null | grep ssh-agent | wc -l`
fi
if [ $SA_RUNNING -eq 0 ]
then
  ## start ssh-agent
  eval `ssh-agent`## > /dev/null
  ssh-add > /dev/null 2>&1
fi

TRACE_ON

verifyTestFiles

# source env settings set defined during build, if any
if [ -f $fwkdir/scripts/run.env ]
then 
  source $fwkdir/scripts/run.env
fi

miscDriverSetup
export username=$USERNAME
export passwrd=$PASSWORD
export vmhosts=$VMHOSTS
export vmnames=$VMNAMES
export isvmotion=$VMOTION  #default is false
export vminterval=$VMINTERVAL # mention the time interval in min to trigger the vmotion . eg: VMINTERVAL=5, which means vmotion will be trigeer every 5 minutes.
if [ "$isvmotion" == "true" ]
then
  LOG " vmotion parameters are username = $username , vmhosts=$vmhosts , vmnames=$vmnames , vminterval=$vminterval"
fi
setJavaHome
runLatestProp

setupHosts

LOG "Will start clients using: " $UHOSTS
showSettings

logTests

####################################################################################################

TestStart=$SECONDS
if [ ${timeLimit:-0} -gt 0 ]
then 
  ((HardStop=$TestStart+$timeLimit+300))
else
  HardStop=0
fi

export summaryDirs=""

testFileCnt=0
for tfile in $testList
do
  # Let's ignore simple errors while we are in the loop
  trap '' ERR
  ((testFileCnt++))
  if [ $testFileTotal -gt 1 ]
  then
    LOG "### Test file $tfile -- $testFileCnt of $testFileTotal"
  else
    LOG "### $tfile"
  fi
  if [ ! -f $fwkdir/xml/$tfile ]
  then
    WARN "Skipping $tfile, file not found."
    continue
  fi
  
  ## runIters loop
  rcnt=0
  while [ $rcnt -lt $runIters ]
  do
    ((rcnt++))
    LOG "#########################################################"
    LOG "### $tfile  -- Run $rcnt of $runIters"

    Now=$SECONDS
    STIME=$Now
    TLIMIT=0
    if [ $HardStop -gt 0 ]
    then
      if [ $HardStop -lt $Now ]
      then
        LOG "Skipping, time allowed for test run is used up."
        continue
      fi
      ((TLIMIT=$HardStop-$Now))
    fi
    ## This will do many things, including creating the test directory, and cd'ing to it
    setupTest
    
    provisionDir $PWD
    
    LOG "Using properties: "
    LOGCONTENT gfcpp.properties
    
    # Chose a port for the driver to use in listening for clients
    random 3031 31321 port
    LOG "Using driver port $port"
    
    driverError=0 
    LOG $vcmd Driver $TLIMIT $logDir $stestFile $port
    # The || is needed to prevent ERR from being trapped if driver exits 
    # abnormally (see man trap)
    $vcmd Driver $TLIMIT $logDir $stestFile $port 2>&1 | tee Driver.log || driverError=$?
    LOG "driverError value is $driverError"
    if [ $driverError -ne 0 ]; then
      ERROR "Error: Driver has returned $driverError, continuing with remaining tests"
    fi
    stopProcess Driver $PWD
    summaryDirs="$summaryDirs $PWD"

    stopAll all $PWD
    
    timeBreakDown $STIME
    ftag=`echo $tfile | tr "\/\\\\\\\\" "____"`
    touch test.$ftag
    LOG "### $tfile  -- End of run $rcnt of $runIters"
    LOG "#########################################################"
    
    doSubSummary
    doGenCsvReport  
    cd $BASEDIR 

  done  ## end of runIters loop
done  ## end of testList loop


####################################################################################################

cleanup
