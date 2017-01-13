#!/bin/bash

#set -x
getPath()
{
  if uname | grep '^CYGWIN' >/dev/null 2>/dev/null; then
    cygpath "$1"
  else
    echo "$1"
  fi
}

showUsage()
{
  echo -e "Usage: ${scriptName} [OPTION] [<host1> ...]";
  echo -e " <host1> ... are (optional) hostnames of the machines on which to distribute the clients";
  echo -e " Each host can optionally have the hostGroup name prefixed to it with ':'";
  echo -e "   e.g. CS:host1; the hostGroup should not be '*' which implicitly stands for all hosts";
  echo -e "Options are:";
  echo -e "  --xml=FILE \t\t An XML file having the test to be executed.";
  echo -e "\t\t\t This option can be specified multiple times";
  echo -e "  --list=FILE \t\t A file containing a list XML files having the tests to be executed";
  echo -e "  --pool=POOLOPTION \t\t where POOLOPTIONs are poolwithendpoints/poolwithlocator";
  echo -e "  --sleep=SECS \t\t The seconds for which to sleep before executing the regression test";
  echo -e "  --at=TIME \t\t The time at which to execute the regression test";
  echo -e "  --database \t\t This to be used for upload the data in database for regression history. Need to be mentioned at every regression run.";
  echo -e "  --auto-ssh \t\t Use ssh configured for passwordless login instead of plink as remote shell"
  echo -e "\t\t\t Note that this shall only work if sshd is running as a domain user having"
  echo -e "\t\t\t permissions to access network shares and other resources on given hosts."
  echo -e "  --skip-report \t Skip error.report generation"
  echo -e "At least one XML file should be specified using one of --xml or --list options";
  echo -e "Both options can be provided in which case all the specified tests in both will be run.";
}


# Initialization

scriptLocation="`getPath "$0"`"
scriptDir="`dirname "${scriptLocation}"`"
scriptName="`basename "${scriptLocation}"`"

CSBBDIR="${scriptDir}"
export CSBBDIR

if [ -r "${scriptDir}/runCSFunctions.sh" ]; then
  source "${scriptDir}/runCSFunctions.sh"
else
  echo "Could not read the runCSFunctions.sh script."
  exit 1
fi


# Parse the command-line

# Get the options

xmlList=
xmlListList=
poolopt=
export DBOPTION="false"
useAutoSsh=false
argNum=1
sleepSecs=0
ARGS=
while true; do
  skipArg=false
  getOption "`eval echo \\${${argNum}}`"
  if [ -z "${OPTNAME}" ]; then
    break
  fi
  case "${OPTNAME}" in
    xml) xmlList="`addList "${xmlList}" "${OPTVAL}"`" ;;
    list) xmlListList="`addList "${xmlListList}" "${OPTVAL}"`" ;;
    pool) poolopt="`addList "${poolopt}" "${OPTVAL}"`"
    ;;
    database) DBOPTION="true";;
    auto-ssh) useAutoSsh=true ;;
    at | skip-report) ;;
    sleep) sleepSecs="${OPTVAL}" && skipArg=true ;;
    *) echo "Unknown option [${OPTNAME}]." && echo && showUsage && exit 1 ;;
  esac
  if [ "${skipArg}" != "true" ]; then
    ARGS="${ARGS} `eval echo \\${${argNum}}`"
  fi
  ((argNum++))
done

# Verify validity of options

if [ -z "${xmlList}" -a -z "${xmlListList}" ]; then
  echo "No xml file provided." && echo && showUsage && exit 1
fi


# Get the hostnames

parseHosts "${argNum}" "$@"


bbPort="`getOpenPort`"
driverPort="`getOpenPort`"

# Start the FwkBBServer
set -m

CSFWK_BBADDR="`getIPAddress`:${bbPort}"
export CSFWK_BBADDR
CSFWK_DRIVERADDR="`getIPAddress`:${driverPort}"
export CSFWK_DRIVERADDR
"${scriptDir}/FwkBBServer" "${bbPort}" &

# Set the trap to exit cleanly
trap "stty echo; if [ -n \"\${driverPID}\" ]; then echo Terminating the driver...; echo; \"${scriptDir}/FwkBBClient\" termDriver; wait \${driverPID}; fi; echo Terminating the BB server...; \"${scriptDir}/FwkBBClient\" quit; echo; exit 0" SIGINT SIGTERM

# Sleep for some time to let the BB server start
sleep 5


# Get the passwords from the user and write to BB

loginOpts=
if [ "${useAutoSsh}" = "true" ]; then
  checkLogin
else
  writePasswordsToBB
  loginOpts="--bbPasswd"
fi

if [ "${sleepSecs}" -gt 0 ]; then
  echo "Sleeping for ${sleepSecs} secs..."
  sleep "${sleepSecs}"
fi

echo
echo "Starting FwkDriver."
echo

"${scriptDir}/FwkDriver" "--bbServer=${CSFWK_BBADDR}" ${loginOpts} "--driverPort=${driverPort}" ${ARGS} ${HOSTS} &

driverPID=$!
export driverPID
wait ${driverPID}

# Ending the BB server...
"${scriptDir}/FwkBBClient" quit
