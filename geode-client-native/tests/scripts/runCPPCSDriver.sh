#!/bin/bash


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
  echo -e " Options and hosts are specified the same as to the runDriver script.";
}


# Initialization

scriptLocation="`getPath "$0"`"
scriptDir="`dirname "${scriptLocation}"`"
scriptName="`basename "${scriptLocation}"`"
csharpDir="${scriptDir}/../csharp/bin"

CSBBDIR="${csharpDir}"
export CSBBDIR

if [ -r "${csharpDir}/runCSFunctions.sh" ]; then
  source "${csharpDir}/runCSFunctions.sh"
else
  echo "Could not read the runCSFunctions.sh script."
  exit 1
fi


# Parse the command-line

argNum=1
while [ "${argNum}" -le $# ]; do
  arg="`eval echo \\${${argNum}}`"
  case "${arg}" in
    --*) ;;
    -*) ((argNum++)) ;;
    *) break ;;
  esac
  ((argNum++))
done

# Get the hostnames

numHosts=0
numGroups=0
export numHosts numGroups
parseHosts "${argNum}" "$@"

# Use the local machine in the special "CSD" group
# when none is provided on command-line
csdHost=
csdIndex="`searchArray hostGroups CSD`"
if [ "${csdIndex}" -le 0 ]; then
  thisHost="`getHostName`"
  csdHost="CSD:${thisHost}"
  thisHost="`nslookup ${thisHost} 2>/dev/null | awk '/^Name:/{print $2}'`"
  ((numHosts++))
  hostNames[${numHosts}]="${thisHost}"
  ((numGroups++))
  hostGroups[${numGroups}]="CSD"
fi

bbPort="`getOpenPort`"

# Start the FwkBBServer

CSFWK_BBADDR="`getIPAddress`:${bbPort}"
export CSFWK_BBADDR
echo "CSFWK_BBADDR=\"${CSFWK_BBADDR}\"" > gfcppcsharp.env
echo "export CSFWK_BBADDR" >> gfcppcsharp.env

"${csharpDir}/FwkBBServer" "${bbPort}" &

# Set the trap to exit cleanly closing the C# BBserver and Driver
trap "stty echo; source gfcppcsharp.env; [ -n \"\${CSFWK_BBADDR}\" ] && echo Before exit: Terminating the BB server... && \"${csharpDir}/FwkBBClient\" quit; [ -n \"\${CSFWK_DRIVERADDR}\" ] && echo Before exit: Terminating the Driver... && \"${csharpDir}/FwkBBClient\" termDriver 2>/dev/null; exit 0" SIGINT SIGTERM

# Sleep for some time to let the BB server start
sleep 5


# Get the passwords from the user and write to BB

writePasswordsToBB

echo
echo "Starting runDriver."
echo

"${scriptDir}/runDriver" "$@" ${csdHost}

echo
echo Terminating the BB server...
echo
"${csharpDir}/FwkBBClient" quit
