#!/bin/sh


getPath()
{
  if uname | grep '^CYGWIN' >/dev/null 2>/dev/null; then
    cygpath "$1"
  else
    echo "$1"
  fi
}


# Initialization

scriptLocation="`getPath "$0"`"
scriptDir="`dirname "${scriptLocation}"`"
scriptName="`basename "${scriptLocation}"`"
csharpDir="${scriptDir}/../csharp/bin"


if [ -r "gfcppcsharp.env" ]; then
  source "gfcppcsharp.env"
else
  echo "Could not read the gfcppcsharp.env for environment variables."
  exit 1
fi

echo
echo "Terminating the C# framework driver..."
echo

"${csharpDir}/FwkBBClient" termDriver
exit 0
