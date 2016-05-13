#!/bin/bash

[ "$#" -gt 3 -o "$#" -lt 2 ] && echo "`basename "$0"`: Incorrect number[$#] of args: $@." && exit 1

# maximum wait for 30min if no time is given
maxSecs=1800
[ "$#" -eq 3 ] && maxSecs="$3"

bbVal="`FwkBB get "$1" "$2" 2>/dev/null`"
numIters=1
while [ -z "${bbVal}" ]; do
  sleep 1
  if [ "${numIters}" -ge "${maxSecs}" ]; then
    break
  fi
  ((numIters++))
  bbVal="`FwkBB get "$1" "$2" 2>/dev/null`"
done

echo ${bbVal}

if [ -z "${bbVal}" ]; then
  exit 1
else
  exit 0
fi
