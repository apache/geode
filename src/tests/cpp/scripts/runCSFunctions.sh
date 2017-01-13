#!/bin/bash

# This script uses some bash specific constructs like arrays.


getOpenPort()
{
  openPorts="`netstat -an | sed -n 's/^[^\.]*[0-9]\+.[0-9]\+.[0-9]\+.[0-9]\+.\([0-9]\+\)[ \t]\+.*\(LISTEN\|ESTABLISH\).*$/\1/p' | sort -n | uniq`"
  declare -a ports
  for port in ${openPorts}; do
    ports[${port}]=1
  done
  found=false
  while [ "${found}" = "false" ]; do
    newPort="`expr ${RANDOM} % 10000 + 20000`"
    if [ "${ports[${newPort}]}" != "1" ]; then
      found=true
    fi
  done
  unset ports
  echo ${newPort}
}

getOption()
{
  OPTNAME=
  OPTVAL=
  case "$1" in
    --*)
      OPTNAME="`echo "$1" | sed 's/^--//;s/=.*$//'`"
      OPTVAL="`echo "$1" | cut -d '=' -f 2-2`"
    ;;
  esac
}

getIPAddress()
{
  ipAddrs="`ipconfig 2>/dev/null | sed 's/\\r//g' | awk '/IP Address/ { print $NF }'`"
  for ipAddr in ${ipAddrs}; do
    hostName="`nslookup ${ipAddr} 2>/dev/null | sed 's/\\r//g' | awk '/^Name:/{print $2}'`"
    if [ -n "${hostName}" ]; then
      echo ${ipAddr}
      break
    fi
  done
}

getHostName()
{
  ipAddr="`getIPAddress`"
  if [ -n "${ipAddr}" ]; then
    nslookup ${ipAddr} 2>/dev/null | sed 's/\\r//g' | awk '/^Name:/{print $2}'
  fi
}

getHost()
{
  HNAME=
  HGROUP=
  case "$1" in
    *:*)
      HNAME="`echo "$1" | cut -d ':' -f 2-2`"
      HGROUP="`echo "$1" | cut -d ':' -f 1-1`"
    ;;
    *)
      HNAME="$1"
    ;;
  esac
}

readPassword()
{
  PASSWD=
  echo -n "Please enter the password: "
  stty -echo
  read PASSWD
  stty echo
  echo
}

getHostType()
{
  plink -pw "${PASSWD}" "${USER}@$1" "uname"
}

addList()
{
  if [ -z "$1" ]; then
    echo "$2"
  else
    echo "$1,$2"
  fi
}

searchArray()
{
  arraySize="`eval echo \\${#$1[@]}`"
  arrayIndex=1
  while [ "${arrayIndex}" -le "${arraySize}" ]; do
    if [ "`eval echo \\${$1[${arrayIndex}]}`" = "$2" ]; then
      echo ${arrayIndex}
      return
    fi
    ((arrayIndex++))
  done
  echo 0
}

writeBBConfigKeyVal()
{
  if [ -n "$1" -a -n "$2" ]; then
    "${CSBBDIR}/FwkBBClient" set CONFIG "$1" "$2"
  fi
}

declare -a hostNames
declare -a hostGroups

parseHosts()
{
  argNum="$1"
  ((argNum++))
  numHosts=0
  numGroups=0
  HOSTS=
  while true; do
    getHost "`eval echo \\${${argNum}}`"
    if [ -z "${HNAME}" ]; then
      break
    fi
    if [ "${HGROUP}" = "*" ]; then
      echo "Cannot use '*' as the hostGroup." && echo && showUsage && exit 1
    fi
    if echo ${HNAME} | fgrep . >/dev/null; then
      hostName="${HNAME}"
    else
      hostName="`nslookup ${HNAME} 2>/dev/null | sed 's/\\r//g' | awk '/^Name:/{print $2}'`"
    fi
    hostIndex="`searchArray hostNames "${hostName}"`"
    if [ -z "${HGROUP}" ]; then
      HOSTS="${HOSTS} ${hostName}"
    else
      HOSTS="${HOSTS} ${HGROUP}:${hostName}"
    fi
    if [ "${hostIndex}" -le 0 ]; then
      ((numHosts++))
      hostNames[${numHosts}]="${hostName}"
    fi
    groupIndex="`searchArray hostGroups "${HGROUP}"`"
    if [ "${groupIndex}" -le 0 ]; then
      ((numGroups++))
      hostGroups[${numGroups}]="${HGROUP}"
    fi
    ((argNum++))
  done
}

writePasswordsToBB()
{
  hostIndex=1
  numHosts="${#hostNames[@]}"
  readPassword
  while [ "${hostIndex}" -le "${numHosts}" ]; do
    hostName="${hostNames[${hostIndex}]}"
    maxTries=3
    echo -n "Checking password on ${hostName}. "
    hostType="`getHostType "${hostName}"`"
    if [ -n "${hostType}" ]; then
      echo
    else
      while [ "${maxTries}" -gt 0 ]; do
        readPassword
        hostType="`getHostType "${hostName}"`"
        if [ -n "${hostType}" ]; then
          break
        fi
        echo "Incorrect password for server ${hostName}."
        ((maxTries--))
      done
    fi
    if [ -n "${hostType}" ]; then
      writeBBConfigKeyVal "host.${hostName}" "${PASSWD}"
      if [ "`echo ${hostType} | tr "cyglinsu" "CYGLINSU" | cut -b1-3`" = "CYG" ]; then
        hostIsWindows="true"
      else
        hostIsWindows="false"
      fi
      writeBBConfigKeyVal "hostType.${hostName}" "${hostIsWindows}"
    else
      echo "Failed to login to server ${hostName}; removing from hosts list"
    fi
    ((hostIndex++))
  done
}

checkLogin()
{
  hostIndex=1
  numHosts="${#hostNames[@]}"
  while [ "${hostIndex}" -le "${numHosts}" ]; do
    hostName="${hostNames[${hostIndex}]}"
    echo -n "Checking login on ${hostName}. "
    hostType="`ssh -o StrictHostKeyChecking=no "${USER}@${hostName}" "uname"`"
    if [ -n "${hostType}" ]; then
      if [ "`echo ${hostType} | tr "cyglinsu" "CYGLINSU" | cut -b1-3`" = "CYG" ]; then
        hostIsWindows="true"
      else
        hostIsWindows="false"
      fi
      writeBBConfigKeyVal "hostType.${hostName}" "${hostIsWindows}"
    else
      echo "Failed to login to server ${hostName}"
    fi
    ((hostIndex++))
  done
}
