#!/bin/sh


getPath()
{
  if uname | grep '^CYGWIN' >/dev/null 2>/dev/null; then
    cygpath "$1"
  else
    echo "$1"
  fi
}

getRealPath()
{
  if uname | grep '^CYGWIN' >/dev/null 2>/dev/null; then
    cygpath -w "$1" | sed 's/[\\\/]*$//'
  else
    realpath "$1" | sed 's/[\\\/]*$//'
  fi
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


EPBB=GFE_BB
EPCNT=EP_COUNT
EPDONE=EP_DONE
EPLABEL=EndPoints


if [ -r "gfcppcsharp.env" ]; then
  source "gfcppcsharp.env"
else
  echo "Could not read the gfcppcsharp.env for environment variables."
  exit 1
fi

# Create the extra environment variables
extraVars="GF_BBADDR"

currDir="`pwd`"
if [ -z "${BASEDIR}" ]; then
  baseDir="${currDir}/.."
else
  baseDir="${BASEDIR}"
fi
if [ -z "${BUILDDIR}" ]; then
  buildDir="${baseDir}/build_CYG"
else
  buildDir="${BUILDDIR}"
fi
GFBASE="`getRealPath "${baseDir}"`"
GFXMLBASE="`getRealPath "${buildDir}/framework/xml"`"
FWK_WINLOGDIR="`getRealPath "${currDir}/csharp"`"

echo "GFBASE=${GFBASE}" > run.env.extra
echo "GFXMLBASE=${GFXMLBASE}" >> run.env.extra
echo "GFCPP=`getRealPath "${buildDir}/product"`" >> run.env.extra
echo "PATH+=`getRealPath "${buildDir}/framework/scripts"`;`getRealPath "${buildDir}/framework/bin"`;`getRealPath "${buildDir}/framework/lib"`;`getRealPath "${buildDir}/hidden/lib"`;`getRealPath "${buildDir}/hidden/gpl"`" >> run.env.extra
echo "FWK_WINLOGDIR=${FWK_WINLOGDIR}" >> run.env.extra
for var in ${extraVars}; do
  varVal="`eval echo \\${${var}}`"
  if [ -n "${varVal}" ]; then
    echo "${var}=${varVal}" >> run.env.extra
  fi
done
mkdir -p "${currDir}/csharp"


driverPort="`getOpenPort`"

CSFWK_DRIVERADDR="`getIPAddress`:${driverPort}"
export CSFWK_DRIVERADDR
echo "CSFWK_DRIVERADDR=\"${CSFWK_DRIVERADDR}\"" >> gfcppcsharp.env
echo "export CSFWK_DRIVERADDR" >> gfcppcsharp.env

cat gfcppcsharp.env > "${baseDir}/../gfcppcsharp.env"

epCnt="`FwkBB getInt ${EPBB} ${EPCNT}`"
[ -z "${epCnt}" ] && echo "Could not get the endpoint count." && exit 1

epIndex=1
endPts=
while [ ${epIndex} -le ${epCnt} ]; do
  endPt="`FwkBB get ${EPBB} "${EPLABEL}_${epIndex}"`"
  if [ -z "${endPts}" ]; then
    endPts="${endPt}"
  else
    endPts="${endPts},${endPt}"
  fi
  epIndex="`expr ${epIndex} + 1`"
done

GFE_DIR="${endPts}"
export GFE_DIR

hostLine=
if [ "`echo ../hosts/*`" != "../hosts/*" ]; then
  for groupPath in ../hosts/*; do
    groupName="`basename "${groupPath}"`"
    if [ "${groupName}" = "DEFAULT" ]; then
      defaultHosts="`cat "${groupPath}"`"
    else
      hostLine="${hostLine} `cat "${groupPath}" | sed "s/^/${groupName}:/"`"
    fi
  done
  hostLine="${hostLine} ${defaultHosts}"
else
  hostLine="${UHOSTS}"
fi

"${csharpDir}/FwkDriver.exe" "--bbServer=${CSFWK_BBADDR}" --bbPasswd --skip-report "--driverPort=${driverPort}" "$@" ${hostLine}
