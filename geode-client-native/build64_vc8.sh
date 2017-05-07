#!/bin/bash
# Set BASEDIR to be the toplevel checkout directory.
# We do this so that BASEDIR can be used elsewhere if needed
#set -xv
BASEDIR=`/usr/bin/dirname $0`
OLDPWD=$PWD
cd $BASEDIR
#export BASEDIR=`/usr/bin/dirname $PWD`
export BASEDIR=$PWD
cd $OLDPWD

ulimit -c 500000

unset GEMFIRE
export GEMFIRE

GCMDIR="${GCMDIR:-/export/gcm}"
THIRDPARTY_BASE="${THIRDPARTY_BASE:-${GCMDIR}/where/cplusplus/thirdparty}"
CYGWIN=""

if [ -f $BASEDIR/myBuild.sh ]
then 
  . $BASEDIR/myBuild.sh
fi

PING="$SYSTEMROOT/system32/ping -n 1 ldap.pune.gemstone.com"

if [ `uname` = "SunOS" ]; then
  echo "This script should be executed only for windows platform.";
  exit 1
elif [ `uname` = "Linux" ]; then
  echo "This script should be executed only for windows platform.";
  exit 1
else
  echo "Defaulting to Windows build"
  THIRDPARTY_BASE=${THIRDPARTY_BASE:-//n080-fil01/cplusplus2/users/gcmlinkdir/thirdparty}
  export THIRDPARTY=${CPP_THIRDPARTY:-${THIRDPARTY_BASE}/windows}
  
  . ./buildfiles/vcvars64_8.sh
 
  logfile=buildWin.log
  NO_BUILD_LOG=1
  # detect compiler version
  CYGWIN=true
fi

export JAVA_HOME=${ALT_JAVA_HOME:-${THIRDPARTY_BASE}/common/jdk1.7.0_72/x86_64.Windows_NT}
export ANT_HOME=${ALT_ANT_HOME:-${THIRDPARTY_BASE}/common/ant/apache-ant-1.8.4}
if [ -z "${CYGWIN}" ]; then
  export PATH="${ANT_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
else
  export PATH="`cygpath "${ANT_HOME}/bin"`:`cygpath "${JAVA_HOME}/bin"`:${PATH}"
fi
unset CLASSPATH

export ANT_OPTS=-Xmx200M

function logant {
  # cygwin tee causes hang on windows
  $ANT_HOME/bin/ant --noconfig -DcPointerModel=64bit -Dplatforms=x64 -DVCVER=8 -DBUG481=1 -Dthirdparty.dir=${THIRDPARTY} -Dthirdparty_base.dir=${THIRDPARTY_BASE} -Dgcm.dir=${GCMDIR} ${ANT_ARGS:-""} "$@"
 }

echo "JAVA_HOME = $JAVA_HOME"
echo "ANT_HOME = $ANT_HOME"
date "+%a %D %H.%M.%S"

# setup the LDAP server for Pune/Beaverton networks;

if [ -z "${LDAP_SERVER}" ]; then
  if expr `$PING | sed -n 's/^.* time[^ ]\([0-9\.]*\).*$/\1/p'` '<' 50 >/dev/null 2>/dev/null; then
    LDAP_SERVER="ldap.pune.gemstone.com"
  else
    LDAP_SERVER="ldap.gemstone.com"
  fi
fi

export LDAP_SERVER
echo "Using LDAP server: $LDAP_SERVER"

# ant likes to be in the directory that build.xml is in
{ cd "${BASEDIR}" &&
if [[ "x$NO_BUILD_LOG" = "x" ]]; then
  logant "$@"
else
  echo "running $ANT_HOME/bin/ant "
  $ANT_HOME/bin/ant --noconfig -DcPointerModel=64bit -Dplatforms=x64 -DVCVER=8 -DBUG481=1 -Dthirdparty.dir=${THIRDPARTY} -Dthirdparty_base.dir=${THIRDPARTY_BASE} -Dgcm.dir=${GCMDIR} "$@"
fi; }
result=$?
date "+%a %D %H.%M.%S"
exit $result
