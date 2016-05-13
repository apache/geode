#!/bin/bash
# Set BASEDIR to be the toplevel checkout directory.
# We do this so that BASEDIR can be used elsewhere if needed
#set -xv


echo "deprecated"
exit 1

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
THIRDPARTY_JAVA_BASE="${THIRDPARTY_BASE}/common/jdk1.7.0_79"
CYGWIN=""

if [ -f $BASEDIR/myBuild.sh ]
then 
  . $BASEDIR/myBuild.sh
fi

if [ -x $BASEDIR/buildfiles/nprocs ]; then
  nprocs=`$BASEDIR/buildfiles/nprocs`
  ANT_ARGS="${ANT_ARGS} -Dant.make.threads=-j${nprocs}"
fi


if [ `uname` = "SunOS" ]; then
  _PLAT="solaris"
  PING="/usr/sbin/ping -s ldap.pune.gemstone.com 56 1"
  if [ `uname -p` = "sparc" ]; then
    _ARCH="sparc"
    export THIRDPARTY=${CPP_THIRDPARTY:-${THIRDPARTY_BASE}/solaris}
    logfile=buildSol.log
  else
    _ARCH="x86"
    export THIRDPARTY=${CPP_THIRDPARTY:-${THIRDPARTY_BASE}/solx86}
    logfile=buildsolx86.log
  fi

  echo "Building for ${_PLAT} on ${_ARCH}"

  export JAVA_HOME=${ALT_JAVA_HOME:-${THIRDPARTY_JAVA_BASE}/${_ARCH}.Solaris}
  
  export CC_HOME=${ALT_CC_HOME:-/export/gcm/where/cplusplus/compiler/solaris/${_ARCH}/sunstudio12.1/prod}
  if [ -x "${CC_HOME}/bin/CC" ]; then
    export SunCompilerDir=${CC_HOME}/bin
  else
    echo "Sun C++ compiler not found at ${CC_HOME}";
    exit 1
  fi

  export PATH=$SunCompilerDir:$PATH

#  RequiredVer2="CC: Sun C++ 5.10 SunOS_i386 128229-09 2010/06/24"
  SunCompilerVer=`$SunCompilerDir/CC -V 2>&1 `
  echo "Using Sun C++ from $SunCompilerDir "
  echo "    version   $SunCompilerVer "

elif [ `uname` = "Linux" ]; then

  PING=`ping -c 1 ldap.pune.gemstone.com`
  
  export THIRDPARTY=${CPP_THIRDPARTY:-${THIRDPARTY_BASE}/linux}

  #GCCBIN=${THIRDPARTY}/gcc3_2_3/bin
  export GCCBIN=/usr/bin
  export JAVA_HOME=${ALT_JAVA_HOME:-${THIRDPARTY_JAVA_BASE}/x86.linux}
  export PATH=${GCCBIN}:$PATH
  export GccCCompiler=${GCCBIN}/gcc
  export GccCplusplusCompiler=${GCCBIN}/g++

  GccCompilerVer=`$GccCCompiler --version | head -1  2>&1 `

  echo "Using gcc version: $GccCompilerVer"
  logfile=buildLinux.log
  export PATH=`dirname $GccCplusplusCompiler`:$PATH
else
  echo "Defaulting to Windows build"
  PING=`$SYSTEMROOT/system32/ping -n 1 ldap.pune.gemstone.com`
  # suppress DOS path warnings
  if [ -z "${CYGWIN}" ]; then
    export CYGWIN="nodosfilewarning"
  else
    export CYGWIN="${CYGWIN} nodosfilewarning"
  fi
  THIRDPARTY_BASE=${THIRDPARTY_BASE:-${GCMDIR}/where/cplusplus/thirdparty}
  export THIRDPARTY=${CPP_THIRDPARTY:-${THIRDPARTY_BASE}/windows}
  export DXROOT=${THIRDPARTY}/sandcastle_2.7.1.0
  export SHFBROOT=${THIRDPARTY}/SandcastleBuilder_1.9.5.0
 
  . ./buildfiles/vcvars32_10.sh
  
  logfile=buildWin.log
  NO_BUILD_LOG=1
  # detect compiler version
  CYGWIN=true
fi

export ANT_HOME=${ALT_ANT_HOME:-${THIRDPARTY_BASE}/common/ant/apache-ant-1.8.4}
if [ -z "${CYGWIN}" ]; then
  export PATH="${ANT_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
else
  export JAVA_HOME=${ALT_JAVA_HOME:-${THIRDPARTY_JAVA_BASE}/x86.Windows_NT}
  export PATH="`cygpath "${ANT_HOME}/bin"`:`cygpath "${JAVA_HOME}/bin"`:${PATH}"
fi
unset CLASSPATH

export ANT_OPTS=-Xmx200M

function logant {
  if [[ `uname` == "SunOS" || `uname` == "Linux" ]]; then
    rm -f .xbuildfailure
    ( $ANT_HOME/bin/ant --noconfig -Dthirdparty.dir=${THIRDPARTY} -Dthirdparty_base.dir=${THIRDPARTY_BASE} -Dgcm.dir=${GCMDIR} ${ANT_ARGS} "$@" || echo "$?" > .xbuildfailure ) 2>&1 | tee $logfile
    if [ -r .xbuildfailure ]; then
      read stat <.xbuildfailure
      rm -f .xbuildfailure
      exit $stat
    fi
  else
	# cygwin tee causes hang on windows
    $ANT_HOME/bin/ant --noconfig -Dplatforms="Win32" -DVCVER=10 -Dthirdparty.dir=${THIRDPARTY} -Dthirdparty_base.dir=${THIRDPARTY_BASE} -Dgcm.dir=${GCMDIR} ${ANT_ARGS} "$@"
  fi
}

echo "JAVA_HOME = $JAVA_HOME"
echo "ANT_HOME = $ANT_HOME"
date "+%a %D %H.%M.%S"

# setup the LDAP server for Pune/Beaverton networks;

if [ -z "${LDAP_SERVER}" ]; then
  # Solaris ping returns extra character so trim this off for that platform only
  if [ `uname` = "SunOS" ]; then
    PINGTEMP=`echo $PING | sed -n 's/^.* time[^ ]\([0-9\.]*\).*$/\1/p'`
    echo PINGTEMP | grep \. >/dev/null
    if [ $? -eq 0 ]; then
      PING=`echo $PINGTEMP | sed s'/.$//'`
    else
      PING=$PINGTEMP
    fi
    if expr `echo $PING '<' 50` >/dev/null 2>/dev/null; then
      LDAP_SERVER="ldap.pune.gemstone.com"
    else
      LDAP_SERVER="ldap.gemstone.com"
    fi
  else
    if expr `echo $PING | sed -n 's/^.* time[^ ]\([0-9\.]*\).*$/\1/p'` '<' 50 >/dev/null 2>/dev/null; then
      LDAP_SERVER="ldap.pune.gemstone.com"
    else
      LDAP_SERVER="ldap.gemstone.com"
    fi
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
  $ANT_HOME/bin/ant --noconfig -Dplatforms="Win32" -DVCVER=10 -Dthirdparty.dir=${THIRDPARTY} -Dthirdparty_base.dir=${THIRDPARTY_BASE} -Dgcm.dir=${GCMDIR} "$@"
fi; }
result=$?
date "+%a %D %H.%M.%S"
exit $result
