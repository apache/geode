#!/bin/bash
# Set BASEDIR to be the toplevel checkout directory.
# We do this so that BASEDIR can be used elsewhere if needed
#set -xv

BASEDIR=`/usr/bin/dirname $0`
OLDPWD=$PWD
cd $BASEDIR
export BASEDIR=`/usr/bin/dirname $PWD`
cd $OLDPWD

unset GEMFIRE
export GEMFIRE

export GCMDIR=${GCMDIR:-"/gcm"}

if [ `uname` = "SunOS" ]; then
  export JAVA_HOME=${ALT_JAVA_HOME:-$GCMDIR/where/jdk/1.6.0_26/sparc.Solaris}
  logfile=buildSol.log
elif [ `uname` = "Darwin" ]; then
  export JAVA_HOME=${ALT_JAVA_HOME:-/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home}
  logfile=buildMac.log
elif [ `uname` = "Linux" ]; then
  export JAVA_HOME=${ALT_JAVA_HOME:-$GCMDIR/where/jdk/1.6.0_26/x86.linux}
  logfile=buildLinux.log
else
  echo "Defaulting to Windows build"
  export GCMDIR=${GCMDIR:-"J:\\"}
  export JAVA_HOME=${ALT_JAVA_HOME:-$GCMDIR/where/jdk/1.6.0_26/x86.Windows_NT}
  logfile=buildWin.log
  if [ ! -d "$GCMDIR" ]; then
    echo "ERROR: unable to locate GCMDIR "$GCMDIR" maybe you forgot to map the J: network drive to //samba/gcm"
    exit 1
  fi
#  NO_BUILD_LOG=1
fi

export ANT_HOME=${ALT_ANT_HOME:-$GCMDIR/where/java/ant/apache-ant-1.8.2}
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$PATH
export CLASSPATH=${TASKDEFS:-$GCMDIR/where/java/ant/gemstoneExtensions/lib/ant-contrib-1.0b3.jar}


export ANT_OPTS="-Xmx384m"

function logant {
#  if [[ `uname` == "SunOS" || `uname` == "Linux" ]]; then
    rm -f .xbuildfailure
    ( $ANT_HOME/bin/ant --noconfig "$@" || echo "$?" > .xbuildfailure ) 2>&1 | tee $logfile
    if [ -r .xbuildfailure ]; then
      read stat <.xbuildfailure
      rm -f .xbuildfailure
      exit $stat
    fi
#
    # cygwin tee causes hang on windows
#    $ANT_HOME/bin/ant --noconfig -DuseSSH=false "$@"
#  fi
}

echo "JAVA_HOME = $JAVA_HOME"
echo "ANT_HOME  = $ANT_HOME"
date

# ant likes to be in the directory that build.xml is in
if [[ "x$NO_BUILD_LOG" = "x" ]]; then
  logant "$@"
else
  echo "running $ANT_HOME/bin/ant "
  $ANT_HOME/bin/ant --noconfig "$@"
fi

