#!/bin/sh
#set -x
script=`basename $0`
OSBUILDDIR=$1 # c++ build directory path
JAVA_HOME=$2  # java path 
GEMFIRE=$3    # gemfire path
RESULT_DIR=$4    # directory path from where you start the tests
#HYDRA_DIR=$7
echo $RESULT_DIR
export PERFTEST=true
#Do framework setup, mostly creating the config files
#/export/bass2/users/rkumar/project/ThinClient_3100/build-artifacts/linux/framework/scripts/runPerfTest /export/bass2/users/rkumar/project/ThinClient_3100/build-artifacts/linux /export/gcm/where/jdk/1.6.0_7/x86_64.linux/bin/java /gcm/where/gemfire/60/snaps.Any/snapshots.24944/gf60MAINTsancout/product . w1-gst-dev01 w1-gst-dev02 w1-gst-dev03 w1-gst-dev04 w1-gst-dev05 w1-gst-dev06 w1-gst-dev07 w1-gst-dev08

if [ $# -lt 6 ]
then
  echo "Problem executing script: $script"
  echo ""
  echo "Too few arguments were specified."
  echo ""
  echo "Usage:   path_to_script/$script OSBUILDDIR JAVA_HOME GEMFIRE RESULT_DIR HOST1 HOST2"
  echo ""
  echo " Where:"
  echo " OSBUILDDIR -- C++ build directory"
  echo " JAVA_HOME -- Java path"
  echo " GEMFIRE -- Gemfire java path"
  echo " RESULT_DIR -- Directory to use as cwd when starting test"
  echo " HOST1 -- First hosts to use in test"
  echo " HOST2 -- Second hosts to use in test"
  echo ""
  exit
fi
cp $OSBUILDDIR/hidden/internal.license.nativeclientonly.zip gfcpp.native.license
myOS=`uname | tr "cyglinsu" "CYGLINSU" | cut -b1-3`

cat << __ENV_END  > $RESULT_DIR/gfcpp.env
GF_JAVA_${myOS}_32=$JAVA_HOME
GF_JAVA_${myOS}_64=$JAVA_HOME
GF_JAVA=$JAVA_HOME
GFE_DIR=$GEMFIRE
GF_DEBUG_WAIT=600
__ENV_END

cat << __GFE_PROPS_END > $RESULT_DIR/gfcpp.gfe.properties
__GFE_PROPS_END

cat << __CPP_PROPS_END > $RESULT_DIR/gfcpp.properties
log-level=info
license-file=gfcpp.native.license
__CPP_PROPS_END

host1=$5

#call runDriver
# $5 and $6 are the host names.
$OSBUILDDIR/framework/scripts/runDriver -l scale.list CS1:$5 CS1:$6 CS1:$7 CS1:$8 CS2:$9 CS2:$10 CS3:$11 CS3:$12
#rm -f $RESULT_DIR/latest.prop $RESULT_DIR/perfcom*
#Generate report
#JTESTS=$GEMFIRE/../tests/classes
#$JAVA_HOME -cp $HYDRA_DIR:$GEMFIRE/lib/gemfire.jar <fillout command line to generate reports>
#$JAVA_HOME -cp $GEMFIRE/lib/gemfire.jar:$JTESTS -Xmx750m -Dmode=raw -DomitFailedTests=true -DcompareByKey=true -DJTESTS=$JTESTS -Dgemfire.home=$GEMFIRE -DaddTestKey=true perffmwk.PerfComparer latest
