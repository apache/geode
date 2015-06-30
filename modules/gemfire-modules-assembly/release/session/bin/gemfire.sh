#!/bin/bash
# Set GEMFIRE to the product toplevel directory
GEMFIRE=`dirname $0`
OLDPWD=$PWD
cd $GEMFIRE
GEMFIRE=`dirname $PWD`
cd $OLDPWD

if [ "x$WINDIR" != "x" ]; then
  echo "ERROR: The variable WINDIR is set indicating this script is running in a Windows OS, please use the .bat file version instead."
  exit 1
fi

GEMFIRE_JARS=$GEMFIRE/lib/gemfire.jar
if [ ! -f "${GEMFIRE_JARS}" ]; then
  echo "ERROR: Could not determine GEMFIRE location."
  exit 1
fi

LOG4J_API=$( ls $GEMFIRE/lib/log4j-api*jar )
LOG4J_CORE=$( ls $GEMFIRE/lib/log4j-core*jar )

GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/antlr.jar:$LOG4J_API:$LOG4J_CORE

# Initialize classpath
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/gemfire-modules-${gemfire.modules.version}.jar
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/gemfire-modules-session-${gemfire.modules.version}.jar
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/gemfire-modules-session-external-${gemfire.modules.version}.jar
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/servlet-api-${servlet-api.version}.jar
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/slf4j-api-${slf4j.version}.jar
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/slf4j-jdk14-${slf4j.version}.jar

if [ "x$CLASSPATH" != "x" ]; then
  GEMFIRE_JARS=$GEMFIRE_JARS:$CLASSPATH
fi

# Command line args that start with -J will be passed to the java vm in JARGS.
# See java --help for a listing of valid vm args.
# Example: -J-Xmx1g sets the max heap size to 1 gigabyte.

JARGS=
GEMFIRE_ARGS=
for i in "$@"
do
  if [ "-J" == "${i:0:2}" ]
  then
    JARGS="${JARGS} \"${i#-J}\""
  else
    GEMFIRE_ARGS="${GEMFIRE_ARGS} \"${i}\""
  fi
done

eval ${GF_JAVA:-java} ${JAVA_ARGS} ${JARGS} -classpath ${GEMFIRE_JARS} com.gemstone.gemfire.internal.SystemAdmin ${GEMFIRE_ARGS}
