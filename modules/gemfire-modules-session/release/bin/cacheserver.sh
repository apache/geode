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

GEMFIRE_JARS=$GEMFIRE/lib/gemfire-[0-9]*.jar
if [ ! -f ${GEMFIRE_JARS} ]; then
  echo "ERROR: Could not determine GEMFIRE location."
  exit 1
fi

# Initialize classpath
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/gemfire-modules-[0-9]*
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/servlet-api-[0-9]*
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/slf4j-api-[0-9]*
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/lib/slf4j-jdk14-[0-9]*

# Add configuration
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/conf

if [ "x$CLASSPATH" != "x" ]; then
  GEMFIRE_JARS=$GEMFIRE_JARS:$CLASSPATH
fi

${GF_JAVA:-java} ${JAVA_ARGS} -classpath ${GEMFIRE_JARS} com.gemstone.gemfire.internal.cache.CacheServerLauncher "$@"
