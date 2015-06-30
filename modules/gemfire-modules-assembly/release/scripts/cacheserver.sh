#!/usr/bin/env bash

#
# Copyright 2015 Pivotal Software, Inc
# cacheserver.sh - Script used to control the cacheserver
# Release Version - @bundle.version@
#

# Set GEMFIRE to the product toplevel directory
GEMFIRE=`dirname $0`
OLDPWD=$PWD
cd $GEMFIRE
GEMFIRE=`dirname $PWD`
cd $OLDPWD

TOMCAT_VER=`cat "${GEMFIRE}/conf/tomcat.version"`

if [ -f $GEMFIRE/bin/modules.env ]; then
  # Pull in TOMCAT_MAJOR_VER
  . $GEMFIRE/bin/modules.env
fi

# Pull out the installation directory arguments passed in
ARGS=( "$@" )
ARGS_LENGTH=${#ARGS[@]}
CLASS_ARGS=()
for (( i==0; i<$ARGS_LENGTH; i++ )); 
do
	if [ "${ARGS[$i]}" == "-d" ]; then
		i=$(($i+1))
	else 
		CLASS_ARGS="${CLASS_ARGS} ${ARGS[$i]}"
	fi
done
# End pulling out arguments

# See if the user specified the tomcat installation directory location
while [ $# -gt 0 ]; do
  case $1 in
    -d )
      TC_INSTALL_DIR="$2"
      break
      ;;
  esac
  shift
done


if [[ -n $TC_INSTALL_DIR && -d $TC_INSTALL_DIR ]]; then
  TOMCAT_DIR="$TC_INSTALL_DIR/tomcat-${TOMCAT_VER}"
else
  TOMCAT_DIR=`ls -d "${GEMFIRE}"/../tomcat-${TOMCAT_MAJOR_VER}* 2> /dev/null`
fi

if [[ -z "$TOMCAT_DIR" || ! -f "$TOMCAT_DIR/lib/catalina.jar" ]]; then
  echo "ERROR: Could not determine TOMCAT library location."
  echo "       Use the -d <tc Server installation directory> option."
  echo "       Example: ./cacheserver.sh start -d /opt/pivotal/tcserver/pivotal-tc-server-standard"
  exit 1
fi

if [ "x$WINDIR" != "x" ]; then
  echo "ERROR: The variable WINDIR is set indicating this script is running in a Windows OS, please use the .bat file version instead."
  exit 1
fi

if [ ! -f $GEMFIRE/lib/gemfire.jar ]; then
  echo "ERROR: Could not determine GEMFIRE location."
  exit 1
fi

# Initialize classpath

LOG4J_API=$( ls $GEMFIRE/lib/log4j-api*jar )
LOG4J_CORE=$( ls $GEMFIRE/lib/log4j-core*jar )

MOD_JAR=`ls $GEMFIRE/lib/gemfire-modules-?.*.jar` 2>/dev/null
if [ -z "$MOD_JAR" ]; then
  MOD_JAR=$GEMFIRE/lib/gemfire-modules.jar
fi

# Add GemFire classes
GEMFIRE_JARS=$GEMFIRE/lib/gemfire.jar:$GEMFIRE/lib/antlr.jar:$LOG4J_API:$LOG4J_CORE

# Add Tomcat classes
GEMFIRE_JARS=$GEMFIRE_JARS:$MOD_JAR:$TOMCAT_DIR/lib/servlet-api.jar:$TOMCAT_DIR/lib/catalina.jar:$TOMCAT_DIR/lib/tomcat-util.jar:$TOMCAT_DIR/bin/tomcat-juli.jar

# Add configuration
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/conf

if [ "x$CLASSPATH" != "x" ]; then
  GEMFIRE_JARS=$GEMFIRE_JARS:$CLASSPATH
fi

${GF_JAVA:-java} ${JAVA_ARGS} -classpath ${GEMFIRE_JARS} com.gemstone.gemfire.internal.cache.CacheServerLauncher ${CLASS_ARGS}
