#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
for (( i=0; i<$ARGS_LENGTH; i++ ));
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

GEMFIRE_DEP_JAR=$GEMFIRE/lib/gemfire-core-dependencies.jar
if [ ! -f "$GEMFIRE_DEP_JAR" ]; then
  echo "ERROR: Could not determine GEMFIRE location."
  exit 1
fi

MOD_JAR=`ls $GEMFIRE/lib/gemfire-modules-?.*.jar` 2>/dev/null
if [ -z "$MOD_JAR" ]; then
  MOD_JAR=$GEMFIRE/lib/gemfire-modules.jar
fi

# Add Tomcat classes
GEMFIRE_JARS=$GEMFIRE_DEP_JAR:$MOD_JAR:$TOMCAT_DIR/lib/servlet-api.jar:$TOMCAT_DIR/lib/catalina.jar:$TOMCAT_DIR/lib/tomcat-util.jar:$TOMCAT_DIR/bin/tomcat-juli.jar

# Add configuration
GEMFIRE_JARS=$GEMFIRE_JARS:$GEMFIRE/conf

if [ "x$CLASSPATH" != "x" ]; then
  GEMFIRE_JARS=$GEMFIRE_JARS:$CLASSPATH
fi

${GF_JAVA:-java} ${JAVA_ARGS} -classpath ${GEMFIRE_JARS} com.gemstone.gemfire.internal.cache.CacheServerLauncher ${CLASS_ARGS}
