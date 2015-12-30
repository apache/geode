@echo off
rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem      http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

setlocal enableextensions
set scriptdir=%~dp0
set gf=%scriptdir:\bin\=%

set GEMFIRE_JARS=%gf%\lib\gemfire.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\antlr.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\spring-core-3.1.1.RELEASE.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\spring-shell-1.0.0.RC1.jar
if exist "%GEMFIRE_JARS%" goto gfok
echo Could not determine GEMFIRE location
verify other 2>nul
goto done
:gfok

REM Initialize classpath
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\gemfire-modules-@GEMFIRE_MODULES_VERSION@.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\gemfire-modules-session-@GEMFIRE_MODULES_VERSION@.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\gemfire-modules-session-external-@GEMFIRE_MODULES_VERSION@.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\servlet-api-@SERVLET_API_VERSION@.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\slf4j-api-@SLF4J_VERSION@.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\slf4j-jdk14-@SLF4J_VERSION@.jar

if defined CLASSPATH set GEMFIRE_JARS=%GEMFIRE_JARS%;%CLASSPATH%

if not defined GF_JAVA (
REM %GF_JAVA% is not defined, assume it is on the PATH
set GF_JAVA=java
)

"%GF_JAVA%" %JAVA_ARGS% -classpath "%GEMFIRE_JARS%" com.gemstone.gemfire.internal.SystemAdmin %*
:done
set scriptdir=
set gf=
set GEMFIRE_JARS=

