@echo off
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM      http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

REM
REM Environment variables:
REM
REM GF_JAVA - java executable path. 
REM
REM JAVA_ARGS - java arguments, i.e., -Xms1024m -Xmx1024m ... 
REM
REM GEMFIRE - GemFire product Directory
REM
REM

@setlocal enableextensions
@set scriptdir=%~dp0
@set gf=%scriptdir:\bin\=%
REM echo %gf%
REM echo %scriptdir%
@if exist "%gf%\lib\geode-dependencies.jar" @goto gfok
@echo Could not determine GEMFIRE location
@verify other 2>nul
@goto done
:gfok
@set GEMFIRE=%gf%

@set GEMFIRE_JARS=%GEMFIRE%\lib\gfsh-dependencies.jar
@if defined CLASSPATH (
@set GEMFIRE_JARS=%GEMFIRE_JARS%;%CLASSPATH%
)

@if not defined GF_JAVA (
@REM %GF_JAVA% is not defined, assume it is on the PATH
@if defined JAVA_HOME (
@set GF_JAVA=%JAVA_HOME%\bin\java.exe
) else (
@set GF_JAVA=java
)
) 

REM
REM GFSH_JARS
REM
@set GFSH_JARS=;%GEMFIRE%\lib\gfsh-dependencies.jar
@set CLASSPATH=%GFSH_JARS%;%GEMFIRE_JARS%

REM
REM Copy default .gfshrc to the home directory. Uncomment if needed.
REM
REM @if not exist "%USERPROFILE%\.gemfire\.gfsh2rc" (
REM @xcopy /q "%GEMFIRE%\defaultConfigs\.gfsh2rc" "%USERPROFILE%\.gemfire"
REM )

REM
REM Make dir if .gemfire does not exist. Uncomment if needed.
REM
REM @if not exist "%USERPROFILE%\.gemfire" (
REM @mkdir "%USERPROFILE%\.gemfire"
REM )

REM  Consider java is from JDK
@set TOOLS_JAR=%JAVA_HOME%\lib\tools.jar
@IF EXIST "%TOOLS_JAR%" (
    @set CLASSPATH=%CLASSPATH%;%TOOLS_JAR%
) ELSE (
    set TOOLS_JAR=
)

@set LAUNCHER=com.gemstone.gemfire.management.internal.cli.Launcher
@if defined JAVA_ARGS (
@set JAVA_ARGS="%JAVA_ARGS%"
)
@"%GF_JAVA%" -Dgfsh=true -Dlog4j.configurationFile=classpath:log4j2-cli.xml %JAVA_ARGS% %LAUNCHER% %*
:done
