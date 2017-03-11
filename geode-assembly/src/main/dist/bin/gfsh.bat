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
REM GEODE_HOME - Geode product Directory
REM
REM

@setlocal enableextensions
@set scriptdir=%~dp0
@set gf=%scriptdir:\bin\=%
REM echo %gf%
REM echo %scriptdir%
@if exist "%gf%\lib\geode-dependencies.jar" @goto gfok
@echo Could not determine GEODE_HOME location
@verify other 2>nul
@goto done
:gfok
@set GEODE_HOME=%gf%

@set GFSH_JARS=%GEODE_HOME%\lib\gfsh-dependencies.jar
REM if a system level classpath is set, append it to the classes gfsh will need
@if defined CLASSPATH (
    @set DEPENDENCIES=%GFSH_JARS%;%CLASSPATH%
) else (
    @set DEPENDENCIES=%GFSH_JARS%
)

@if not defined GF_JAVA (
REM %GF_JAVA% is not defined, assume it is on the PATH
    @if defined JAVA_HOME (
    @set GF_JAVA=%JAVA_HOME%\bin\java.exe
) else (
    @set GF_JAVA=java
  )
)

REM
REM Copy default .gfshrc to the home directory. Uncomment if needed.
REM
REM @if not exist "%USERPROFILE%\.gemfire\.gfsh2rc" (
REM @xcopy /q "%GEODE_HOME%\defaultConfigs\.gfsh2rc" "%USERPROFILE%\.gemfire"
REM )

REM
REM Make dir if .gemfire does not exist. Uncomment if needed.
REM
REM @if not exist "%USERPROFILE%\.gemfire" (
REM @mkdir "%USERPROFILE%\.gemfire"
REM )

@set LAUNCHER=org.apache.geode.management.internal.cli.Launcher
@if defined JAVA_ARGS (
    @set JAVA_ARGS="%JAVA_ARGS%"
)

REM Call java with our classpath
@"%GF_JAVA%" -Dgfsh=true -Dlog4j.configurationFile=classpath:log4j2-cli.xml -classpath "%DEPENDENCIES%" %JAVA_ARGS% %LAUNCHER% %*
:done
