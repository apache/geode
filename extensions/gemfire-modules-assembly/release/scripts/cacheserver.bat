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

setlocal
setlocal enableextensions
setlocal enabledelayedexpansion
set scriptdir=%~dp0
set gf=%scriptdir:\bin\=%
if exist "%gf%\lib\gemfire.jar" @goto gfok
echo Could not determine GEMFIRE location
rem verify other 2>nul
goto done
:gfok

if exist "%gf%\bin\modules.env" (
  FOR /F "tokens=*" %%i in ('type %gf%\bin\modules.env') do SET %%i
)

rem Check for the -d argument
set /a FOUND_ARG=0
set TC_INSTALL_DIR=
FOR %%A IN (%*) DO (
  if !FOUND_ARG! == 1 (
    set /a FOUND_ARG-=1
    set TC_INSTALL_DIR=%%~fA
  )
  if %%A == -d (
    set /a FOUND_ARG+=1
  )
)

rem Pull out the unused args for the java class
set CLASS_ARGS=
:loop
IF "%1"=="" GOTO ENDLOOP
  if "%1" == "-d" (
    shift
    shift
  ) else ( 
    set "CLASS_ARGS=!CLASS_ARGS! %1" 
    shift
  )
GOTO loop

:ENDLOOP

IF NOT "%TC_INSTALL_DIR%" == "" goto SET_TOMCAT_DIR
FOR /f %%f in ('forfiles /P %gf%\.. /m tomcat-%TOMCAT_MAJOR_VER%* /c "cmd /c echo @path"') do set TOMCAT_DIR=%%f
REM Strip the surrounding quotes
set TOMCAT_DIR=%TOMCAT_DIR:"=%
goto TEST_TOMCAT_DIR

:SET_TOMCAT_DIR
set /p TOMCAT_VER=<"%gf%\conf\tomcat.version"
set TOMCAT_DIR="!TC_INSTALL_DIR!\tomcat-!TOMCAT_VER!"

:TEST_TOMCAT_DIR
if not exist "!TOMCAT_DIR!\lib\catalina.jar" goto TOMCAT_NOT_FOUND
goto FIND_MOD_JAR

:FIND_MOD_JAR
FOR %%f in (!gf!\lib\gemfire-modules-?.*.jar) do set MOD_JAR=%%f
IF NOT "%MOD_JAR%" == "" goto FIND_LOG_API
rem This is the default modules jar
set MOD_JAR="!gf!\lib\gemfire-modules.jar"

:FIND_LOG_API
FOR %%f in (!gf!\lib\log4j-api*.jar) do set LOG_API_JAR=%%f
IF NOT "%LOG_API_JAR%" == "" goto FIND_LOG_CORE
echo ERROR: Log4J API jar not found.
goto LIBS_NOT_FOUND

:FIND_LOG_CORE
FOR %%f in (!gf!\lib\log4j-core*.jar) do set LOG_CORE_JAR=%%f
IF NOT "%LOG_CORE_JAR%" == "" goto MAIN_PROCESSING
echo ERROR: Log4J Core jar not found.
goto LIBS_NOT_FOUND


:LIBS_NOT_FOUND
echo ERROR: The required libraries could not be located. 
echo Try using the -d ^<tc Server installation directory^> option or make sure it was installed correctly.
echo Example: cacheserver.bat start -d "c:\Program Files\Pivotal\tcServer\pivotal-tc-server-standard"
exit /b 1

:TOMCAT_NOT_FOUND
echo ERROR: The TOMCAT libraries could not be located. 
echo Try using the -d ^<tc Server installation directory^> option or make sure it was installed correctly.
echo Example: cacheserver.bat start -d "c:\Program Files\Pivotal\tcServer\pivotal-tc-server-standard"
exit /b 1

:MAIN_PROCESSING
REM Initialize classpath

REM Add GemFire classes
set GEMFIRE_JARS=%MOD_JAR%;%LOG_API_JAR%;%LOG_CORE_JAR%;%gf%/lib/gemfire.jar;%gf%/lib/antlr.jar;%gf%/lib/mail.jar

REM Add Tomcat classes
set GEMFIRE_JARS=%GEMFIRE_JARS%;%TOMCAT_DIR%/lib/servlet-api.jar;%TOMCAT_DIR%/lib/catalina.jar;%gf%/lib/gemfire-modules.jar;%TOMCAT_DIR%/bin/tomcat-juli.jar;%TOMCAT_DIR%/lib/tomcat-util.jar

REM Add conf directory
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%/conf


if defined CLASSPATH set GEMFIRE_JARS=%GEMFIRE_JARS%;%CLASSPATH%

if not defined GF_JAVA (
  REM %GF_JAVA% is not defined, assume it is on the PATH
  set GF_JAVA=java
)

"%GF_JAVA%" %JAVA_ARGS% -classpath "%GEMFIRE_JARS%" com.gemstone.gemfire.internal.cache.CacheServerLauncher !CLASS_ARGS!
:done
set scriptdir=
set gf=
set GEMFIRE_JARS=

endlocal
