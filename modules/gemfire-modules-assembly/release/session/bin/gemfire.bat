@echo off
@echo off
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
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\gemfire-modules-${gemfire.version}.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\gemfire-modules-session-${gemfire.version}.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\gemfire-modules-session-external-${gemfire.version}.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\servlet-api-${servlet-api.version}.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\slf4j-api-${slf4j.version}.jar
set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%\lib\slf4j-jdk14-${slf4j.version}.jar

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

