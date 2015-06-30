@setlocal enableextensions
@set scriptdir=%~dp0
@set gf=%scriptdir:\bin\=%
@if exist "%gf%\lib\gemfire.jar" @goto gfok
@echo Could not determine GEMFIRE location
@verify other 2>nul
@goto done
:gfok

@REM Initialize classpath

@REM Add GemFire classes
@set GEMFIRE_JARS=%gf%/lib/gemfire.jar;%gf%/lib/antlr.jar;%gf%/lib/mail.jar

@REM Add Tomcat classes
@set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%/lib/servlet-api.jar;%gf%/lib/catalina.jar;%gf%/lib/${artifact.artifactId}-${artifact.version}.jar;%gf%/bin/tomcat-juli.jar

@REM Add conf directory
@set GEMFIRE_JARS=%GEMFIRE_JARS%;%gf%/conf

@if defined CLASSPATH set GEMFIRE_JARS=%GEMFIRE_JARS%;%CLASSPATH%

@if not defined GF_JAVA (
@REM %GF_JAVA% is not defined, assume it is on the PATH
@set GF_JAVA=java
)

@"%GF_JAVA%" %JAVA_ARGS% -classpath "%GEMFIRE_JARS%" com.gemstone.gemfire.internal.cache.CacheServerLauncher %*
:done
@set scriptdir=
@set gf=
@set GEMFIRE_JARS=

