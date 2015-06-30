@echo off
set scriptdir=%~dp0
set BASEDIR=%scriptdir:\buildfiles\=%
if exist "%BASEDIR%\build.xml" @goto baseok
echo Could not determine BASEDIR location
verify other 2>nul
goto done
:baseok

set GEMFIRE=

if not defined GCMDIR (
  set GCMDIR=J:\
)
if not exist %GCMDIR% (
  echo "ERROR: unable to locate GCMDIR %GCMDIR% maybe you forgot to map the J: network drive to //samba/gcm"
  verify other 2>nul
  goto done
)

set LDAP_SERVER_FQDN=ldap.gemstone.com
if exist \\inf1\shared\users (
  set LDAP_SERVER_FQDN=ldap.pune.gemstone.com
)

set JAVA_HOME=%GCMDIR%\where\jdk\1.6.0_26\x86.Windows_NT
if defined ALT_JAVA_HOME (
  set JAVA_HOME=%ALT_JAVA_HOME%
)
set ANT_HOME=%GCMDIR%\where\java\ant\apache-ant-1.8.2
if defined ALT_ANT_HOME (
  set ANT_HOME=%ALT_ANT_HOME%
)
set ANT_ARGS=%ANT_ARGS% -lib %GCMDIR%\where\java\jcraft\jsch\jsch-0.1.44\jsch-0.1.44.jar
set PATHOLD=%PATH%
set PATH=%JAVA_HOME%\bin;%PATH%

echo JAVA_HOME = %JAVA_HOME%
echo ANT_HOME = %ANT_HOME%
echo CLASSPATH = %CLASSPATH%
echo %DATE% %TIME%

echo running %ANT_HOME%\bin\ant.bat
call %ANT_HOME%\bin\ant.bat %*
if not defined ERRORLEVEL set ERRORLEVEL=0

:done
echo %ERRORLEVEL% > .xbuildfailure
set ERRORLEVEL=
if defined PATHOLD set PATH=%PATHOLD%
