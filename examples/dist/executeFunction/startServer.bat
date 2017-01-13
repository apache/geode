@echo off

rem GFCPP must be set
rem GEMFIRE must be set


if not "%GEMFIRE%"=="" goto startexamples

echo GEMFIRE is not set.
goto finished


:startexamples


:runexample

echo.
echo Running GemFire Server

set CLASSPATH=%CLASSPATH%;../javaobject.jar;
set PATH=%GEMFIRE%\bin;%PATH%;%GEMFIRE%\bin;..\bin;

if not exist gfecs mkdir gfecs
if not exist gfecs2 mkdir gfecs2



call cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions.xml mcast-port=35673 -dir=gfecs
call cacheserver start cache-xml-file=../XMLs/serverExecuteFunctions2.xml mcast-port=35673 -dir=gfecs2

rem pause

:finished
