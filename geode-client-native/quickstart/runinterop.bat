@echo off

if not exist interop\InteropCPP.exe goto notexist
if not exist interop\InteropCSHARP.exe goto notexist
if not exist interop\InteropJAVA.class goto notexist

echo.
echo Running GemFire QuickStart Interop example ...

set CLASSPATH=%GEMFIRE%\lib\gemfire.jar;./interop

if not exist gfecs mkdir gfecs

call cacheserver start cache-xml-file=../XMLs/serverInterop.xml -dir=gfecs

start /b interop\InteropCPP.exe checkcsharp

start /b interop\InteropCSHARP.exe

start /b java InteropJAVA checkcsharp

rem call cacheserver stop -dir=gfecs

rem echo Please review the example's log output above then

rem pause

goto closeup

:notexist

echo.
echo Interop example not found, please check whether it is built.
echo.

:closeup

echo Finished Interop example.

:finished
