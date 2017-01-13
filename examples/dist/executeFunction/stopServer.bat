@echo off

rem GEMFIRE must be set


if not "%GEMFIRE%"=="" goto startexamples

echo GEMFIRE is not set.
goto finished


:startexamples

echo.
echo Stopping GemFire Server

set PATH=%GEMFIRE%\bin;%PATH%;%GEMFIRE%\bin;..\bin;

call cacheserver stop -dir=gfecs
call cacheserver stop -dir=gfecs2
:finished

