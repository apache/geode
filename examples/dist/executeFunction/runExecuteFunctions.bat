@echo off

rem GFCPP must be set

if not "%GFCPP%"=="" goto startexamples

echo GFCPP is not set.
goto finished


:startexamples

echo.
echo Running GemFire C++ ExecuteFunctions example

set PATH=%PATH%;%GFCPP%\bin;..\bin;


call ExecuteFunctions.exe %1

