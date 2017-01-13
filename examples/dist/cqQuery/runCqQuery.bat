@echo off

rem GFCPP must be set

if not "%GFCPP%"=="" goto startexamples

echo GFCPP is not set.
goto finished


:startexamples

echo.
echo Running GemFire C++ CqQuery example

set PATH=%PATH%;%GFCPP%\bin;..\bin;


call CqQuery.exe %1

