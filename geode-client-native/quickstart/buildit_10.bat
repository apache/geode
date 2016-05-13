@echo off

rem Builds both C++ and C# quickstart examples
rem Run from Visual Studio 200X Command Prompt to find MSBuild and VCBuild
rem GFCPP must be set
rem GEMFIRE must be set

if not "%GFCPP%"=="" goto checkGEMFIRE

echo GFCPP is not set.
goto finished

:checkGEMFIRE

if not "%GEMFIRE%"=="" goto startbuild

echo GEMFIRE is not set.
goto finished

:startbuild

echo Generating Pdx Auto files ...


%GFCPP%\bin\pdxautoserializer.exe  --outDir=cpp\queryobjects  cpp\queryobjects\PortfolioPdxAuto.hpp 
%GFCPP%\bin\pdxautoserializer.exe  --outDir=cpp\queryobjects  cpp\queryobjects\PositionPdxAuto.hpp


echo Building GemFire C++ and C# QuickStart examples...
set is64bit=__IS_64_BIT__
echo 64 bit set %is64bit%
if not %is64bit%==1 goto else
msbuild.exe quickstart_csharp.sln /p:Platform="x64" /p:Configuration=Release
if not "%ERRORLEVEL%" == "0" exit /B 1
msbuild.exe quickstart_cpp.sln /p:Platform="x64" /p:Configuration=Release  /nologo
if not "%ERRORLEVEL%" == "0" exit /B 1
javac.exe -classpath "%GEMFIRE%\lib\gemfire.jar" interop/InteropJAVA.java
if not "%ERRORLEVEL%" == "0" exit /B 1
goto finished 
:else
msbuild.exe quickstart_csharp_10.sln /p:Platform="Mixed Platforms" /p:Configuration=Release
if not "%ERRORLEVEL%" == "0" exit /B 1
msbuild.exe quickstart_cpp_10.sln /p:Platform="Win32" /p:Configuration=Release /nologo
if not "%ERRORLEVEL%" == "0" exit /B 1
javac.exe -classpath "%GEMFIRE%\lib\gemfire.jar" interop/InteropJAVA.java

:finished
