@echo off

rem genrate autp pdx class
if not "%GFCPP%"=="" goto startbuild
echo GFCPP is not set.
goto finished

:startbuild
echo starting pdxautoserialization
for /r  %%i in (*.hpp) do (
echo %%i
%GFCPP%\bin\pdxautoserializer --outDir=. %%i
)
%GFCPP%\bin\pdxautoserializer --outDir=. --classNameStr=AutoPdxVersioned1:PdxTests.PdxVersioned AutoPdxVersioned1.hpp
%GFCPP%\bin\pdxautoserializer --outDir=. --classNameStr=AutoPdxVersioned2:PdxTests.PdxVersioned AutoPdxVersioned2.hpp

IF /I "%VCVER:~-2%" == "10" (
        set kernel32="%MSSDK%\Windows\v7.0A\Lib\kernel32.lib"
        IF /I "%PLATFORM%" == "x64" (
                set kernel32="%MSSDK%\Windows\v7.0A\Lib\x64\kernel32.lib"
        )
) ELSE (
        set kernel32=""
)

echo INCLUDE %INCLUDE%
cl /Ox /MD /Zc:wchar_t /EHsc /Gd /TP /GR /wd4996 /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DBUILD_TESTOBJECT /D_WIN32 /DWINVER=0x0500 /D_WINDLL /I%GFCPP%\include /I. /Fe%OSBUILDDIR%\framework\lib\pdxobject.dll /LD *.cpp /link /DLL /MANIFEST /MANIFESTFILE:%OSBUILDDIR%\framework\lib\pdxobject.dll.intermediate.manifest %kernel32% %GFCPP%\..\hidden\lib\gfcppcache.lib

cl /Ox /MD /Zc:wchar_t /EHsc /Gd /TP /GR /wd4996 /DDEBUG=1 /DASSERT_LEVEL=4 /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DBUILD_TESTOBJECT /D_WIN32 /DWINVER=0x0500 /D_WINDLL /I%GFCPP%\include /I. /Fe%OSBUILDDIR%\framework\lib\debug\pdxobject.dll /LD *.cpp /link /DLL /MANIFEST /MANIFESTFILE:%OSBUILDDIR%\framework\lib\debug\pdxobject.dll.intermediate.manifest %kernel32% %GFCPP%\..\hidden\lib\debug\gfcppcache.lib
IF NOT %ERRORLEVEL% == 0 EXIT %ERRORLEVEL%

mt -manifest %OSBUILDDIR%\framework\lib\pdxobject.dll.intermediate.manifest /outputresource:%OSBUILDDIR%\framework\lib\pdxobject.dll;#2

IF NOT %ERRORLEVEL% == 0 EXIT %ERRORLEVEL%

IF EXIST "*.obj" del "*.obj"
IF EXIST ":%OSBUILDDIR%\framework\lib\pdxobject*.*.manifest" (
  del "%OSBUILDDIR%\framework\lib\pdxobject*.*.manifest" "%OSBUILDDIR%\framework\lib\pdxobject*.exp" "%OSBUILDDIR%\framework\lib\pdxobject*.lib"
)
IF EXIST ":%OSBUILDDIR%\framework\lib\debug\pdxobject*.*.manifest" (
  del "%OSBUILDDIR%\framework\lib\debug\pdxobject*.*.manifest" "%OSBUILDDIR%\framework\lib\debug\pdxobject*.exp" "%OSBUILDDIR%\framework\lib\debug\pdxobject*.lib"
)



rem cl  -nologo /MD /Zc:wchar_t /GR /EHsc /wd4996 /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DWINVER=0x0500 /I%GFCPP%\include /I. /Ox /c /Fo%GFCPP%\..\tests\objects\testobject\ *.cpp


:finished

