@echo off

echo "GFCPP=%GFCPP%"

cl /MD /GX /GR /D_WIN32 /DWINVER=0x0500 /I%GFCPP%\include /FegfHelloWorld.exe *.cpp %GFCPP%\lib\gfcppcache.lib

del *.obj
