@echo off

echo "GFCPP=%GFCPP%"

cl /MD /Zc:wchar_t /GR /EHsc /wd4996 /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DWINVER=0x0500 /I%GFCPP%\include /Feuserobjects.exe *.cpp %GFCPP%\lib\gfcppcache.lib

del *.obj
