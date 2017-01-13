@echo off

rem GFCPP must be set

cl /MD /Zc:wchar_t /EHsc /GR /wd4996 /D_EXAMPLE /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DWINVER=0x0500 /DBUILD_TESTOBJECT /I%GFCPP%/include /FeCqQuery.exe CqQuery.cpp Portfolio.cpp Position.cpp %GFCPP%/lib/gfcppcache.lib

cl /MD /Zc:wchar_t /EHsc /GR /wd4996 /D_EXAMPLE /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DWINVER=0x0500 /DBUILD_TESTOBJECT /I%GFCPP%/include /FeUpdater.exe Updater.cpp Portfolio.cpp Position.cpp %GFCPP%/lib/gfcppcache.lib
del *.obj
