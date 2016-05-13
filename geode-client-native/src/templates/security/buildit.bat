@echo off

rem execute vcvars32.bat before executing this batch file.
rem GFCPP must be set
rem OPENSSL if not set, PKCS implementation will not work.

set PKCSimpl=PkcsAuthInit.cpp
set opensslLib=%OPENSSL%\lib\libeay32.lib

IF NOT EXIST %opensslLib% (
  rem The library pointed to here should match your
  rem /MD or /MDd argument used below
  set opensslLib=%OPENSSL%\lib\VC\libeay32MD.lib
)

rem users do not require to set the VCVER and PLATFORM when running from a visual studio console 
IF /I "%VCVER:~-2%" == "10" (
	set kernel32="%MSSDK%\Windows\v7.0A\Lib\kernel32.lib"   
	IF /I "%PLATFORM%" == "x64" (
		set kernel32="%MSSDK%\Windows\v7.0A\Lib\x64\kernel32.lib"
	)
) ELSE (
	set kernel32=""  
)
IF /I "%OPENSSL%" == "" (
  echo warning: compiling without PkcsAuthInit... PKCS implementation will not work.
  set PKCSimpl=
  set opensslLib=
)


IF /I NOT "%OPENSSL%" == "" (
  echo using OPENSSL from %OPENSSL%.
)

echo INCLUDE %INCLUDE%
cl /Ox /MD /Zc:wchar_t /EHsc /Gd /TP /GR /wd4996 /D_EXAMPLE /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NON_CONFORMING_SWPRINTFS /DBUILD_TESTOBJECT /D_WIN32 /DWINVER=0x0500 /D_WINDLL /I%GFCPP%\include /I%OPENSSL%\include /Fe%GFCPP%\bin\securityImpl.dll /LD UserPasswordAuthInit.cpp %PKCSimpl% /link /DLL /MANIFEST /MANIFESTFILE:%GFCPP%\bin\securityImpl.dll.intermediate.manifest %opensslLib% %kernel32% %GFCPP%\lib\gfcppcache.lib

IF NOT %ERRORLEVEL% == 0 EXIT %ERRORLEVEL%

mt -manifest %GFCPP%\bin\securityImpl.dll.intermediate.manifest /outputresource:%GFCPP%\bin\securityImpl.dll;#2

IF NOT %ERRORLEVEL% == 0 EXIT %ERRORLEVEL%

IF EXIST "*.obj" del "*.obj"
IF EXIST "%GFCPP%\bin\security*.*.manifest" (
  del "%GFCPP%\bin\security*.*.manifest" "%GFCPP%\bin\security*.exp" "%GFCPP%\bin\security*.lib"
)
