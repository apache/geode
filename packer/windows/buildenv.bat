@rem Fixup ENV issues with SSH
set APPDATA=C:\Users\build\AppData\Roaming
set LOCALAPPDATA=C:\Users\build\AppData\Local
set TEMP=C:\Users\build\AppData\Local\Temp\3
set TMP=C:\Users\build\AppData\Local\Temp\3
if not exist %TEMP% mkdir %TEMP%

@rem Setup VC
call "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" %*

