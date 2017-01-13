@echo off

set vs=Microsoft Visual Studio 12.0

call "c:\Program Files (x86)\%vs%\VC\vcvarsall.bat" %1

echo Environment setup for %vs% %1.