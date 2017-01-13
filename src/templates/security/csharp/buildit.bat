@echo off

rem Execute vcvars32.bat before executing this batch file,
rem or start the Visual Studio command prompt.
rem GFCPP must be set to the product installation folder

csc /out:GemStone.GemFire.Templates.Cache.Security.dll /target:library /optimize /reference:%GFCPP%\bin\GemStone.GemFire.Cache.dll *.cs

copy GemStone.GemFire.Templates.Cache.Security.dll %GFCPP%\SampleCode\quickstart\csharp

 
