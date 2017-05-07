@echo off

rem GFCPP must be set
rem GEMFIRE must be set
rem OPENSSL must be set for Security example

if not "%GFCPP%"=="" goto checkGEMFIRE

echo GFCPP is not set.
goto finished

:checkGEMFIRE

if not "%GEMFIRE%"=="" goto checkOPENSSL

echo GEMFIRE is not set.
goto finished

:checkOPENSSL

if not "%OPENSSL%"=="" goto startexamples

echo OPENSSL is not set.
echo If OpenSSL libraries are not found in PATH then Security example will fail.


:startexamples

mkdir ..\bin
copy /V %GFCPP%\bin\GemStone.GemFire.Cache.dll ..\bin\.

set LANG=C#
set LANGDIR=csharp

if not exist %LANGDIR%\%1.exe goto presentmenu

set exname=%1

:runexample

if not exist %LANGDIR%\%exname%.exe goto presentmenu

echo.
echo Running GemFire %LANG% QuickStart example %exname% ...

set CLASSPATH=%CLASSPATH%;../lib/javaobject.jar;%GEMFIRE%\lib\gfSecurityImpl.jar
set PATH=%GEMFIRE%\bin;%PATH%;%GEMFIRE%\bin;%GFCPP%\bin;%OPENSSL%\bin;..\bin;

if not exist gfecs mkdir gfecs

if '%exname%' neq 'Delta' goto notwithlocator

echo locator start

call gemfire start-locator -port=34756 -dir=gfecs

:notwithlocator

if '%exname%' neq 'PoolRemoteQuery' goto skiplocatorstart

echo locator start

call gemfire start-locator -port=34756 -dir=gfecs

:skiplocatorstart

if '%exname%' neq 'HACache' goto skiphadir

if not exist gfecs2 mkdir gfecs2

:skiphadir

if '%exname%' neq 'ExecuteFunctions' goto skipfuncdir

if not exist gfecs2 mkdir gfecs2

:skipfuncdir

if '%exname%' equ 'PoolRemoteQuery' goto startserverwithlocator

if '%exname%' equ 'Delta' goto startserverwithlocator

if '%exname%' equ 'Security' goto startserverforsecurity

if '%exname%' equ 'MultiuserSecurity' goto startserverformultiusersecurity

call cacheserver start cache-xml-file=../XMLs/server%exname%.xml mcast-port=35673 -dir=gfecs

echo cacheServer started

if '%exname%' neq 'HACache' goto skiphastart

call cacheserver start cache-xml-file=../XMLs/server%exname%2.xml mcast-port=35673 -dir=gfecs2

echo cacheServer2 started

:skiphastart

if '%exname%' neq 'ExecuteFunctions' goto skipfuncstart

call cacheserver start cache-xml-file=../XMLs/server%exname%2.xml mcast-port=35673 -dir=gfecs2

echo cacheServer2 started

:skipfuncstart

if '%exname%' neq 'Security' goto :skipsecuritystart 

if '%exname%' neq 'MultiuserSecurity' goto :skipsecuritystart 

:startserverforsecurity

call cacheserver start cache-xml-file=../XMLs/server%exname%.xml mcast-port=35673 -dir=gfecs security-client-authenticator=templates.security.DummyAuthenticator.create security-authz-xml-uri=../XMLs/authz-dummy.xml security-client-accessor=templates.security.XmlAuthorization.create security-client-auth-init=templates.security.UserPasswordAuthInit.create

if '%exname%' neq 'MultiuserSecurity' goto :skipsecuritystart 
:startserverformultiusersecurity
call cacheserver start cache-xml-file=../XMLs/server%exname%.xml mcast-port=35673 -dir=gfecs security-client-authenticator=templates.security.DummyAuthenticator.create security-authz-xml-uri=../XMLs/authz-dummy.xml security-client-accessor=templates.security.XmlAuthorization.create

goto :skipsecuritystart

:startserverwithlocator

call cacheserver start cache-xml-file=../XMLs/server%exname%.xml mcast-port=0 -dir=gfecs locators=localhost:34756  

:skipsecuritystart

if '%exname%' neq 'PoolRemoteQuery' goto notstartedserverwithlocator

:notstartedserverwithlocator

if '%exname%' neq 'DistributedSystem' goto skipDSstart

if not exist gfecs2 mkdir gfecs2

call cacheserver start cache-xml-file=../XMLs/server%exname%2.xml mcast-port=35674 -dir=gfecs2

:skipDSstart

call %LANGDIR%\%exname%.exe
if not "%errorlevel%"=="0" (
call cacheserver stop -dir=gfecs
call cacheserver stop -dir=gfecs2
exit %errorlevel% 
)
call cacheserver stop -dir=gfecs

if '%exname%' neq 'HACache' goto skiphastop

call cacheserver stop -dir=gfecs2

:skiphastop

if '%exname%' neq 'ExecuteFunctions' goto skipfuncstop

call cacheserver stop -dir=gfecs2

:skipfuncstop

if '%exname%' neq 'PoolRemoteQuery' goto skiplocatorstop1

call gemfire stop-locator -port=34756 -dir=gfecs

:skiplocatorstop1

if '%exname%' neq 'Delta' goto skiplocatorstop2

call gemfire stop-locator -port=34756 -dir=gfecs

:skiplocatorstop2

if '%exname%' neq 'DistributedSystem' goto skipDSstop

call cacheserver stop -dir=gfecs2

:skipDSstop

if '%exname%' equ 'invalidoption' goto invalidoption

rem echo Please review the example's log output above then

rem pause

goto closeup

:presentmenu

echo.
echo Please select a GemFire %LANG% QuickStart example to run.
echo.
echo 1. BasicOperations
echo 2. DataExpiration
echo 3. LoaderListenerWriter
echo 4. RegisterInterest
echo 5. RemoteQuery
echo 6. HA Cache
echo 7. Exceptions
echo 8. DurableClient
echo 9. Security
echo 10.PutAllGetAllOperations
echo 11.Continuous Query
echo 12.DistributedSystem
echo 13.PoolWithEndpoints
echo 14.PoolRemoteQuery
echo 15.ExecuteFunctions
echo 16.Pool Continuous Query
echo 17.Delta
echo 18.MultiuserSecurity
echo 19.RefIDExample
echo 20.Transactions
echo 21.PdxRemoteQuery 
echo 22.PdxSerializer 
echo 23.PdxInstance 
echo 24.Quit
echo.

:getoption

rem choice /c:123 /n

set /p option=Enter option: 

set exname=invalidoption

if '%option%' equ '1' set exname=BasicOperations
if '%option%' equ '2' set exname=DataExpiration
if '%option%' equ '3' set exname=LoaderListenerWriter
if '%option%' equ '4' set exname=RegisterInterest
if '%option%' equ '5' set exname=RemoteQuery
if '%option%' equ '6' set exname=HACache
if '%option%' equ '7' set exname=Exceptions
if '%option%' equ '8' set exname=DurableClient
if '%option%' equ '9' set exname=Security
if '%option%' equ '10' set exname=PutAllGetAllOperations
if '%option%' equ '11' set exname=CqQuery
if '%option%' equ '12' set exname=DistributedSystem
if '%option%' equ '13' set exname=PoolWithEndpoints
if '%option%' equ '14' set exname=PoolRemoteQuery
if '%option%' equ '15' set exname=ExecuteFunctions
if '%option%' equ '16' set exname=PoolCqQuery
if '%option%' equ '17' set exname=Delta
if '%option%' equ '18' set exname=MultiuserSecurity
if '%option%' equ '19' set exname=RefIDExample
if '%option%' equ '20' set exname=Transactions
if '%option%' equ '21' set exname=PdxRemoteQuery
if '%option%' equ '22' set exname=PdxSerializer
if '%option%' equ '23' set exname=PdxInstance
if '%option%' equ '24' goto finished

if '%exname%' equ 'invalidoption' goto invalidoption

goto runexample

:invalidoption

echo Invalid option.
goto getoption

:closeup

rmdir /Q /S ..\bin

echo Finished example %exname%.

:finished
