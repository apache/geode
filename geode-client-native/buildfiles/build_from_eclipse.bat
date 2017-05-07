@setlocal enableextensions
@set scriptdir=%~dp0
@set bd=%scriptdir:\buildfiles\=%

rem set THIRDPARTY in your eclipse project environment.
rem set THIRDPARTY=C:/gemstone/altcplusplus

c:/cygwin/bin/bash --login -c "cd '%bd%'; ./build.sh -emacs %* | ./buildfiles/winlog.sh"


