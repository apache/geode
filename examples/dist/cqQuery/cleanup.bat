@echo off

echo Deleting GemFire Statistics and Log files...

del /q *.gfs
del /q gfecs\*.*
rmdir gfecs
