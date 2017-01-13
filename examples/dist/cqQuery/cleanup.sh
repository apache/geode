#!/bin/bash

echo Deleting GemFire Statistics and Log files...

rm -f *.gfs
rm -f gfecs/*
rmdir gfecs
