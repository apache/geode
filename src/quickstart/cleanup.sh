#!/bin/bash

echo Deleting Geode Statistics and Log files...

rm -f *.gfs
rm -f gfecs/*
rmdir gfecs
if [ -d gfecs2 ]
then
  rm -f gfecs2/*
  rmdir gfecs2
fi