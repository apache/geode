#! /bin/sh

old=$1
new=$2
target=$3
f=$4

merge $target/$f $old/$f $new/$f
status=$?

if [ $status -eq 0 ]; then
  echo "$f: success"
fi
if [ $status -eq 1 ]; then
  echo "$f: conflicts"
fi
if [ $status -eq 2 ]; then
  echo "$f: trouble"
fi
exit $status
