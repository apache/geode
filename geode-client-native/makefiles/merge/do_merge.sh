#! /bin/csh

source mergesetup.csh

$mergeScript >& $mergeScript.log
chmod -x $mergeScript

egrep -e ': conflicts$' $mergeScript.log > $mergeScript.conflicts

cat $mergeScript.conflicts

