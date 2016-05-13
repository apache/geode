#!/bin/sh
#
# ct.sh
#
echo
echo starting CacheTest.exe instance
echo 

XML=""
NAME="Application-09"
CMD="CacheTest.exe --region=HeeHaw --burstct=4 --burstus=10000 --name="

if [ "${1}" != "" ]; then
	XML="--xml=../xml/${1}_cache.xml"		
	CMD="$CMD$NAME${1} $XML"
else
	CMD="$CMD$NAME${1}"
fi
echo
echo $CMD
echo
#cd /trunk/VS_Solutions/examples/CacheTest/Debug/
cd ../Debug/
$CMD

