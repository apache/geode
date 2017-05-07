#!/bin/bash

# This script processes a set of archives to produce one inclusive archive.

# options to 'ar' exe.
AR_ARGS=$1
shift

# name of ar (.a) to build
AR_TARGET=$1
shift

OBJECTS=./.composit-aros.list
ARDIRS=./.composit-ardirs.list

TMPDIR=./.tmp

function isDotA
{
  bname=`basename $1`
  aname=`basename $1 .a`
  if [ "$bname" == "$aname" ]
  then
    false
  else
    true
  fi
}

function isDotO
{
  bname=`basename $1`
  oname=`basename $1 .o`
  if [ "$bname" == "$oname" ]
  then
    false
  else
    true
  fi
}

function handleArchive
{
  tmp="${TMPDIR}_`basename $1`"
  mkdir -p $tmp
  echo $tmp >>$ARDIRS
  cd $tmp
  ar x $1
  cd -
  find $tmp -name "*.o" -print >>$OBJECTS
}

function handleObject
{
  echo "$1" >>$OBJECTS
}

echo "" >$OBJECTS

while [ ! -z ${1:-} ]
do
  file=$1
  shift
  isDotA $file && handleArchive $file
  isDotO $file && handleObject $file
done

if [ -f "$AR_TARGET" ]; then
  rm $AR_TARGET
fi

ar $AR_ARGS $AR_TARGET `cat $OBJECTS`

# cleanup
rm $OBJECTS
if [ -f "$ARDIRS" ]; then
  for i in `cat $ARDIRS`; do
    rm -r $i
  done
  rm $ARDIRS
fi
