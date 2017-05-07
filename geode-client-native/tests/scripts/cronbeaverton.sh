#!/usr/bin/bash

# Arguments:
# -b ( optional ) do git pull and clean build before running test
# name_of_test_list ( optional ) name of file in $BUILDDIR/tests/xml/Regression containing names of tests to run
#            defaults to name specified below, see TESTLIST
# list of hosts to use for test ( optional ) defaults to list below, see TESTHOSTS

RESULTDRIVE=tiny2
BUILDDRIVE=tiny1
BUILDHOST=tiny

TESTLIST=shortNightly.list
TESTHOSTS="hoth tiny sam gloin"

RESULTSDIR=/export/${RESULTDRIVE}/users/tkeith/regression
BUILDDIR=/export/${BUILDDRIVE}/users/tkeith/regression

doBuild=no

if [ $# -gt 0 ]
then
  doBuild=$1
fi

if [ $doBuild == "-b" ]
then
  shift
  echo "Updating checkout and beginning clean build at " `date`
  ssh -x -q $BUILDHOST "cd $BUILDDIR && git pull && ./build.sh clean build-product tests ; if [ $? -eq 0 ]; then echo 'Build successful.'; else echo 'Build failed.'; fi" > build${BUILDHOST}.log 2>&1
  ssh -x -q $BUILDHOST "cd $BUILDDIR && cat build-artifacts/lastUpdate.txt"
  echo "Build complete at " `date`
  echo ""
fi

if [ $# -gt 0 ]
then
  list=$1
  if [ -f $BUILDDIR/build-artifacts/linux/framework/xml/$list ]
  then
    TESTLIST=$list
    shift
  fi
fi

if [ $# -gt 0 ]
then
  TESTHOSTS="$*"
fi

logPrefix=`date +'%y-%m-%d_%H-%M-%S_'`
OUTDIR=$RESULTSDIR/${logPrefix}regression-results
mkdir -p $OUTDIR

$BUILDDIR/tests/scripts/nightly-batch.sh \
    $BUILDDIR/build-artifacts/linux/framework \
    $OUTDIR \
    $BUILDDIR/build-artifacts/linux/framework/xml/$TESTLIST \
    $TESTHOSTS \
    | tee $OUTDIR/cronreg.log 2>&1

