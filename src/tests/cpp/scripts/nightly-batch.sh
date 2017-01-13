#!/usr/bin/bash
echo "This script has not been updated!"
exit

function buildResult {
if [ $# -eq 0 ]
then
  BUILDRESULT=""
  return
fi

if [ -z "$BUILDRESULT" ]
then
  sep=""
else
  sep=","
fi

case $1 in
  0)
    ;;
  1) BUILDRESULT="${BUILDRESULT}$sep $1 ${2}"
    ;;
  *) BUILDRESULT="${BUILDRESULT}$sep $1 ${2}s"
    ;;
esac
}

function timeBreakDown {
  ((tot=$2-$1))
  ((day=$tot/86400))
  ((dayR=$tot%86400))
  ((hr=$dayR/3600))
  ((hrR=$dayR%3600))
  ((min=$hrR/60))
  ((sec=$hrR%60))

  buildResult
  buildResult $day day
  buildResult $hr hour
  buildResult $min minute
  buildResult $sec second
  TIMEBREAKDOWN=$BUILDRESULT
}

# Set CHECKOUT to the checkout toplevel directory
JNK=`/usr/bin/dirname $0`
OLDPWD=$PWD
# go back three dirs.
cd `/usr/bin/dirname $JNK`/..
export CHECKOUT=$PWD
cd $OLDPWD

## cmd <framework-dir> <result-dir> <batch-list> <host> ...

FRAMEWORK=${1:-baddir}
shift

if [ ! -e "$FRAMEWORK/scripts/runDriver" ]; then
  echo "First argument must be path to built framework directory"
  exit 1
fi

RESULTDIR=${1:-baddir}
shift

BATCHLIST=${1:-baddir}
shift

if [ ! -f "$BATCHLIST" ]; then
  echo "Third argument must be path to a batch list, containing the xml names to run."
  exit 1
fi

if [ ! -e "$RESULTDIR" ]; then
  echo "Creating result directory: $RESULTDIR"
  mkdir -p $RESULTDIR
fi

XMLFILES=`cat $BATCHLIST`

cd $RESULTDIR

REPORT="$PWD/error.report"

rm -f $REPORT

echo "Test results are in " `pwd`
echo ""
echo "C++ Regression Error Report" >$REPORT
echo "---------------------------" >>$REPORT

dayOfWeek=`date +'%u'`
((calcDay=($dayOfWeek/2)*2))
if [ $dayOfWeek -eq $calcDay ]
then
  echo ""
  echo "Will use static libraries for test run."
  echo ""
  staticArg="-s"
else
  staticArg=""
fi

xcnt=0
for xml in $XMLFILES; do
  # set marker file to find directory that is newer than it.
  # run the xml
  echo "" >>$REPORT
  echo "" >>$REPORT
  echo "" >>$REPORT
  echo "#########################################################" >>$REPORT
  echo "## $xml" >>$REPORT
  echo "#########################################################" >>$REPORT
  echo "Starting test:  $xml"
  start=`date +'%s'`
  ((xcnt=$xcnt+1))
  export LOGPREFIX=`basename $xml .xml`_
  rundir=${LOGPREFIX}run_0
  $FRAMEWORK/scripts/runDriver $xml 1 $staticArg $* > /dev/null
  if [ -d $rundir ]
  then
    cd $rundir
    ./stopAll
    grep SUMMARY Driver.log >>$REPORT
    perl $CHECKOUT/release/build/grepLogs.pl $PWD >>$REPORT 2>&1
    cd -
  else
    echo "Failed to find $rundir." >>$REPORT
  fi
  end=`date +'%s'`
  timeBreakDown $start $end
  echo "    Test $xml used: $TIMEBREAKDOWN"
  echo ""
done

echo "" >>$REPORT
echo "" >>$REPORT
echo "" >>$REPORT
echo "#########################################################" >>$REPORT
echo "## Completed. Processed $xcnt xml files." >>$REPORT
echo "#########################################################" >>$REPORT

MAILTO="tim.keith@gemstone.com sudhir.menon@gemstone.com matthew.splett@gemstone.com qing.he@gemstone.com jdai@gemstone.com"
mail -s "Nightly regression test run." $MAILTO < $REPORT
