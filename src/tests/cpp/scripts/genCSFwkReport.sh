#!/bin/sh

[ "$#" = "0" ] && { echo "Please give the log directory path as an argument"; exit 1; }

DIRNAME="`dirname "$0"`"
LOGDIR="$1"
shift
if [ ! -d "${LOGDIR}" ]; then
  echo "Error:: LogDir ${LOGDIR} does not exit."
  exit 1
fi

LOGPOSTFIX="$1"
shift
POOLOPTION=
if [ $1 != "0" ]; then
  POOLOPTION="$1"
fi
shift
is64bit=__IS_64_BIT__
if [ $is64bit -eq 1 ]; then
  ARCH="64"
else
  ARCH="32"
fi

runLatestProp() {
  ## executing LatestProp.java for fatching latest java properties
  #LCP_PATH="$fwkbuild/framework/lib/javaobject.jar:$GFE_DIR/lib/gemfire.jar"
  gcmdir=`echo $GF_JAVA | sed 's/where.*/where/'` # gcmdir = /export/gcm/where or /gcm/where
  LCP_PATH="${GFBASE}/framework/lib/javaobject.jar"
  if type -p cygpath >/dev/null 2>/dev/null; then
    LCP_PATH="`cygpath -p -m "$LCP_PATH"`"
    LCP_PATH="${GFBASE}/framework/lib/javaobject.jar;$GFE_DIR/lib/gemfire.jar;${GFBASE}/framework/lib/testdb.jar;$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  else
   LCP_PATH="$LCP_PATH:$GFE_DIR/lib/gemfire.jar:${GFBASE}/framework/lib/testdb.jar:$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  fi
  #echo $GF_JAVA -cp "$LCP_PATH" javaobject.LatestProp >> $TESTDIR/latest.prop
  ${GF_JAVA} -cp "${LCP_PATH}" javaobject.LatestProp >> $TESTDIR/latest.prop
}

LOGPREFIX=`date +'%y-%m-%d_%H-%M-%S'`
RUN_REPORT="${LOGDIR}/error.report"
FullCsvReport="${LOGDIR}/regression-$LOGPOSTFIX.csv"
rm -f "${RUN_REPORT}"
TESTDIRS="$@"
for tDir in ${TESTDIRS}; do
  TESTDIR="${LOGDIR}/${tDir}"
  if [ -d "${TESTDIR}" ]; then
    REPORT="${TESTDIR}/error.report"
    CSVREPORT="${TESTDIR}/regression.csv"
    TESTNAME="`basename "${TESTDIR}"`"
    if [ -f "${GF_JAVA}" ]; then
      runLatestProp
    else
      echo "GF_JAVA.WIN variable is not set" 
    fi
    CP_PATH="$GFE_DIR/lib/gemfire.jar;$GFE_DIR/../tests/classes"
    TEMPDIRPATH="${TESTDIR}"
    if type -p cygpath >/dev/null 2>/dev/null; then
      CP_PATH="`cygpath -m "$CP_PATH"`"
      TEMPDIRPATH="`cygpath -m "$TEMPDIRPATH"`"
    fi
    echo "===================================================" > "${REPORT}"
    echo "Test results are in ${TESTDIR}" >> "${REPORT}"
    echo "Ran test: ${TESTNAME}" >> "${REPORT}"
    echo "Error Summary"  >> "${REPORT}"
    echo "===================================================" >> "${REPORT}"
    grep "^\\[TEST:${TESTNAME}:.*TEST SUMMARY" "${TESTDIR}/Driver.log" | sed 's/\r$//' >> "${REPORT}" || echo "No SUMMARY lines found." >> "${REPORT}"
    echo "===================================================" >> "${REPORT}"
    echo "Suspect strings in Driver.log" >> "${REPORT}"
    grep "^\\[TEST:${TESTNAME}:[eE]rror" "${TESTDIR}/Driver.log" | sed 's/\r$//' >> "${REPORT}"
    echo "===================================================" >> "${REPORT}"
    perl ${DIRNAME}/grepLogs.pl "${TESTDIR}" | sed 's/\r$//' >> "${REPORT}" 2>&1
    echo "===================================================" >> "${REPORT}"
    echo "End of ${TESTNAME} test run." >> "${REPORT}"
    echo "===================================================" >> "${REPORT}"
    echo >> "${REPORT}"
    echo >> "${REPORT}"
    echo "The copy variable= $COPYON" >> "${REPORT}"
    if [ "$PERFTEST" == "true" ]
    then
      echo ${GF_JAVA} -cp "$CP_PATH" -DJTESTS=$GFE_DIR/../tests/classes -Dgemfire.home=$GFE_DIR perffmwk.PerfReporter "${TEMPDIRPATH}"
      ${GF_JAVA} -cp "$CP_PATH" -DJTESTS=$GFE_DIR/../tests/classes -Dgemfire.home=$GFE_DIR perffmwk.PerfReporter "${TEMPDIRPATH}"
    ## COPYON variable to be set only when running perf tests using build targets.
      if [ "$COPYON" == "true" ]
      then
        if [ ! -f ${TEMPDIRPATH}/../../summary.prop ]
        then
          cp -f ${TEMPDIRPATH}/latest.prop ${TEMPDIRPATH}/../../summary.prop
        fi
        cp -r ${TEMPDIRPATH} ${TEMPDIRPATH}/../../.
      fi
    fi
    ${AWK:-awk} -F "=" '/^Name/ {print ""; next} {printf "%s,", $2} ' "${TESTDIR}/latest.prop" > "${CSVREPORT}"
    TESTDATENTIME=`date +'%m/%d/%Y %T %Z'`
    ls ${TESTDIR}/failure.txt > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
     echo "$POOLOPTION,${TESTDIR},pass,CYG,$ARCH,$TESTDATENTIME" >>  "${CSVREPORT}"
    else
     echo "$POOLOPTION,${TESTDIR},fail,CYG,$ARCH,$TESTDATENTIME" >>  "${CSVREPORT}"
    fi
    cat "${REPORT}" >> "${RUN_REPORT}"
    cat "${CSVREPORT}" >> "${FullCsvReport}"
    if [ "$DBOPTION" == "true" ]; then
      echo ${GF_JAVA} -cp "$LCP_PATH" testdb.insertRegressionData ${FullCsvReport}
      ${GF_JAVA} -cp "$LCP_PATH" testdb.insertRegressionData ${FullCsvReport}
    fi
  fi
done
