#!/bin/bash

getPath()
{
  if uname | grep '^CYGWIN' >/dev/null 2>/dev/null; then
    cygpath "$1"
  else
    echo "$1"
  fi
}

scriptLocation="`getPath "$0"`"

scriptDir="`dirname "${scriptLocation}"`"
scriptName="`basename "${scriptLocation}"`"

if [ -r "${scriptDir}/runCSFunctions.sh" ]; then
  source "${scriptDir}/runCSFunctions.sh"
else
  echo "Could not read the runCSFunctions.sh script."
  exit 1
fi

if [ -n "${NUNIT}" ]; then
  PATH="${PATH}:`getPath "${NUNIT}/bin"`"
fi

NUNIT_CONSOLE="nunit-console.exe"

  NUNIT_FLAGS="/labels /framework=net-4.0"

UNITTESTS_DLL="UnitTests.dll"
RESULT_XML="TestResult.xml"


getResultsDir()
{
  echo "${TESTRES}/csharp/$1"
}

getNunitFlag()
{
  if [ "$1" = "unicast-deprecated" ]; then
    echo /exclude=multicast-only
	echo /exclude=generics
  elif [ "$1" = "multicast" ]; then
    echo /exclude=unicast_only
  elif [ "$1" = "unicast" ]; then
    echo /exclude=deprecated
  else
    echo
  fi
}

getFailures()
{
  sed -n 's/^[ \t]*<test-case[ \t]\+name[ \t]*=[ \t]*"\([^\.]*\.\)*\([^\.]*\.[^\."]*\)"[^>]*success[ \t]*=[ \t]*"False"[^>]*>.*$/\2/p' "$1" 2>/dev/null
}

runTests()
{
  echo "The logging level is ${DUNIT_LOGLEVEL}."
  echo "The server logging level is ${GFE_LOGLEVEL}."
  echo "The server security logging level is ${GFE_SECLOGLEVEL}."
  echo
  ORIGPWD="`pwd`"
  RESULT_TYPE="$1"
  shift
  RESULTS_DIR="`getResultsDir "${RESULT_TYPE}"`"
  NUNIT_RUN_FLAG="`getNunitFlag "${RESULT_TYPE}"`"
  echo "NUnit flag is ${NUNIT_RUN_FLAG}"

  cd "${TESTSRC}"
  rm -rf "${RESULTS_DIR}"
  mkdir -p "${RESULTS_DIR}"
  cp -f *.dll *.pdb *.xml *.dtd *.exe *.pl *.properties* "${RESULTS_DIR}" 2>/dev/null
  cp -f --preserve=all cache6_5.dtd "${RESULTS_DIR}/.." 2>/dev/null
  cp -f --preserve=all cache6_6.dtd "${RESULTS_DIR}/.." 2>/dev/null
  if [ "${runNet20}" = "true" ]; then
    echo Copying for net20 run.
    if [ -n "${OSBUILDDIR}" ]; then
      OSBUILDDIR="`getPath "${OSBUILDDIR}"`"      
    fi  
	if [ -n "${OSBUILDEXTDIR}" ]; then
      OSBUILDEXTDIR="`getPath "${OSBUILDEXTDIR}"`"      
    fi    
	cp -f --preserve=all ${OSBUILDEXTDIR}/product/bin/Net20/GemStone.GemFire.Cache.dll "${OSBUILDDIR}/tests/clicache/UnitTests/" 2>/dev/null
	cp -f --preserve=all ${OSBUILDEXTDIR}/product/bin/Net20/GemStone.GemFire.Cache.xml "${OSBUILDDIR}/tests/clicache/UnitTests/" 2>/dev/null	
  fi  
  chmod a+rwx -R "${RESULTS_DIR}"

  cd "${RESULTS_DIR}"
  if [ -n "${DUNIT_LOGDIR}" ]; then
    mkdir -p "${DUNIT_LOGDIR}"
  fi

  coveragePrefix1=""
  coveragePrefix2=""
  coveragePrefix3=""
  coveragePrefix4=""
  coverageFile="coverage-master"
  if [ "${COVERAGE_ENABLED}" = "true" ]; then
    coveragePrefix1="NCover.Console.exe //x ${coverageFile}-1.xml"
    coveragePrefix2="NCover.Console.exe //x ${coverageFile}-2.xml"
    coveragePrefix3="NCover.Console.exe //x ${coverageFile}-3.xml"
    coveragePrefix4="NCover.Console.exe //x ${coverageFile}-4.xml"
  fi


  if [ "${RUNSINGLETEST}" = "true" ]; then
      echo "Running single test"
      ${coveragePrefix1} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} ${NUNIT_RUN_FLAG} "/xml=TestResult.xml"  "$@"  "${UNITTESTS_DLL}"  
      if [ "${COVERAGE_ENABLED}" = "true" ]; then
        echo NCover.Reporting.exe ${coverageFile}-1.xml merged-coverage.xml //s merged-coverage-tmp.xml
        NCover.Reporting.exe ${coverageFile}-1.xml merged-coverage.xml //s merged-coverage-tmp.xml
        mv -f merged-coverage-tmp.xml merged-coverage.xml
      fi
  else
      echo "Tests mode is Sequential ${RUNSEQUENTIAL}"
      if [ "${RUNSEQUENTIAL}" = "true" ]; then
        ${coveragePrefix1} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} ${NUNIT_RUN_FLAG} "/xml=TestResult.xml"  "$@"  "${UNITTESTS_DLL}"   
        if [ "${COVERAGE_ENABLED}" = "true" ]; then
          echo NCover.Reporting.exe ${coverageFile}-1.xml merged-coverage.xml //s merged-coverage-tmp.xml
          NCover.Reporting.exe ${coverageFile}-1.xml merged-coverage.xml //s merged-coverage-tmp.xml
          mv -f merged-coverage-tmp.xml merged-coverage.xml
        fi
      else

        ${coveragePrefix1} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} "/xml=TestResult1.xml" "/include=group1" "$@"  "${UNITTESTS_DLL}" &

        sleep 1

        ${coveragePrefix2} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} "/xml=TestResult2.xml" "/include=group2" "$@"  "${UNITTESTS_DLL}" &

        sleep 2

        ${coveragePrefix3} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} "/xml=TestResult3.xml" "/include=group3" "$@"  "${UNITTESTS_DLL}" &

        sleep 3

        ${coveragePrefix4} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} "/xml=TestResult4.xml" "/include=group4" "$@"  "${UNITTESTS_DLL}" &

        wait

        cat "TestResult1.xml" "TestResult2.xml" "TestResult3.xml" "TestResult4.xml" > "TestResult.xml"

        if [ "${COVERAGE_ENABLED}" = "true" ]; then
	  echo NCover.Reporting.exe ${coverageFile}-1.xml ${coverageFile}-2.xml ${coverageFile}-3.xml ${coverageFile}-4.xml merged-coverage.xml //s merged-coverage-tmp.xml
	  NCover.Reporting.exe ${coverageFile}-1.xml ${coverageFile}-2.xml ${coverageFile}-3.xml ${coverageFile}-4.xml merged-coverage.xml //s merged-coverage-tmp.xml
          mv -f merged-coverage-tmp.xml merged-coverage.xml
        fi
      fi
  fi

  RESULT=success
  if [ -n "${DUNIT_LOGDIR}" ]; then
    mkdir -p failures
    testFailures="`getFailures "${RESULT_XML}"`"
    if [ -n "${testFailures}" ]; then
      RESULT=fail
      for failure in ${testFailures}; do
        echo Failure in ${failure}.
        if [ -f "${DUNIT_LOGDIR}/${failure}.log" ]; then
          cp -f "${DUNIT_LOGDIR}/${failure}.log" failures
          rm -f "${DUNIT_LOGDIR}/${failure}.log"
        fi
      done
    fi
  fi

  cd "${ORIGPWD}"
}

resultPrompt()
{
  RESULT_TYPE="$1"
  RESULTS_DIR="`getResultsDir "${RESULT_TYPE}"`"
  if [ "${RESULT}" = "success" ]; then
    echo
    echo
    echo "SUCCESS: All ${RESULT_TYPE} tests passed."
  else
    echo
    echo
    echo -n "FAILURE: Some ${RESULT_TYPE} tests failed"
    if [ -d "${RESULTS_DIR}/failures" ]; then
      echo "; see ${RESULTS_DIR}/failures"
    else
      echo
    fi
  fi
  if [ -f "${RESULTS_DIR}/TestResult.xml" ]; then
    echo "Test report is in: ${RESULTS_DIR}/TestResult.xml"
  fi
  if [ -n "${DUNIT_LOGDIR}" -a -d "${RESULTS_DIR}/${DUNIT_LOGDIR}" ]; then
    echo "Output is in: ${RESULTS_DIR}/${DUNIT_LOGDIR}"
  fi
}

getArgValue()
{
  echo "$1" | sed 's/^-[^=]*=//'
}

cleanUp()
{
  cd "${INITPWD}"
  rm -f "${scriptDir}/gfcpp.properties"
  taskkill /FI "USERNAME eq ${USER}" /F /IM ${NUNIT_CONSOLE} >/dev/null 2>/dev/null
  taskkill /FI "USERNAME eq ${USER}" /IM FwkClient.exe >/dev/null 2>/dev/null
  sleep 1
  taskkill /FI "USERNAME eq ${USER}" /F /IM FwkClient.exe >/dev/null 2>/dev/null
  if [ "$1" = "true" ]; then
    sleep 1
    rm -rf "${TESTRES}/csharp"
  fi
}


isFlag="true"
logging="true"
useDebug="false"
runMulticast="true"
runUnicast="true"
runDeprecated="true"
runGenerics="true"
DUNIT_LOGLEVEL="config"
GFE_LOGLEVEL="config"
GFE_SECLOGLEVEL="config"
COVERAGE_ENABLED="false"
runNet20="false"

while [ "${isFlag}" = "true" ]; do
  case $1 in
    -logging=*)
      logging="`getArgValue "$1"`";
      shift;
    ;;
    -logLevel=*)
      DUNIT_LOGLEVEL="`getArgValue "$1"`";
      shift;
    ;;
    -gfeLogLevel=*)
      GFE_LOGLEVEL="`getArgValue "$1"`";
      shift;
    ;;
    -gfeSecLogLevel=*)
      GFE_SECLOGLEVEL="`getArgValue "$1"`";
      shift;
    ;;
    -coverage)
      COVERAGE_ENABLED="true";
      shift;
    ;;
    -debug=*)
      useDebug="`getArgValue "$1"`";
      shift;
    ;;
    -multicast=*)
      runMulticast="`getArgValue "$1"`";
      shift;
    ;;
    -unicast=*)
      runUnicast="`getArgValue "$1"`";
      shift;
    ;;
    -deprecated=*)
      runDeprecated="`getArgValue "$1"`";
      shift;
    ;;
    -generics=*)
      runGenerics="`getArgValue "$1"`";
      shift;
    ;;
    -clean)
      echo Cleaning up ...
      cleanUp true;
      exit 0;
    ;;	
    -net20=*)	
      runNet20="`getArgValue "$1"`";	  
      shift;
	  ;;
    *)
      isFlag="false";
    ;;
  esac
done

export DUNIT_LOGDIR="output"
if [ -n "${OSBUILDDIR}" ]; then
  OSBUILDDIR="`getPath "${OSBUILDDIR}"`"
  if [ "${useDebug}" = "true" ]; then
    if [ "${runNet20}" = "true" ]; then
	  PATH="${OSBUILDEXTDIR}/hidden/lib/debug:${OSBUILDEXTDIR}/hidden/lib:${OSBUILDEXTDIR}/hidden/gpl:${OSBUILDEXTDIR}/product/bin:`getPath "${OPENSSL}/bin"`:${PATH}"
	else
      PATH="${OSBUILDDIR}/hidden/lib/debug:${OSBUILDDIR}/hidden/lib:${OSBUILDDIR}/hidden/gpl:${OSBUILDDIR}/product/bin:`getPath "${OPENSSL}/bin"`:${PATH}"
	fi
  else
    if [ "${runNet20}" = "true" ]; then
      PATH="${OSBUILDEXTDIR}/product/bin:${OSBUILDEXTDIR}/hidden/lib:${OSBUILDEXTDIR}/hidden/gpl:`getPath "${OPENSSL}/bin"`:${PATH}"
	else
	  PATH="${OSBUILDDIR}/product/bin:${OSBUILDDIR}/hidden/lib:${OSBUILDDIR}/hidden/gpl:`getPath "${OPENSSL}/bin"`:${PATH}"
    fi
  fi
  TESTRES="${OSBUILDDIR}/tests/results"
else
  if [ "${useDebug}" = "true" ]; then
    TESTRES="../../../../tests/results"
  else
    TESTRES="../../../tests/results"
  fi
fi
if [ "${logging}" = "false" ]; then
  unset DUNIT_LOGDIR
fi
if [ "${useDebug}" = "true" ]; then
  TESTSRC="${TESTSRC:-../../../../../../../tests/clicache/UnitTests}"
else
  TESTSRC="${TESTSRC:-../../../../../../tests/clicache/UnitTests}"
fi
export TESTSRC DUNIT_LOGLEVEL GFE_LOGLEVEL GFE_SECLOGLEVEL COVERAGE_ENABLED PATH

mkdir -p "${TESTRES}"
mkdir -p "${TESTRES}/csharp"

INITPWD="`pwd`"

trap "cleanUp false" 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15


if [ "${runUnicast}" = "true" ]; then

  # Next the unicast tests

  if [ "${runDeprecated}" = "true" ] || [ "${runGenerics}" = "false" ]; then
    RESULT_TYPE="unicast-deprecated"
  fi
  if [ "${runDeprecated}" = "false" ] || [ "${runGenerics}" = "true" ]; then
    RESULT_TYPE="unicast"
  fi
  echo Running the ${RESULT_TYPE} tests.
  echo
  DATE_STR="`date`"
  MCAST_PORT="`getOpenPort`"
  STACKTRACE="# No stacktrace"
  if [ "${useDebug}" = "true" ]; then
    STACKTRACE="stacktrace-enabled=true"
  fi
  rm -f "${scriptDir}/gfcpp.properties"
  cat > "${scriptDir}/gfcpp.properties" << EOF
# ${DATE_STR}
stacktrace-enabled=true
log-level=${DUNIT_LOGLEVEL}
EOF

  if [ -f "${scriptDir}/gfcpp.properties.mine" ]; then
    cat "${scriptDir}/gfcpp.properties.mine" >> "${scriptDir}/gfcpp.properties"
  fi
  runTests "${RESULT_TYPE}" "$@"
  resultPrompt "${RESULT_TYPE}"
  if [ "${RESULT}" = "fail" ]; then
    exit 1
  fi

fi


if [ "${runMulticast}" = "true" ]; then

  # First run the multicast tests

  RESULT_TYPE="multicast"
  echo Running the ${RESULT_TYPE} tests.
  echo
  DATE_STR="`date`"
  MCAST_PORT="`getOpenPort`"
  STACKTRACE="# No stacktrace"
  if [ "${useDebug}" = "true" ]; then
    STACKTRACE="stacktrace-enabled=true"
  fi
  rm -f "${scriptDir}/gfcpp.properties"
  cat > "${scriptDir}/gfcpp.properties" << EOF
# ${DATE_STR}
${STACKTRACE}
log-level=${DUNIT_LOGLEVEL}
EOF
  if [ -f "${scriptDir}/gfcpp.properties.mine" ]; then
    cat "${scriptDir}/gfcpp.properties.mine" >> "${scriptDir}/gfcpp.properties"
  fi
  runTests "${RESULT_TYPE}" "$@"
  resultPrompt "${RESULT_TYPE}"
  if [ "${RESULT}" = "fail" ]; then
    exit 1
  fi

fi
