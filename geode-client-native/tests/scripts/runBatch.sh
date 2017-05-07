#!/bin/bash
echo "This script has not been updated!"
exit


function processArgs {
  if [ $# -eq 0 ]; then 
    usage
  fi

  while getopts ":zp:d:h:m:l:x:o:" op
  do
	
    unset SINGLETEST
    unset TESTLIST
    unset TESTHOSTSFILE
    unset mailtmp

    case $op in
      ( "o" ) if ( echo $OPTARG | grep '^-' > /dev/null ); then
                echo "ERROR: -o option must have a file argument that does not s
tart with \"-\""
                errorDetected=1
              fi
      ;;
      ( "p" ) BUILDDIR=$OPTARG ;;
      ( "d" ) TESTSRESULTSDIR=$OPTARG ;;
      ( "h" ) TESTHOSTSFILE=$OPTARG ;;
      ( "m" ) mailtmp=$OPTARG ;;
      ( "l" ) if [ "x$resetXML" = "x1" ]; then 
                unset SINGLETESTS
                TESTLISTS=$OPTARG
                resetXML=0
              else  
                TESTLISTS="$TESTLISTS $OPTARG"
              fi
      ;;
      ( "x" ) if [ "x$resetXML" = "x1" ]; then 
                SINGLETESTS=$OPTARG
                unset TESTLISTS
                resetXML=0
              else  
                SINGLETESTS="$SINGLETESTS $OPTARG"
              fi
      ;;
      ( "z" ) resetXML=1 ;;
      ( * ) echo "Unknown argument provided: -$OPTARG, ignoring." ; echo "" ;;
    esac 

    # Build up a list of mail recipients 
    if [ ! -z "$mailtmp" ]; then
      MAILTO="$MAILTO $mailtmp"
    fi

    # Build up a list of hosts 
    if [ ! -z "$TESTHOSTSFILE" ]; then
      TESTHOSTS="$TESTHOSTS `cat $TESTHOSTSFILE | tr '\n' ' '`"
    fi
  done

  # This section of code passes all remaining arguments to the TESTHOSTS list
  while [ $OPTIND -gt 1 ]
  do
    shift
    ((OPTIND--))
  done
  if [ $# -ne 0 ]; then 
    TESTHOSTS="$TESTHOSTS $*"
  fi 

  # Check for remaining required settings
  if [ -z "$BUILDDIR" ]; then
	echo "OPTIONS error: must specify a path to the build directory with -p"
        errorDetected=1
  elif [ -z "$TESTSRESULTSDIR" ]; then
	echo "OPTIONS error: must specify a path to the test results directory with -d"
        errorDetected=1
  fi

  FRAMEWORK=$BUILDDIR/framework
  TESTHOSTS=${TESTHOSTS:-"localhost"}
  MAILTO=${MAILTO:-"NO_MAIL"}
}

function usage {
  cat<<__USAGE__
typical usages:
  tests/scripts/nightly-batch.sh -o tests/scripts/cronbeaverton.txt
  OR
  tests/scripts/nightly-batch.sh -p <source path> -d <results path> -l <test list> -m <email address> host1 ... hostn
------------------------------
  -h <file>
     @@ name of file containing hosts to use in test, optional
     @@ Any left over arguments on the command line are assumed to be hostnames
  -m <mail recipient> 
     @@ name of mail recipient, can be used multiple times
  -x <xml test file>
     @@ specify a single xml test
  -l <text file> 
     @@ file containing list of xml tests to run 
  -d <test results directory>
     @@ location where testing results are stored, mult be absolute path
  -p <build directory>
     @@ This is a path to the top directory of your source checkout
  -o <file to take commandline options from> 
     @@ lines where the very first character is '#' are treated as comments
     @@ Any -x or -l settings on the command line will override these settings
__USAGE__
  exit 1
}

function showsettings {
  NUMBER_OF_XML=`echo "$XMLFILES" | wc -w | tr -d ' '` 
  echo "$XMLFILES"
cat<<__SETTINGS__
  BUILDDIR        => $BUILDDIR
  TESTSRESULTSDIR => $TESTSRESULTSDIR
  MAILTO          => $MAILTO
  TESTHOSTS       => $TESTHOSTS
  NUMBER_OF_XML   => $NUMBER_OF_XML
__SETTINGS__
}

function buildResult {
  if [ $# -eq 0 ]; then
    BUILDRESULT=""
    return
  fi

  if [ -z "$BUILDRESULT" ]; then
    sep=""
  else
    sep=","
  fi

  case $1 in
    0) ;;
    1) BUILDRESULT="${BUILDRESULT}$sep $1 ${2}" ;;
    *) BUILDRESULT="${BUILDRESULT}$sep $1 ${2}s" ;;
  esac
}

function timeBreakDown {
  # Note: this wont work unless using GNU date because the %s format
  # isn't supported on most implimentations of date 
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

function runTests {

  if [ ! -e "$FRAMEWORK/scripts/runDriver" ]; then
    echo "Problem finding runDriver in \"$FRAMEWORK/scripts\""
    echo "Check your -p <build directory> setting"
    exit 1 
  fi
 
    
  OUTDIR=$TESTSRESULTSDIR/`date +'%y-%m-%d_%H-%M-%S_'`regression-results
  if [ ! -e "$OUTDIR" ]; then
    echo "Creating result directory: $OUTDIR"
    mkdir -p $OUTDIR	
  fi

  echo "cding into OUTDIR \"$OUTDIR\""
  cd $OUTDIR 

  REPORT="$OUTDIR/error.report"

  echo "Test results are in $OUTDIR"
  echo "" 
  echo "C++ Regression Error Report" >$REPORT
  echo "---------------------------" >>$REPORT

  # This code changes the staticArg based on if today is an 
  # even or odd numbered day of the week
  dayOfWeek=`date +'%u'`
  ((calcDay=($dayOfWeek/2)*2))
  if [ $dayOfWeek -eq $calcDay ]; then
    echo ""
    echo "Will use static libraries for test run."
    echo ""
    staticArg="-s"
  else
    staticArg=""
  fi

  rm -f $REPORT

  xcnt=0
  for xml in $XMLFILES; do
    # set marker file to find directory that is newer than it.
    # run the xml
    cat<<__REPORT_TXT__ >> $REPORT



#########################################################
### $xml
#########################################################
__REPORT_TXT__
    echo "Starting test:   $xml" 
    start=`date +'%s'`
    ((xcnt=$xcnt+1))
    export LOGPREFIX=`basename $xml .xml`_
    rundir=${LOGPREFIX}run_0
    echo "running $FRAMEWORK/scripts/runDriver $xml 1 $staticArg $TESTHOSTS"
    $FRAMEWORK/scripts/runDriver $xml 1 $staticArg $TESTHOSTS > /dev/null
    if [ -d $rundir ]; then
      cd $rundir
      ./stopAll
      grep SUMMARY Driver.log >>$REPORT
      perl $FRAMEWORK/scripts/grepLogs.pl $PWD >>$REPORT 2>&1
      cd -
    else
      echo "Failed to find $rundir." >>$REPORT
    fi
    end=`date +'%s'`
    timeBreakDown $start $end
    echo "    Test $xml used: $TIMEBREAKDOWN"
    echo ""
  done
  
  cat<<__REPORT_END__ >> $REPORT



#########################################################
## Completed. Processed $xcnt xml files.
#########################################################
__REPORT_END__
  if [ "x$MAILTO" != "xNO_MAIL" ]; then 
    # This is not perfect, but it works. NOTE: You canoot use the -n flag
    # if you do it will ignore the cat stream and you get an empty email.
    cat $REPORT | ssh zeus "/usr/ucb/mail -s 'Nightly regression test run.' $MAILTO"
  else 
    echo "INFO: Skipping mail step because no email address was specified."
  fi
 
}


###--------------------- Main ----------------------------###

# Clear settings that may have carried over from the environment
unset TESTHOSTS
unset MAILTO

# Detect the presence of an options file and add its contents to the
# begining of the argument list. This way other command line
# arguments take precendence

optionsFile=`echo "$*" | sed 's/.*-o \([^-][^ ]*\).*/\1/'`
if ( echo "$*" | grep -- "-o" > /dev/null ); then
  if [ -f "$optionsFile" ]; then
    additionalArgs=`grep -v '^#' $optionsFile | tr '\n' ' '`
  fi
fi

# The -z is a special switch to notify the parser that we have stopped 
# parsing the options file contents. This is relevant in the case of the
# options that can be concatinated like -x and -l
processArgs $additionalArgs -z $*

# Now we can build the XML list because FRAMEWORK is defined
if [ ! -z "$SINGLETESTS" ]; then
  XMLFILES="$SINGLETESTS"
fi
if [ ! -z "$TESTLISTS" ]; then
  for list in $TESTLISTS ; do
    if [ -f "$list" ]; then
      XMLFILES="$XMLFILES `cat $list | tr '\n' ' '`"
    elif [ -f "$FRAMEWORK/xml/$list" ]; then
      XMLFILES="$XMLFILES `cat $FRAMEWORK/xml/$list | tr '\n' ' '`"
    else
      echo "OPTIONS error: bad argument supplied to -l"
      echo "Could not find the test list \"$list\" in either \".\" or \"$FRAMEWORK/xml/$list\""
      errorDetected=1
    fi
  done
fi

if [ -z "$XMLFILES" ]; then
  echo "OPTIONS error: must have at least one test specified."
  echo "Use either the -x <XML file> or -l <file based list of XML files>"
  errorDetected=1
fi

if [ "x$errorDetected" = "x1" ]; then
  echo "Exiting due to previous options error:"
  usage
fi

showsettings

# In the previous version output was redirected like this, 
# I'm not sure if that is desireable though
# runTests | tee $OUTDIR/cronreg.log 2>&1
runTests
