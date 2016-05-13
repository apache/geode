#!/bin/bash
echo "This script has not been updated!"
exit

function processArgs {
  if [ $# -eq 0 ]; then 
    usage
  fi

  while getopts ":B:p:m:o:" op
  do
	
    unset mailtmp

    case $op in
      ( "o" ) if ( echo $OPTARG | grep '^-' > /dev/null ); then
                echo "ERROR: -o option must have a file argument that does not start with \"-\""
                errorDetected=1
              fi
      ;;
      ( "B" ) BUILDHOST=$OPTARG ;;
      ( "p" ) BUILDDIR=$OPTARG ;;
      ( "m" ) mailtmp=$OPTARG ;;
      ( * ) echo "Unknown argument provided: -$OPTARG, ignoring." ; echo "" ;;
    esac 

    # Build up a list of mail recipients 
    if [ ! -z "$mailtmp" ]; then
      MAILTO="$MAILTO $mailtmp"
    fi

  done

  # Set defaults
  BUILDHOST=${BUILDHOST:-"localhost"}

  # Check for remaining required settings
  if [ -z "$BUILDDIR" ]; then
	echo "OPTIONS error: must specify a path to the build directory with -p"
	errorDetected=1	
  fi

  MAILTO=${MAILTO:-"NO_MAIL"}
}

function usage {
  cat<<__USAGE__
typical usages:
  tests/scripts/nightly-batch.sh -B <machine> -p <source path> -m <email>
------------------------------
  -m <mail recipient> 
     @@ name of mail recipient, can be used multiple times
  -p <build directory>
     @@ This is a path to the top directory of your source checkout
  -B <machine to build on>
     @@ defaults to localhost
  -o <file to take commandline options from> 
     @@ lines where the very first character is '#' are treated as comments
__USAGE__
  exit 1
}

function showsettings {
  NUMBER_OF_XML=`echo "$XMLFILES" | wc -w | tr -d ' '` 
  echo "$XMLFILES"
cat<<__SETTINGS__
  BUILDHOST     => $BUILDHOST
  BUILDDIR      => $BUILDDIR
  MAILTO        => $MAILTO
__SETTINGS__
}

function makeBuild {
  echo "Updating checkout and beginning clean build at " `date`
  ssh -x -q $BUILDHOST "cd $BUILDDIR && git pull && ./build.sh clean product tests ; if [ $? -eq 0 ]; then echo 'Build successful.'; else echo 'Build failed.'; fi" > build${BUILDHOST}.log 2>&1
  ssh -x -q $BUILDHOST "cd $BUILDDIR && cat build-artifacts/lastUpdate.txt"
  echo "Build complete at " `date`
  echo ""
}


###--------------------- Main ----------------------------###

# Clear settings that may have carried over from the environment
unset BUILDHOST
unset MAILTO

# Detect the presence of an options file and add its contents to the 
# begining of the argument list. This way other command line 
# arguments take precendence

optionsFile=`echo "$*" | sed 's/.*-o \([^-][^ ]*\).*/\1/'`
if ( echo "$*" | grep -- "-o" > /dev/null ); then
  if [ -f "$optionsFile" ]; then
    additionArgs=`grep -v '^#' $optionsFile | tr '\n' ' '`  
  fi
fi

processArgs $additionalArgs $@
if [ "x$errorDetected" = "x1" ]; then
  echo "Exiting due to previous options error:"
  usage
fi

makeBuild

if [ "x$MAILTO" != "xNO_MAIL" ]; then
  # This is not perfect, but it works. NOTE: You canoot use the -n flag
  # if you do it will ignore the cat stream and you get an empty email.
  cat build${BUILDHOST}.log | ssh -x -q zeus "/usr/ucb/mail -s 'Nightly regression test run.' $MAILTO"
else
  echo "INFO: Skipping mail step because no email address was specified."
fi
