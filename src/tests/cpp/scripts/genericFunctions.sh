

## Create the file to use for logging.
## Args: one, the name of the log file to create.
INITLOG() {
  export LOGFILE=$1
  echo > $LOGFILE
  if [ "${LOG_MESSAGES:-none}" != "none" ]
  then
    echo -en $LOG_MESSAGES | tee -a $LOGFILE
    LOG_MESSAGES=
  fi
}

## Log a warning message
## Args: the message to log.
WARN() {
  local ts=`date +'%y-%m-%d_%H-%M-%S'`
  if [ ${LOGFILE:-none} == "none" ]
  then
  LOG_MESSAGES="${LOG_MESSAGES}${ts}  WARN: $*\n"
  fi
  echo -e "${ts}  WARN: $*" | tee -a $LOGFILE
}

## Log a warning message
## Args: file to log to, the message to log.
WARNTO() {
  local fil=$1
  shift
  local ts=`date +'%y-%m-%d_%H-%M-%S'`
  echo -e "${ts}  WARN: $*" >> $fil
}

## Log an error message
## Args: the message to log.
ERROR() {
  local ts=`date +'%y-%m-%d_%H-%M-%S'`
  if [ ${LOGFILE:-none} == "none" ]
  then
    LOG_MESSAGES="${LOG_MESSAGES}${ts} ERROR: $*\n"
  fi
  echo -e "${ts} ERROR: $*" | tee -a $LOGFILE
}

## Log a debug message
## Args: the message to log.
DEBUG() {
  if [ ${DEBUG:-0} -eq 1 ]
  then
    local ts=`date +'%y-%m-%d_%H-%M-%S'`
    if [ ${LOGFILE:-none} == "none" ]
    then
      LOG_MESSAGES="${LOG_MESSAGES}${ts} DEBUG: $*\n"
    fi
    echo -e "${ts} DEBUG: $*" | tee -a $LOGFILE
  fi
}

## Log a message
## Args: the message to log.
LOG() {
  local ts=`date +'%y-%m-%d_%H-%M-%S'`
  if [ ${LOGFILE:-none} == "none" ]
  then
    LOG_MESSAGES="${LOG_MESSAGES}${ts}  INFO: $*\n"
  fi
  echo -e "${ts}  INFO: $*" | tee -a $LOGFILE
}

## Log a message
## Args: file to log to, the message to log.
LOGTO() {
  local fil=$1
  shift
  local ts=`date +'%y-%m-%d_%H-%M-%S'`
  echo -e "${ts}  INFO: $*" >> $fil
}

## Log the content of a file
## Args: the file to log
LOGCONTENT() {
  local fil=$1
  if [ ${fil:-none} == "none" ]
  then
    WARN "No file specified in call to LOGCONTENT."
  fi
  if [ ! -f $fil ]
  then
    WARN "File specified in call to LOGCONTENT cannot be found."
  fi
  LOG "Content of $fil:"
  while read lin
  do
    LOG "  $lin"
  done < $fil
}

## Log standard input
## Args: the command whose output should be logged, if none given, stdin is read
LOGSTDIN() {
  local cmd="$*"
  if [ "${cmd:-none}" == "none" ]
  then
    while read lin
    do
      LOG $lin
    done
  else 
    $cmd | while read lin
    do
      LOG $lin
    done
  fi
}

TRACE_ON() {
  if [ ${TRACE:-0} -eq 1 ]
  then
    set -x
  fi
}

TRACE_OFF() {
  set +x
}

## Change directory without the path being displayed
## Args: the directory to cd to.
chdir() {
  cd $1 > /dev/null
}

## Generate a random number between min and max, inclusive
## Args: three: min max varname
## the random value will be available as $varname.
random() {
  if [ ${randval:--1} -eq -1 ] # First time we have called this function
  then  # So we seed the sequence 
    # the pipe to bc is required because date will sometimes return a number 
    # with a leading 0, but is not a valid octal number
    ((RANDOM=( ( `date +'%S' | bc` + 1 ) * $$ ) + `date +'%j' | bc` ))
  fi
  min=$1
  max=$2
  val=$3
  ## $RANDOM will be between 0 and 32767 inclusive
  ##randval=`echo "( ( $RANDOM * ( $max - $min ) ) / 32767 ) + $min" | bc`
  ((randval=( ( $RANDOM * ( $max - $min + 1 ) ) / ( 32767 + 1 ) ) + $min ))
  export $val=$randval
}

## Terminate the script that calls this function
## Args: none.
terminate() {
  LOG "Exiting ..."
  kill $$ >/dev/null 2>&1
  kill -9 $$ >/dev/null 2>&1
}

## Report a problem, execute a cleanup function, and terminate the calling script.
badcmd() {
  trap '' 1 SIGINT 3 4 5 6 7 8 9 10 11 12 13 14 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 ERR EXIT
  ERROR "Failure at line $LINENO of $0"
  ERROR "Cleaning up ..."
  fnam=`declare -F cleanup`
  if [ ${fnam:-none} != "none" ]
  then
    cleanup
  fi
  ERROR "Test run terminating" 
  ERROR `date`
  trap - 1 3 4 5 6 7 8 9 10 11 12 13 14 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 ERR EXIT
  trap terminate SIGINT ERR EXIT
  terminate
}

## Set RETVAL to be the number of arguments passed
## Args: any number of anything
argCount() {
  export RETVAL=$#
}

## Set RETVAL to 1 if the first args is also in the remaining args, 0 otherwise
contains() {
  RETVAL=0
  if [ $# -lt 2 ]
  then
    return
  fi
  local target=$1
  shift
  for id in $*
  do
    if [ $id == $target ]
    then
      RETVAL=1
      return
    fi
  done
}


