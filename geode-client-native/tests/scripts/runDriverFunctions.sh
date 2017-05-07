
setTool() {
  if [ ${VALTOOL:-none} == "none" ]
  then
    useDebug=1
    VALTOOL="--tool=$2"
  else
    WARN "Tool has already been specified, ignoring -$1 option."
  fi
}

showSettings() {
  if [ $testFileTotal -eq 0 ]
  then
    usage "No test files have been specified."
  fi
  if [ $useDebug -eq 1 ]
  then
    LOG "Will use debug libraries for test."
  fi
  if [ $unicastLocator -eq 1 ]
  then
	  LOG "Will use unicast locator for topic resolution."
  fi
  if [ ${VALTOOL:-none} != "none" ]
  then
    LOG "Will use $VALTOOL with valgrind."
    if [ "${toolClients:-none}" != "none" ]
    then
      LOG "Will use valgrind only with clients: $toolClients." 
    fi
  fi
  if [ "${toolClients:-none}" != "none" ]
  then
	  LOG "Will use valgrind with clients: $toolClients." 
  fi
  if [ "${POOLOPT:-none}" != "none" ]
  then
	  LOG "Will use pool scheme with clients: $POOLOPT." 
  fi
  if [ $testFileTotal -gt 1 ]
  then
    LOG "Will process $testFileTotal test files."
  else
    LOG "Will process $testFileTotal test file."
  fi
  if [ $runIters -gt 1 ]
  then
    LOG "Will execute $runIters test iterations per test file."
  else
    LOG "Will execute $runIters test iteration per test file."
  fi
  if [ $timeLimit -gt 0 ]
  then
    LOG "Run will be limited to $timeLimit seconds."
  fi
  DEBUG "PATH=$PATH"
  DEBUG "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
  DEBUG "Using framework build in $fwkbuild"
}

usage() {
  LOG ""
  LOG "Problem executing script: $script"
  LOG ""
  LOG "  $1"
  LOG ""
  LOG "Usage:   path_to_script/$script [ options as explained below ] [ hosts to run on ]"
  LOG ""
  LOG "The options are: ( at least one of these must be specified )"
  LOG "  -x test_xml -- xml file containing test, this can be specified more than once"
  LOG "  -l test_list -- file containing a list of test files, this can be specified more than once"
  LOG "  Any combination of these two options are allowed."
  LOG ""
  LOG "  Optionally any of:"
  LOG "  -W <windows build location>"
  LOG "  -L <linux build location>"
  LOG "  -S <solaris build location>"
  LOG "    These locations must be specified as: host:path_to_build"
  LOG "    For example:   tiny:/export/tiny1/users/tkeith/built/current"
  LOG ""
  LOG "  Optionally one of:"
  LOG "  -D -- use debug build"
  LOG "  -M -- use valgrind with memcheck to do memory checking"
  LOG "  -C -- use valgrind with cachegrind to do cache profiling"
  LOG "  -H -- use valgrind with massif to do heap profiling"
  LOG "  -P -- use valgrind with callgrind to do function call profiling"
  LOG "  -R -- use to upload the regression results into database table"
  LOG ""
  LOG "  Optionally:"
  LOG "  -V path_to_valgrind -- location where valgrind is installed,"
  LOG "         defaults to /gcm/where/cplusplus/perftools/valgrind"
  LOG ""
  LOG "  Optionally any of:"
  LOG "  -r <n>  -- run test files n number of times, the default is once"
  LOG "  -u -- use a unicast locator, default is to use multicast for topic resolution"
  LOG "  -t n -- use the valgrind tool with this client, default is all clients, use multiple times to specify multple clients"
  LOG "  -d <path> -- directory to use as cwd when starting test"
  LOG "  -X <time_string> -- maximum amount of time for entire test run to run in"
  LOG "  -N -- no execute mode, this exercises the framework, but clients do not run the task, they just return success"
  LOG "  -h <file_containing_host_name> -- hosts to use in test"
  LOG "  -m mail_address -- email address to send results of run to, can be specified multiple times"
  LOG "  -o options_file -- file containing commandline options"
  LOG "  -I -- do not copy the framework build before running tests"
  LOG "  -k -- do not start the dialog killer on windows"
  LOG "  -p [poolwithlocator/poolwithendpoints] -- to start the test with pool using locatorEndpoint/serverEndpoints"
  LOG "  host list -- optional list of hosts to use in test, default is localhost"
  LOG ""
  LOG "  If no hosts are specified, all clients will be started on the local host."
  LOG "  Hosts may be specified more than once on the command line."
  LOG "  If the number of hosts is greater than the number of clients specified in the xml, one client per host will be started."
  LOG "  If the xml specifies more clients than the number of hosts on the command line, the hosts will be used for multiple clients."
  LOG "  Hosts will be selected for use round robin from the list on the command line."
  LOG ""
  LOG "  The script uses the path information to find other components it requires,"
  LOG "  so the script must remain with the rest of the build results,"
  LOG "  and the location of the script should not be part of the PATH environment variable,"
  LOG "  and so the script should be invoked with a path prepended to the script, even if it is just ./runDriver"
  LOG ""
  LOG ""
  terminate
}

##  while getopts ":IDMCHPkuNx:l:r:t:V:L:S:W:X:h:m:d:o:" op
#-I -- do not copy the framework build before running tests
#-D -- use debug build
#-M -- use valgrind with memcheck
#-C -- use valgrind with cachegrind
#-H -- use valgrind with massif
#-P -- use valgrind with callgrind
#
# at least one of the next two arguments are REQUIRED
#-x <xml file> -- xml file containing test to run
#-l <file containing names of xml files> -- list of xml files to run
#
#-r <n>  -- run test suite n number of times, the default is once, as if -r 1 was specified
#-u -- use a unicast locator, default is to use multicast for topic resolution
#-t n -- use the valgrind tool with this client, use 0 to indicate Driver program
#-k -- do not start the dialog killer on windows
#
#-L <host>:<build_location> -- location of linux build to use for test
#-S <host>:<build_location> -- location of solaris build to use for test
#-W <host>:<build_location> -- location of windows build to use for test
#
#-V valgrind_location  -- location where valgrind is installed
#
#-h <file_containing_host_name> -- hosts to use in test
#-m mail_address -- email address to send results of run to, can be specified multiple times
#-d test_directory -- directory to use as base directory during test run
#-X <time_string> -- maximum amount of time for entire test run to run in
#-N -- tells Clients to not run tasks, just return success to Driver
#
#-o options_file -- file containing commandline options 
#host list -- optional list of hosts to use in test, default is localhost

processArgs() {
  local scnt=0
  local OPTIND=1
  
  while getopts ":IDMCHPRkuNx:l:r:t:V:L:S:W:X:h:m:d:o:p:s" op
  do
    case $op in
      ( "I" ) useInplace=1 ;;
      ( "D" ) useDebug=1 ;;
      ( "s" )
              TESTSTICKY="ON" 
              ;;
      ( "M" ) setTool $op memcheck
        VALGRINDOPTS="--gen-suppressions=yes --workaround-gcc296-bugs=yes --leak-check=full --show-reachable=yes --leak-resolution=med"
      ;;
      ( "C" ) setTool $op cachegrind ;;
      ( "H" ) setTool $op massif 
        VALGRINDOPTS="--dump-instr=yes --depth=10 --collect-jumps=yes"
          ;;
      ( "P" ) setTool $op callgrind
        #  VALGRINDOPTS="--separate-threads=yes --dump-instr=yes --collect-jumps=yes"
        VALGRINDOPTS="--dump-instr=yes --collect-jumps=yes"
      ;;
      ( "W" ) ((scnt++))
        cygBuild=$OPTARG
      ;;
      ( "L" ) ((scnt++))
        linBuild=$OPTARG
      ;;
      ( "S" ) ((scnt++))
        sunBuild=$OPTARG
      ;;
      ( "V" ) ((scnt++)) ; VALGRIND_INSTALL=$OPTARG ;;
      ( "R" ) uploadDatabase=1 ;;
      ( "X" ) ((scnt++)) ; runLimit=$OPTARG ;;
      ( "x" ) ((scnt++)) ; clXmls="$clXmls $OPTARG" ; clTests="$clTests $OPTARG" ;;
      ( "l" ) ((scnt++)) ; clLists="$clLists $OPTARG" ; clTests="$clTests $OPTARG" ;;
      ( "r" ) ((scnt++))
        case $OPTARG in 
          ( [0-9]* ) runIters=$OPTARG ;;
          ( * ) ;;
        esac ;;
      ( "u" ) unicastLocator=1 ;;
      ( "k" ) noDialog=0 ;;
      ( "t" ) ((scnt++)) ; toolClients="$toolClients $OPTARG" ;;
      ( "d" ) ((scnt++)) ; DIRNAM=$OPTARG ;;
      ( "p" ) ((scnt++)) ; 
        poolopt=$OPTARG
        if [ "${poolopt:-none }" != "none" ]
        then
          if [ "$poolopt" == "poolwithendpoints" ]
          then
            POOLOPT="$poolopt"
          elif [ "${poolopt}" == "poolwithlocator" ]
          then
            POOLOPT="poolwithlocator"
          fi
        fi
       ;;                                      
      ( "m" ) ((scnt++)) ; mailAddrs="${mailAddrs} $OPTARG" ;;
      ( "o" ) ((scnt++))
        fil=$OPTARG
        if [ -f $fil ]
        then
        for nam in `cat $fil | ${AWK:-awk} '{gsub(/#.*$/, ""); print}' | grep -e "-"`
          do
            xargs="$xargs $nam"
          done
          processArgs $xargs
        fi
      ;;
      ( "h" ) ((scnt++))
        fil=$OPTARG
        if [ -f $fil ]
        then
          for nam in `cat $fil`
          do
            HOSTS="$HOSTS $nam"
          done
        fi
      ;;
      ( "N" ) noExecTasks=1 ;;
      ( * ) WARN "Unknown argument provided: -$OPTARG, ignoring." ;;
    esac
    ((scnt++))
  done

  while [ $scnt -gt 0 ]
  do
    shift
    ((scnt--))
  done
  if [ "${POOLOPT:- }" == " " ]
  then
   if [ ${num:-none } == "none" ]
   then   
     random 0 1 num
   fi   
   #LOG "num = $num"
   if [ $num -eq 1 ]
   then
      POOLOPT="poolwithlocator"
    else
      POOLOPT="poolwithendpoints"
   fi
  fi
  #LOG "pool is set as $POOLOPT" 
  #LOG "TestSticky is set as $TESTSTICKY"
  HOSTS="$HOSTS $*"
}

processGroups() {
  if [ "${HOSTS:- }" == " " ]
  then
    HOSTS=$GF_FQDN
  fi
  
  ## Scan HOSTS var looking for group tag
  rm -rf hosts
  mkdir hosts
  for entry in $HOSTS
  do
    ## FwkClientSet::m_defaultGroup is defined as "DEFAULT", so we need to use it here
    echo $entry | tr ":" " " | ${AWK:-awk} '{if ( NF == 1 ) print "DEFAULT", $1 ; else print $1, $2 }' > hst_tmp
    read grp ahost < hst_tmp
    fhost=`nslookup $ahost 2>/dev/null | ${AWK:-awk} '/^Name:/{print $2}'`
    echo $fhost >> hosts/$grp
    new_hosts="$new_hosts $fhost"
  done
  rm -f hst_tmp
  HOSTS="$new_hosts"
  
  LOG "Commandline hosts are: $HOSTS"
  export UHOSTS=`for nam in $HOSTS; do echo $nam; done | sort -u | tr '\n' ' '`
}

clearVars() {
  ## Clear some values to be set by commandline args
  export useInplace=0
  export useDebug=0
  export uploadDatabase=0
  export runIters=1
  export noExecTasks=0
  export unicastLocator=0
  export toolClients=
  export testList=
  export clXmls=
  export clLists=
  export clTests=
  export rawTestList=
  export HOSTS=
  export hostnames=
  export mailAddrs=
  export runLimit=
  export timeLimit=0
  export noDialog=1
  export DIRNAM=$PWD
  export cygBuild=
  export linBuild=
  export sunBuild=
  export testFileTotal=0
  export VALTOOL=
  export VALCMD=
  export VALGRINDOPTS=
  export VALGRIND_INSTALL=
  export POOLOPT= 
  export TESTSTICKY=
  export username=
  export passwrd=
  export hosts=
  export vmnames=
  export isvmotion=false
  export vminterval=0
  LOG "TESTSTICKY value inside clearVars is $TESTSTICKY"
}

runLatestProp() {
  ## executing LatestProp.java for fatching latest java properties
  #LCP_PATH="$fwkbuild/framework/lib/javaobject.jar:$GEMFIRE/lib/gemfire.jar"
  gcmdir=`echo $JAVA_HOME | sed 's/where.*/where/'` # gcmdir = /export/gcm/where or /gcm/where
  LCP_PATH="$fwkbuild/framework/lib/javaobject.jar"
  TESTDB_PATH="$fwkbuild/framework/lib/testdb.jar"
  MYSQLCONN_PATH="$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  SERVERDEP="$GEMFIRE/lib/server-dependencies.jar"
  if type -p cygpath >/dev/null 2>/dev/null; then
   LCP_PATH="`cygpath -p -m "$LCP_PATH"`"
   TESTDB_PATH="`cygpath -p -m "$TESTDB_PATH"`"
   MYSQLCONN_PATH="`cygpath -p -m "$MYSQLCONN_PATH"`"
   SERVERDEP="`cygpath -p -m "$SERVERDEP"`"
   LCP_PATH="$LCP_PATH;$GEMFIRE/lib/gemfire.jar;$TESTDB_PATH;$MYSQLCONN_PATH"
  else 
   LCP_PATH="$LCP_PATH:$GEMFIRE/lib/gemfire.jar:$TESTDB_PATH:$MYSQLCONN_PATH:$SERVERDEP"
  fi
   
  echo $JAVA_HOME -cp "$LCP_PATH" javaobject.LatestProp > $PWD/latest.prop
  ${JAVA_HOME} -cp "$LCP_PATH" javaobject.LatestProp > $PWD/latest.prop
}

setLocations() {
  chdir `dirname $fullscript`
  local adir=$PWD  # absolute path
  chdir -
  adir=`dirname $adir`  # absolute path
  fwkbuild=`dirname $adir`  # absolute path
  export fwkdir=$fwkbuild/framework 
}
handleRestart() {
  if [ ${DRIVER_RESTART:-no} == "no" ]
  then
    export DRIVER_RESTART="yes"
    if [ ${LOGPREFIX:-none} == "none" ]
    then
      export LOGPREFIX=`date +'%y-%m-%d_%H-%M-%S'`
    fi
    mkdir -p $DIRNAM/$LOGPREFIX
    chdir $DIRNAM/$LOGPREFIX
    rm -f ../latest
    ln -s $PWD ../latest
    export BASEDIR=$PWD 
    INITLOG $PWD/runDriver.log
    if [ $useInplace -eq 0 ]
    then
      LOG "Copying framework build to $PWD/buildfwk"
      rsync -rLq --exclude=docs --exclude=examples --exclude=vsd --delete $fwkbuild/product $fwkbuild/framework $fwkbuild/hidden buildfwk/
      LOG "Restarting script from copied framework directory ..."
      exec $PWD/buildfwk/framework/scripts/$script $cmdline
    fi
  fi ## driver restart
}

verifyTestFiles() {
  local testSpecErrors=0
  rawTestList=$clXmls
  for nam in $clLists
  do
    fil=""
    if [ -f $fwkdir/xml/$nam ]
    then
      fil=$fwkdir/xml/$nam
    fi
    if [ ${fil:-none} != "none" ]
    then
      for tst in `cat $fil | ${AWK:-awk} '{gsub(/#.*$/, ""); print}'`
      do
        rawTestList="$rawTestList $tst"
      done
    else
      ERROR "List file: $nam could not be found in \"$fwkdir/xml\""
      ((testSpecErrors++))
    fi
  done
  testFileTotal=0
  for nam in $rawTestList
  do
    if [ -f $fwkdir/xml/$nam ]
    then
      testList="$testList $nam"
      ((testFileTotal++))
    else
      ERROR "Test file: $nam could not be found in $fwkdir/xml"
      ((testSpecErrors++))
    fi
  done
  
  if [ $testSpecErrors -ne 0 ]
  then
    ERROR "Failure near line $LINENO of $0"
    ERROR "Found $testSpecErrors error(s) when parsing your -x and -l options, please verify the paths to your xml files" 
    terminate 
  fi
}

logTests() {
  LOG "Tests/Lists specified on the command line:"
  for nam in $clTests
  do
    LOG "    $nam"
  done
  LOG "Resulted in these tests being selected to run:"
  cnum=0
  for nam in $testList
  do
    ((cnum++))
    LOG "    $cnum  $nam"
  done
}

miscDriverSetup() {
  # Where are we?
  currdir=`pwd`
  mkdir -p envs
  
  if [ $noDialog -eq 1 -a "$myOS" == "CYG" ]
  then
    windowsDialogKiller $$ > windowsDialogKiller.log 2>&1 &
    windowsDialogKillerPid=$!
    LOG "Launched windowsDialogKiller $windowsDialogKillerPid"
  fi

  for fil in ../*.env ../gfcpp.properties ../gfcpp.gfe.properties ../gfcpp.*license ../gfcsharp.properties
  do
    if [ -f $fil ]
    then
      cp $fil .
      if [ "${fil}" == "../gfcpp.env" ]
      then
        source $fil
      fi
    fi
  done

  if [ $useDebug -eq 1 ]
  then
    export dp="/debug"
  else
    export dp=""
  fi
  export GFE_INSTALL="/export/gcm/where/cplusplus/thirdparty/linux/gfe/product"
  if [ ${VALTOOL:-none} != "none" ]
  then
    VALCMD="valgrind $VALTOOL $VALGRINDOPTS"
  else
    VALCMD=
  fi
  if [ ${VALGRIND_INSTALL:-none} == "none" ]
  then
    VALGRIND_INSTALL="/export/gcm/where/cplusplus/perftools/valgrind320"
  fi 
  ## content of envs/extra_env
  echo -n "export UHOSTS=\"$UHOSTS\" ; " > envs/extra_env
  for var in ${!GF*}
  do
    echo -n "export $var=\"${!var}\" ; " >> envs/extra_env
  done
  echo -n "export VALGRIND_INSTALL=$VALGRIND_INSTALL ; " >> envs/extra_env
  echo -n "export VALCMD=\"$VALCMD\" ; " >> envs/extra_env
  echo -n "export toolClients=\"$toolClients\" ; " >> envs/extra_env
  echo -n "export driverHost=$GF_FQDN ; " >> envs/extra_env
  echo -n "export POOLOPT=$POOLOPT ; " >> envs/extra_env
  echo -n "export TESTSTICKY=$TESTSTICKY ; " >> envs/extra_env
  if [ $noExecTasks -eq 0 ]
  then
    echo -n "export ne=' ' ; " >> envs/extra_env
  else
    echo -n "export ne='-N' ; " >> envs/extra_env
  fi
  
  vp=
  vl=
  vcmd=
  valLoc=$VALGRIND_INSTALL/$HOSTTYPE
  contains 0 $toolClients
  if [ -d $valLoc -a $RETVAL == 1 ]  ## Use valgrind on Driver
  then
    vp=$valLoc/bin
    vl=$valLoc/lib
    vcmd="$VALCMD"
  fi
  fp=$fwkbuild/framework
  hp=$fwkbuild/hidden
  pp=$fwkbuild/product
  export PATH=$fp/scripts:$fp/bin$dp:$fp/lib$dp:$hp/lib$dp:$pp/lib:$pp/bin:$fp/bin/openssl:$vp:$PATH
  export LD_LIBRARY_PATH=$fp/lib$dp:$hp/lib$dp:$hp/lib:$pp/lib:$fp/lib/openssl:$vl:$LD_LIBRARY_PATH

  case $myOS in
    ( "CYG" ) 
      if [ ${cygBuild:-none} == "none" ]
      then
        cygBuild=$GF_IPADDR:$fwkbuild
      fi ;;
    ( "LIN" ) 
      if [ ${linBuild:-none} == "none" ]
      then
        linBuild=$GF_IPADDR:$fwkbuild
      fi ;;
    ( "SUN" ) 
      if [ ${sunBuild:-none} == "none" ]
      then
        sunBuild=$GF_IPADDR:$fwkbuild
      fi ;;
    ( * ) usage "This script not ready for use on $OSTYPE"  | tee -a $LOG ;;
  esac
  
  if [ ${GFE_DIR:-NO_GFE_DIR_SPECIFIED} == "NO_GFE_DIR_SPECIFIED" ]
  then
    export GFE_DIR=$GFE_INSTALL
  fi
  
  if [ ${runLimit:-none} != "none" ]
  then
    #toSeconds $runLimit
    timeLimit=`TimeStr $runLimit`
  fi
  
  trap '' ERR
  ulimit -c unlimited 2> /dev/null
  ulimit -n `ulimit -Hn` 2> /dev/null
  trap badcmd ERR
}

writeHostSetup() {
  echo "#!/bin/bash"
  echo "#set -x"
  echo "hst=\$1"
  echo "myOS=\`uname | tr \"cyglinsu\" \"CYGLINSU\" | cut -b1-3\`"
  echo ""
  echo "valLoc=$VALGRIND_INSTALL/\$HOSTTYPE"
  echo "if [ -d \$valLoc ]"
  echo "then"
  echo "  vp=\$valLoc/bin"
  echo "  vl=\$valLoc/lib"
  echo "else"
  echo "  vp="
  echo "  vl="
  echo "fi"
  echo ""
  echo "unprov=0"
  echo "if [ -d $BASEDIR ]" 
  echo "then"
  echo "  cd $BASEDIR" 
  echo "else"
  echo "  unprov=1"
  echo "  mkdir -p ~/$LOGPREFIX"
  echo "  cd ~/$LOGPREFIX"
  echo "fi"
  echo ""
  echo "mkdir -p pids"
  echo "mkdir -p envs"
  echo "export BASEDIR=\$PWD"
  echo "export ExtraPATH=\$PATH"
  echo "export ExtraLibraryPATH=\$LD_LIBRARY_PATH"
  echo ""
  echo "if [ `uname` = \"SunOS\" ]"
  echo "then"
  echo "if [ `uname -p` = \"sparc\" ]"
  echo "then"
  echo "   JAVA_HOME_SUN=/export/gcm/where/jdk/1.7.0_79/sparcv9.Solaris/bin"
  echo "   export ExtraPATH=/usr/ccs/bin:/opt/sfw/bin:/usr/sfw/bin:/usr/sbin:/opt/SUNWspro/bin:\${HOME}/bin:\$JAVA_HOME_SUN:\$ExtraPATH"
  echo "   export ExtraLibraryPATH=/opt/SUNWspro/lib:/opt/sfw/lib:/usr/sfw/lib:\$ExtraLibraryPATH"
  echo "else"
  echo "  export ExtraPATH=/usr/ccs/bin:/opt/sfw/bin:/usr/sfw/bin:/usr/sbin:/opt/SUNWspro/bin:\$ExtraPATH"
  echo "  export ExtraLibraryPATH=/opt/SUNWspro/lib:/opt/sfw/lib:/usr/sfw/lib:\$ExtraLibraryPATH"
  echo "fi"
  echo "fi"
  echo ""
  echo "echo -n \"export BASEDIR=\$BASEDIR \" > envs/\${hst}_tmp" 
  echo "echo -n \" ; if [ \$myOS == \"CYG\" ] \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; then export BUILDDIR=\\\`cygpath -p -w \$BASEDIR/build_\$myOS\\\` \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; else export BUILDDIR=\$BASEDIR/build_\$myOS \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; fi \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; if [ \$myOS == \"CYG\" ] \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; then export GFCPP=\\\`cygpath -p -w \$BASEDIR/build_\$myOS/product\\\` \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; else export GFCPP=\$BASEDIR/build_\$myOS/product \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; fi \" >> envs/\${hst}_tmp"
  echo "fp=\$PWD/build_\$myOS/framework"
  echo "hp=\$PWD/build_\$myOS/hidden"
  echo "pp=\$PWD/build_\$myOS/product"
  #echo "echo -n \" ; export PATH=\$fp/scripts:\$fp/bin$dp:\$fp/lib$dp:\$hp/lib$dp:\$pp/lib:\$pp/bin:\$fp/bin/openssl:\$vp:\$PATH \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; export PATH=\$fp/scripts:\$fp/bin$dp:\$fp/lib$dp:\$hp/lib$dp:\$pp/lib:\$pp/bin:\$fp/bin/openssl:\$vp:\$ExtraPATH \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; export LD_LIBRARY_PATH=\$fp/lib$dp:\$hp/lib$dp:\$hp/lib:\$pp/lib:\$fp/lib/openssl:\$vl:\$ExtraLibraryPATH \" >> envs/\${hst}_tmp"
  echo "echo -n \" ; ulimit -c unlimited 2> /dev/null; ulimit -n \`ulimit -Hn\` 2> /dev/null\" >> envs/\${hst}_tmp"
  echo "echo -n \" ; if [ -d \$BASEDIR ] ; then cd \$BASEDIR ; fi ;\" >> envs/\${hst}_tmp"
  echo "export AWK=\`which nawk 2>/dev/null\`"
  echo "if [ ! -d build_\$myOS ]"
  echo "then"
  echo "  ls *_BUILD_\$myOS > /dev/null 2>&1"
  echo "  if [ \$? -ne 0 ]"
  echo "  then"
  echo "    touch \${hst}_BUILD_\$myOS"
  echo "    sleep 3"
  echo "    fnam=\`ls *_BUILD_\$myOS 2> /dev/null | \${AWK:-awk} '{if ( NR == 1 ) print \$1;}'\`"
  echo "    snam=\`basename \$fnam _BUILD_\$myOS\`"
  echo "    if [ \"\$snam\" == \$hst ]"
  echo "    then"
  echo "      echo \"     hostSetup:: \$hst will provision for \`uname\`\""
  echo "      mkdir build_\$myOS && touch \${hst}_COPY"
  echo "    fi"
  echo "  fi"
  echo "fi"
  echo ""
  echo "OK=1"
  echo "if [ -f \${hst}_COPY ]"
  echo "then"
  echo "  CYGBuild=${cygBuild:-none}"
  echo "  LINBuild=${linBuild:-none}"
  echo "  SUNBuild=${sunBuild:-none}"
  echo "  build_name=\${myOS}Build"
  echo "  COPY_BUILD=\${!build_name:-none}"
  echo "  if [ \$COPY_BUILD == \"none\" ]"
  echo "  then"
  echo "    OK=0"
  echo "    echo \"     hostSetup:: ERROR: Build not specified for os: \`uname\`\""
  echo "  else"
  echo "    rsync -rLqe \"ssh -A -x -q\" --exclude=docs --exclude=examples --exclude=vsd \$COPY_BUILD/[fhp]\* build_\$myOS && OK=1"
  echo "    echo \"     hostSetup:: \${hst}: provisioning for \`uname\` complete.\""

#  echo "  if [ \$myOS == \"CYG\" ]"
#  echo "  then"
#  echo "    echo \"     Detected \$myOS : installing SxS dlls\""
#  echo "#   Work around \"product is already installed\", install then repair"
#  echo "    msiexec /quiet /log MSI.log /i build_\$myOS\\\\framework\\\\scripts\\\\fwk_provision.msi"
#  echo "    msiexec /quiet /log MSI_repair.log /fa build_\$myOS\\\\framework\\\\scripts\\\\fwk_provision.msi && MSIOK=1"
#  echo "    if [ \${MSIOK:-0} -ne 1 ]"
#  echo "    then"
#  echo "      OK=0"
#  echo "      echo \"     hostSetup:: ERROR: installation of SxS dlls failed, see MSI*.log\""
#  echo "    fi"
#  echo "  fi"

  echo "    cp build_\$myOS/framework/xml/*.xsd ."
  echo "    cp build_\$myOS/framework/rcsinfo rcsinfo_\$myOS"
  echo "  fi"
  echo "  if [ \$unprov == 1 ]"
  echo "  then"
  echo "    touch \${hst}_UNPROV"
  echo "  fi"
  echo "  rm -f \${hst}_COPY"
  echo "else"
  echo "  tcnt=0"
  echo "  while [ ! -f build_\$myOS/framework/scripts/startClient ]"
  echo "  do"
  echo "    ((tcnt++))"
  echo "    sleep 20"
  echo "    if [ \$tcnt -gt 10 ]"
  echo "    then"
  echo "      touch build_\$myOS "
  echo "      touch build_\$myOS/framework "
  echo "      touch build_\$myOS/framework/scripts "
  echo "      touch build_\$myOS/framework/scripts/startClient "
  echo "    fi"
  echo "    if [ \$tcnt -gt 20 ]"
  echo "    then"
  echo "      OK=0"
  echo "      break"
  echo "    fi"
  echo "    ls -R build_\$myOS >/dev/null 2>&1 "
  echo "  done"
  echo "fi"
  echo ""
  echo "flist="
  echo "if [ \$OK -eq 1 ]"
  echo "then"
  echo "  if [ -f \${hst}_UNPROV ]"
  echo "  then"
  echo "    scp $GF_IPADDR:$BASEDIR/envs/extra_env envs/" 
  echo "    echo \"rm -f envs/\${hst}_unprov\" >> \${hst}_unprovision"
  echo "##    echo \"stopAll local\" >> \${hst}_unprovision"
  echo "    cnt=0"
  echo "    while [ -d pids/\$hst -a \$cnt -lt 6 ]"
  echo "    do"
  echo "      ((cnt++))"
  echo "      sleep 10"
  echo "    done"
  echo "    echo \"rm -rf build_\$myOS\" >> \${hst}_unprovision"
  echo "    echo \"unam=\\\`ls *_unprovision 2> /dev/null | \${AWK:-awk} '( NR == 1 ) { print \\\$1}'\\\`\" >> \${hst}_unprovision"
  echo "    echo \"if [ \\\${unam:-none} == \"\${hst}_unprovision\" ]\" >> \${hst}_unprovision"
  echo "    echo \"then\" >> \${hst}_unprovision"
  echo "    echo \"  rsync --exclude build_* --exclude unprovision -rLqe \\\"ssh -A -q -x\\\" * $GF_IPADDR:$BASEDIR\" >> \${hst}_unprovision" 
  echo "    echo \"  cd\" >> \${hst}_unprovision"
  echo "    echo \"  sleep 20 ; rm -rf \$BASEDIR &\" >> \${hst}_unprovision"
  echo "    echo \"fi\" >> \${hst}_unprovision"
  echo "    touch envs/\${hst}_unprov"
  echo "    flist=\"\$flist envs/\${hst}_unprov\""
  echo "    rm -f \${hst}_UNPROV"
  echo "  fi"
  echo "  mv envs/\${hst}_tmp envs/\${hst}_env"
  echo "  flist=\"\$flist envs/\${hst}_env\""
  echo "else"
  echo "  touch envs/\${hst}_bad"
  echo "  flist=\"\$flist envs/\${hst}_bad\""
  echo "fi"
  echo "if [ \$unprov -eq 1 ]"
  echo "then"
  echo "  scp \$flist $GF_IPADDR:$BASEDIR/envs" 
  echo "fi"
  echo "rm -f \${hst}_BUILD_\$myOS"
}

waitForCompletion() {
  if [ $# -lt 2 ]
  then
    echo "Too few arguments to waitForCompletion: \"$*\", require tag and at least one host name."
    return
  fi
  local stime=$SECONDS
  local tag=$1
  shift
  local whosts="$*"
  argCount $whosts
  local wcnt=$RETVAL
  local dcnt=0
  local bcnt=0
  local tcnt=-1
  local bad=
  local good=
  local allowed=0
  local ival=0
  local odiv=0
  local etime=0
  ((allowed=600+(30*$wcnt)))
  ((ival=$allowed/10))
  ((etime=$stime+$allowed))
  while [ $tcnt -lt $wcnt ]
  do
    if [ $tcnt -gt -1 ]
    then
      dcnt=0
      bcnt=0
      bad=
      good=
      sleep 10
    fi
    for nam in $whosts
    do
      if [ -f $BASEDIR/envs/${nam}$tag ] 
      then
        good="$good $nam"
        ((dcnt++))
      fi
      if [ -f $BASEDIR/envs/${nam}_bad ] 
      then
        bad="$bad $nam"
        ((bcnt++))
      fi
    done
    ((tcnt=$dcnt+$bcnt))
    ((now=$SECONDS-$stime))
    ((ndiv=$now/$ival))
    if [ $ndiv -gt $odiv ]
    then
      odiv=$ndiv
      LOG "$now of $allowed seconds used, $wcnt hosts total, $dcnt completed ok, $bcnt failed."
      LOG "Completed hosts $good"
    fi
    if [ $now -gt $allowed ]
    then
      ERROR "Hosts failed to complete in time allowed."
      ERROR "$allowed seconds allowed for $scnt hosts, $dcnt completed ok, $bcnt failed."
      ERROR "Completed hosts $good"
      terminate
    fi
  done
  if [ "${bad:-none}" != "none" ]
  then
    ERROR "Hosts failed to complete successfully."
    ERROR "$dcnt completed ok, $bcnt failed."
    ERROR "$bad failed."
    ERROT "$good Completed successfully."
    terminate
  fi
}

## First arg is host, rest are command to execute
goHost() {
  TRACE_ON
  local host=$1
  shift
  local cmd="$*"
  DEBUG "In goHost"
  local ev="`cat $BASEDIR/envs/extra_env $BASEDIR/envs/${host}_env`" 
  DEBUG "goHost: ssh -A -x -q -f $host \"$ev $cmd \" "
  ssh -A -x -o "NumberOfPasswordPrompts 0" -f $host "$ev $cmd" 
  DEBUG "goHost complete."
  TRACE_OFF
}

provisionDir() {
  sDir=$1
  setPHOSTS
  if [ "${PHOSTS:-none}" != "none" ]
  then
    LOG "Provisioning $sDir to: $PHOSTS"
    for hst in $PHOSTS
    do
      LOG "Provision $hst"
      goHost $hst "rsync -rLqe \"ssh -A -x -q\" $GF_IPADDR:$sDir . ; \
                    touch tmp ; \
                    scp tmp $GF_IPADDR:$BASEDIR/envs/${hst}_prov ; \
                    rm tmp" 
    done
    LOG "Waiting for directory provisioning to complete."
    waitForCompletion _prov $PHOSTS
    rm -f $BASEDIR/envs/*_prov 
    LOG "Directory provisioning complete."
  fi
}

teardownHosts() {
  setPHOSTS
  if [ "${PHOSTS:-none}" != "none" ]
  then
    LOG "Unprovisioning hosts: $PHOSTS"
    for hst in $PHOSTS
    do
      LOG "Unprovision $hst"
      rm -f $BASEDIR/envs/${hst}_unprov
      goHost $hst "touch envs/${hst}_done ; source ./${hst}_unprovision"
    done
    LOG "Waiting for host unprovisioning to complete."
    waitForCompletion _done $PHOSTS
    rm -f $BASEDIR/envs/*_done 
    LOG "Host unprovisioning complete."
  fi
}
  
sshCheckHosts() {
  local failed=0
  LOG "Checking access to hosts ..."
  for hnam in $*
  do
    res=`ssh -A -x -q $hnam "ssh -A -x -q $GF_FQDN echo OK"`
    if [ "${res:-NOTOK}" != "OK" ]
    then
      ERROR "SSH PROBLEM between $hnam and $GF_FQDN"
      ((failed++))
    fi
  done
  if [ $failed -ne 0 ]
  then
    ERROR "Unable to run tests due to ssh problems with $failed hosts."
    ERROR "Resolve host access issues and try again."
    terminate
  fi
}

setupHosts() {
  sshCheckHosts $UHOSTS
  
  writeHostSetup > hostSetup
  chmod +rwx hostSetup
  
  for hst in $UHOSTS
  do
    LOG "Setting up $hst"
    dest=./hostSetup${hst}_$$
    ssh -A -x -o "NumberOfPasswordPrompts 0" -f $hst "rm -f $dest ; \
                     scp $GF_IPADDR:$BASEDIR/hostSetup $dest ; \
                     $dest $hst ; \
                     rm -f $dest &" 
  done
  
  LOG "Waiting for host setup to complete."
  waitForCompletion _env $UHOSTS
  LOG "Host setup complete."
}

setPHOSTS() {
  export PHOSTS= 
  for nam in `ls -1 $BASEDIR/envs | grep _unprov`
  do
    uhst=`basename $nam _unprov`
    PHOSTS="${PHOSTS:-} $uhst"
  done
}

setupTest() {
  logbase=`basename $tfile .xml`
  perfstatFile=`echo $tfile | sed 's/.xml$//'`
  if [ "$PERFTEST" == "true" ]
  then
    export latestPropFile=${perfstatFile}.conf
  else
    export latestPropFile=${perfstatFile}.xml
  fi
  #echo $perfstatFile
  export stestFile=${logbase}.xml
  
  if [ $testFileTotal -gt 1 ]
  then
    tfSuff=$testFileCnt
  else
    tfSuff=""
  fi
  if [ $runIters -gt 1 ]
  then
    riSuff=$rcnt
  else
    riSuff=""
  fi
  if [ ${#tfSuff} -gt 0 -a ${#riSuff} -gt 0 ]
  then
    sep="."
  else
    sep=""
  fi
  prefix=$tfSuff$sep$riSuff
  if [ ${prefix:-xXx} != "xXx" ]
  then 
    prefix=${prefix}_
  fi
  logDate=`date +'-%m%d-%H%M%S'`
  logDir=${logbase}${logDate}
  mkdir -p $logDir
  
  rm -f $logDir/gfcpp.properties
  echo "log-level=Info" >> $logDir/gfcpp.properties
  echo "stacktrace-enabled=true" >> $logDir/gfcpp.properties
  
  mkdir -p pids/$GF_FQDN/$logDir
  
  if [ $unicastLocator -eq 1 ]
  then
    export LBM_LICENSE_INFO='Organization=GemStone:Expiration-Date=never:License-Key=B05B CCC1 D6B5 288C'
    LOG "Starting a unicast resolver on $GF_FQDN"
    $fwkbuild/product/lib/gfcpp_locator &
    echo $! > pids/$GF_FQDN/$logDir/locator_pid
    echo "locators=$GF_IPADDR" >> $logDir/gfcpp.properties
  fi
    
  for fil in *.env gfcpp.*properties 
  do
    if [ -f $fil ]
    then
      cat $fil >> $logDir/$fil
    fi
  done

  for fil in gfcpp.*license gfcsharp.properties gfcsharp.env gfcppcsharp.env
  do
    if [ -f $fil ]
    then
      cp $fil $logDir
    fi
  done

  cp $fwkdir/xml/$tfile $logDir
  #cp $fwkdir/xml/${perfstatFile}.spec $logDir/statistics.spec
  cp $fwkdir/xml/*.xsd .
  
  chdir $logDir
  LOG "Using test directory: $logDir"
  export valRes=0
  LOG "Validate: $stestFile" | tee Val_${stestFile}.log
  Validate $stestFile  2>&1 >> Val_${stestFile}.log || valRes=$?
  if [ $valRes -ne 0 ]
  then
    ERROR "$stestFile failed xml parsing. ( $valRes )"
    LOGCONTENT Val_${stestFile}.log
    break
  fi
  # These files required for smoke perf tests comparision with hydra perf tool.
  echo "perffmwk.comparisonKey=${logbase}" >> ${logbase}.prop
  #echo "TestName=${perfstatFile}.conf" >> latest.prop
  echo "nativeClient.TestType=C++" >> latest.prop
  echo "nativeClient.Hosts=$UHOSTS" >> latest.prop
  #cat ../../latest.prop >> latest.prop
  rm -f local.files
  LOG FileProcessor $stestFile | tee FileProcessor.log
  FileProcessor $stestFile 2>&1 | tee -a FileProcessor.log
}

doSubSummary() {
  FIRST="first.part"
  LAST="last.part"
  rm -f $FIRST $LAST
  
  LOGTO $FIRST "---------------------------"
  LOGTO $FIRST "Test results are in $PWD"
  LOGTO $FIRST "Ran test: $tfile  -- Run $rcnt of $runIters"
  LOGTO $FIRST "Test used: $TIMEBREAKDOWN"
  LOGTO $FIRST "  "
  LOGTO $FIRST "Results Summary"
  LOGTO $FIRST "---------------------------"

  LOGTO $LAST "End of $tfile test run  -- $rcnt of $runIters"
  LOGTO $LAST "---------------------------"
  LOGTO $LAST ""
  LOGTO $LAST ""
}
doGenCsvReport()
{
    echo "Generating csv report..."
  REPORT="error.report"
  FIRST="first.part"
  MIDDLE="middle.part"
  LAST="last.part"
  FullReport="../error.report"
  CSVREPORT="regression.csv"
  FullCsvReport="../regression-$LOGPREFIX.csv"
  is64bit=__IS_64_BIT__
  if [ $is64bit -eq 1 ]; then
    ARCH="64"
  else
    ARCH="32"
  fi

    gcmdir=`echo $JAVA_HOME | sed 's/where.*/where/'` # gcmdir = /export/gcm/where or /gcm/where
  CP_PATH="$fwkbuild/framework/lib/testdb.jar"
  if type -p cygpath >/dev/null 2>/dev/null; then
   CP_PATH="`cygpath -p -m "$CP_PATH"`"
   CP_PATH="$CP_PATH;$GEMFIRE/lib/gemfire.jar;$JTESTS;$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  else
   CP_PATH="$CP_PATH:$GEMFIRE/lib/gemfire.jar:$JTESTS:$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  fi

    cd $PWD
    cat ../latest.prop >> latest.prop
    DIR_NAME="$PWD"
    if type -p cygpath >/dev/null 2>/dev/null; then
      DIR_NAME="`cygpath -p -m "$PWD"`"
    fi
    # Generating perfreport.txt for each tests in the last because it need latest.prop which comes after unprovisionig in completion of the tests.
    if [ "$PERFTEST" == "true" ]
    then
      echo $JAVA_HOME -cp "$CP_PATH" -DJTESTS=$GFE_DIR/../tests/classes -Dgemfire.home=$GEMFIRE perffmwk.PerfReporter "$DIR_NAME"
      $JAVA_HOME -cp "$CP_PATH" -DJTESTS=$GFE_DIR/../tests/classes -Dgemfire.home=$GEMFIRE perffmwk.PerfReporter "$DIR_NAME"
      if [ ! -f $DIR_NAME/../../summary.prop ]
      then
       cp -f $DIR_NAME/latest.prop $DIR_NAME/../../summary.prop
      fi
      cp -r $DIR_NAME $DIR_NAME/../../.
    fi
    rm -f $MIDDLE $REPORT
    grep SUMMARY Driver.log >> $FIRST || LOGTO $FIRST "No SUMMARY lines found."
    LOGTO $MIDDLE "---------------------------"
    LOGTO $MIDDLE "Suspect strings:"
    grepLogs.pl $PWD >> $MIDDLE 2>&1
    LOGTO $MIDDLE "---------------------------"
    mcnt=`wc -l $MIDDLE | ${AWK:-awk} '{print $1}'`
    if [ $mcnt -gt 10 ]
    then
      WARNTO $FIRST "$mcnt lines of suspect string content associated with this test:"
      WARNTO $FIRST " "                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               $FullReport "$mcnt lines of suspect string content associated with this test. Details in: $PWD/$REPORT"
    fi
    ${AWK:-awk} -F "=" '/^Name/ {print ""; next} {printf "%s,", $2} ' latest.prop > $CSVREPORT
    TESTDATENTIME=`date +'%m/%d/%Y %T %Z'`
    ls failure.txt > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
     echo "$POOLOPT,$PWD,pass,$myOS,$ARCH,$TESTDATENTIME" >>  $CSVREPORT
    else
     echo "$POOLOPT,$PWD,fail,$myOS,$ARCH,$TESTDATENTIME" >>  $CSVREPORT
    fi
    cat $FIRST >> $REPORT
    cat $FIRST >> $FullReport
    cat $MIDDLE >> $REPORT
    cat $LAST >> $REPORT
    cat $LAST >> $FullReport
    cd $currDir

}
doSummary() {
  echo "Generating tests summary..."
  REPORT="error.report"
  FIRST="first.part"
  MIDDLE="middle.part"
  LAST="last.part"
  FullReport="../error.report"
  CSVREPORT="regression.csv"
  FullCsvReport="../regression-$LOGPREFIX.csv"
  is64bit=__IS_64_BIT__
  if [ $is64bit -eq 1 ]; then
    ARCH="64"
  else
    ARCH="32"
  fi


  
  if [ "$summaryDirs" == "" ]
  then
    LOGTO $REPORT "No test results to summarize."
    return
  fi
  gcmdir=`echo $JAVA_HOME | sed 's/where.*/where/'` # gcmdir = /export/gcm/where or /gcm/where
  CP_PATH="$fwkbuild/framework/lib/testdb.jar"
  if type -p cygpath >/dev/null 2>/dev/null; then
   CP_PATH="`cygpath -p -m "$CP_PATH"`"
   CP_PATH="$CP_PATH;$GEMFIRE/lib/gemfire.jar;$JTESTS;$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  else 
   CP_PATH="$CP_PATH:$GEMFIRE/lib/gemfire.jar:$JTESTS:$gcmdir/java/mysql/mysql-connector-java-5.1.8-bin.jar"
  fi
  #if [ $myOS == "CYG" ]
  #then
  #  CP_PATH="$GEMFIRE/lib/gemfire.jar;$JTESTS"
  #else
  #  CP_PATH="$GEMFIRE/lib/gemfire.jar:$JTESTS"
  #fi
  
  
  for dnam in $summaryDirs
  do
    cd $dnam
    cat $CSVREPORT >> $FullCsvReport
    #echo "rjk --- $gcmdir"
    if [ $uploadDatabase -eq 1 ]
    then
      echo $JAVA_HOME -cp "$LCP_PATH" testdb.insertRegressionData ${FullCsvReport}
      $JAVA_HOME -cp "$LCP_PATH" testdb.insertRegressionData ${FullCsvReport}
    fi
    rm -f $FIRST $MIDDLE $LAST
    cd $currDir
  done
  
  # Now mail out the results, if $mailAddrs is set
  if [ "${mailAddrs:-none}" != "none" ]
  then
    case $myOS in
      ( "CYG" ) 
        MAILPROG= ;;
      ( "LIN" ) 
        MAILPROG=mail ;;
      ( "SUN" ) 
        MAILPROG=mailx ;;
      ( * ) usage "This script not ready for use on $OSTYPE"  | tee -a $LOG ;;
    esac
    MAILPROG=`which $MAILPROG`
    if [ "${MAILPROG:none}" == "none" ]
    then
      ERROR "Results cannot be mailed to $mailAddrs, no mail program found."
    else
    LOG $MAILPROG -s "Test run results for: $clTests" $mailAddrs
      cat error.report | $MAILPROG -s "Test run results for: $clTests" $mailAddrs
    fi
  fi
  for ftnam in $clTests
  do
    tr_ftnam=`echo $ftnam | tr "\/\\\\\\\\" "____"`
    touch ran.$tr_ftnam
  done
}

cleanup() {
  stopProcess Driver $PWD
  cd $BASEDIR 
  
  stopAll all
  LOG "Waiting for all test processes to exit."
  numHosts=0
  for hst in $UHOSTS
  do
    ((cnt++))
  done
  sleepTime=20
  ((sleeptime+=$numHosts/10))
  sleep $sleepTime ## give stop commands on hosts a chance to complete
  
  ## Unprovision hosts
  teardownHosts
  
  doSummary
  
  stopWindowsDialogKiller # NOOP if not on cygwin
  
  if [ $SA_RUNNING -eq 0 ] ## If we started it, we stop it
  then
    ## Stop ssh-agent
    eval `ssh-agent -k` ##> /dev/null
  fi
  
  dirs="`ls -1d build_[CSL]* 2> /dev/null`"
  for nam in $dirs
  do
    rm -rf $nam
  done
  
  timeBreakDown $TSTIME
  rm -f latest.prop
  LOG "Test run used: $TIMEBREAKDOWN"
  LOG "Test results are in $currdir"
  LOG "Test run complete."
  
  ( sleep 200 ; rm -rf buildfwk ) &
}

## Used only by timeBreakDown
buildResult() {
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
    0) ;;
    1) BUILDRESULT="${BUILDRESULT}$sep $1 ${2}" ;;
    *) BUILDRESULT="${BUILDRESULT}$sep $1 ${2}s" ;;
  esac
}

## Convert a number of seconds into a string giving days, hours, minutes, seconds
timeBreakDown() {
  end=$SECONDS
  ((tot=$end-$1))
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

#### Convert a time string, as used in test xml, to a number of seconds
##toSeconds() {
##  resultSeconds=`echo $1 | ${AWK:-awk} ' \
##  function toSeconds( val, chr ) { \
##    if ( chr == "m" ) \
##      return val * 60; \
##    if ( chr == "h" ) \
##      return val * 3600; \
##    return val; \
##  } \
##  { \
##    match( tolower( $0 ), "(^[[:digit:]]*)([[:alpha:]]*)([[:digit:]]*)([[:alpha:]]*)([[:digit:]]*)([[:alpha:]]*)", darr ); \
##    seconds = toSeconds( darr[1], darr[2] ); \
##    seconds += toSeconds( darr[3], darr[4] ); \
##    seconds += toSeconds( darr[5], darr[6] ); \
##    print seconds; \
##  }'`
##}

windowsDialogKiller() {
  # This function is launched into the background and detects+kills the process that Windows creates when it has detected a crash.
  parent=$1 #PID of the parent process, script exits if no longer running
  sleepInterval=300

  #Run as long as the parent is alive
  while ( /bin/kill -s0 $parent > /dev/null 2>&1 )
  do
    sleep $sleepInterval 
    pid=`ps -W | ${AWK:-awk} '/dwwin.exe/{print $1}'`
    if [ ! -z "$pid" ]
    then
      #Found a Windows error dialog, killing it to prevent hang
      WARN "Found an instance of dwwin.exe, Driver.exe has likely crashed"
      if ( /bin/kill -f $pid )
      then
        WARN "Dialog: $pid killed."
      else
        ERROR "Failed to kill dialog: $pid"
      fi
    fi 
  done 
}

stopWindowsDialogKiller() {
  if [ ! -z "$windowsDialogKillerPid" ]
  then
    # We also have to kill the sleep process 
    sleepPid=`ps -ef | grep $windowsDialogKillerPid | ${AWK:-awk} '/sleep/{print $2}'`
    LOG "Shutting down windowsDialogKiller pid=$windowsDialogKillerPid"
    /bin/kill -f $windowsDialogKillerPid
    /bin/kill -f $sleepPid
  fi
  if [ -f windowsDialogKiller.log ]
  then
    LOGCONTENT windowsDialogKiller.log
    rm -f windowsDialogKiller.log
  fi
}
setJavaHome() {
# setup GF_JAVA
case $myOS in
  ( "SUN" ) myBits=64 ;;
  ( * )
   if [ $HOSTTYPE != `basename $HOSTTYPE 64` ]
    then
      myBits=64
    else
      myBits=32
    fi
  ;;
  esac
  defaultjava=GF_JAVA
  var_name=GF_JAVA_${myOS}_${myBits}
  gfjava_name=${!var_name:-${!defaultjava}}
  if type -p cygpath >/dev/null 2>/dev/null; then
   GFE_DIR="`cygpath -m "$GFE_DIR"`"
   gfjava_name="`cygpath -m "$gfjava_name"`"
  fi
  export GEMFIRE=$GFE_DIR
  export JAVA_HOME=${gfjava_name}
  export JTESTS=$GFE_DIR/../tests/classes
  echo "GEMFIRE = $GEMFIRE JAVA_HOME=$JAVA_HOME JTESTS=$JTESTS"
}
