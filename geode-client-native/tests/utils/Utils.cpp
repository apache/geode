/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

/**
* @file    Utils.cpp
* @since   1.0
* @version 1.0
* @see
*
*/

// ----------------------------------------------------------------------------

#include <GemfireCppCache.hpp>
#include <gfcpp_globals.hpp>

#include "Utils.hpp"

#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/GsRandom.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/TimeLimit.hpp"
#include "fwklib/Timer.hpp"

#include "fwklib/FwkExport.hpp"

#include "security/CredentialGenerator.hpp"


#include <ace/ACE.h>
#include <ace/OS.h>

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::security;


#ifndef _WIN32
#endif

#ifdef _WIN32

#define popen _popen
#define pclose _pclose
#define MODE "wt"

#else // linux, et. al.

#include <unistd.h>
#define MODE "w"

#endif // WIN32

static Utils * g_test = NULL;

// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing Utils library." );
    try {
      g_test = new Utils( initArgs );
    } catch( const FwkException &ex ) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing Utils library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

void Utils::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();
    bool statEnable = getBoolValue( "statEnable" );
    if(!statEnable){
      pp->insert("statistic-sampling-enabled", "false");
    }
    cacheInitialize( pp );

    // Utils specific initialization
    // none
  }
}

std::string Utils::getServerSecurityParams(std::string arguments) {

  std::string securityParams = g_test->getStringValue("securityParams");
  bool multiUserMode=g_test->getBoolValue("multiUserMode");

  // no security means security param is not applicable
  if( securityParams.empty() ) {
    return "";
  }

  FWKDEBUG("security is : " << securityParams);

  std::string bb ( "GFE_BB" );
  std::string key( "securityScheme" );
  std::string lastsc = bbGetString(bb,key);
  std::string sc = "";

  //take the next scheme only once, startJavaServers might be multiple times.
  //relying on the argument which marks the instance.
  if( arguments.empty() || ACE_OS::atoi(arguments.c_str()) == 1 ) {

    //skip until current scheme is lastsc.
    resetValue(key.c_str());
    while(sc != lastsc) {
      sc = g_test->getStringValue( key.c_str() );
    }

    while(sc == lastsc || sc.empty()) {
      sc = g_test->getStringValue( key.c_str() );
      if( sc.empty() ) {
        resetValue(key.c_str());
        sc = lastsc;
        continue;
      }
    }

    if( !sc.empty() ) {
      bbSet(bb, key, sc);
    }
  } else if( !arguments.empty() ) {
    while( sc.empty() ) {
      FWKINFO("waiting for scheme to arrive in " << bb);
      sc = bbGetString(bb,key);
      perf::sleepSeconds( 1 );
    }
  }

  FWKINFO("security scheme : " << sc);
  CredentialGeneratorPtr cg = CredentialGenerator::create( sc );

  return cg->getServerCmdParams(securityParams,"",multiUserMode);
//  return std::string("com.gemstone.gemfire.internal.security.DummyAuthenticator.create");
}
//-----------------------------------------------------------------------------
std::string Utils::getKeystoreFile() {
	char *tempPath = ACE_OS::getenv("BUILDDIR");
	std::string path = std::string(tempPath) + "/framework/data";
    char pubfile[1000] = { '\0' };
	sprintf(pubfile, "%s/keystore/server_keystore.jks", path.c_str());
	return std::string(pubfile);
}

std::string Utils::getTruststoreFile() {
	char *tempPath = ACE_OS::getenv("BUILDDIR");
	std::string path = std::string(tempPath) + "/framework/data";
    char pubfile[1000] = { '\0' };
	sprintf(pubfile, "%s/keystore/server_truststore.jks", path.c_str());
	return std::string(pubfile);
}
//-----------------------------------------------------------------------------
std::string Utils::getSslProperty(bool server)
{
  bool isSslEnable = g_test->getBoolValue( "sslEnable" );
  if(!isSslEnable) {
      return "";
  }
  FILE * pfil = ACE_OS::fopen( "gemfire.properties", "w+" );
  if ( pfil == ( FILE * )0 ) {
    FWKSEVERE( "Could not openfile" );
    exit( -1 );
  }
  ACE_OS::fprintf(pfil,"\nssl-enabled=true\n");
  ACE_OS::fprintf(pfil,"ssl-require-authentication=true\n");
  ACE_OS::fprintf(pfil,"mcast-port=0\n");
  ACE_OS::fprintf(pfil,"ssl-ciphers=SSL_RSA_WITH_NULL_SHA\n");
  ACE_OS::fflush( pfil );
  ACE_OS::fclose( pfil );

  std::string sslCmdStr;
  std::string keyStorePath = getKeystoreFile();
  std::string trustStorePath = getTruststoreFile();
  sslCmdStr = std::string(" -N -Djavax.net.ssl.keyStore=") + keyStorePath;
  sslCmdStr += std::string(" -N -Djavax.net.ssl.keyStorePassword=gemstone -N -Djavax.net.ssl.trustStore=")+ trustStorePath;
  sslCmdStr += std::string(" -N -Djavax.net.ssl.trustStorePassword=gemstone ");

  // no security means security param is not applicable
  if (server) {
	sslCmdStr = std::string(" -N -J-Djavax.net.ssl.keyStore=") + keyStorePath;
	sslCmdStr += std::string(" -N -J-Djavax.net.ssl.keyStorePassword=gemstone -N -J-Djavax.net.ssl.trustStore=")+ trustStorePath;
	sslCmdStr += std::string(" -N -J-Djavax.net.ssl.trustStorePassword=gemstone ");
    sslCmdStr +=std::string(" -N ssl-enabled=true -N ssl-require-authentication=true -N ssl-ciphers=SSL_RSA_WITH_NULL_SHA ");
  }
  return sslCmdStr;
}
// ----------------------------------------------------------------------------

TESTTASK doRunProcess( const char * taskId ) {
  g_test->checkTest( taskId );
  const std::string prog = g_test->getStringValue( "program" );
  if ( prog.empty() ) {
    FWKSEVERE( "doRunProcess(): program not specified." );
    return FWK_SEVERE;
  }
  std::string args = g_test->getStringValue( "arguments" );
  args = g_test->getServerSecurityParams(args) + " " + (prog == "stopJavaServers" ? "" : g_test->getSslProperty(prog == "startJavaServers")) + " " + args;

  const char * cargs = "";
  if ( !args.empty() ) {
    cargs = ( char * )args.c_str();
  }
  const std::string bgArg = g_test->getStringValue( "background" );
  int32_t backgroundWait = g_test->getIntValue( "backgroundWait" );

  if ( bgArg == "true" || bgArg == "TRUE" ) {
    if ( backgroundWait <= 0 ) {
      backgroundWait = 5;
    }
    return g_test->doRunBgProcess( prog.c_str( ), cargs, backgroundWait );
  } else {
    return g_test->doRunProcess( prog.c_str( ), cargs );
  }
}

// ----------------------------------------------------------------------------

TESTTASK doRunProcessAndSleep( const char * taskId ) {
  int32_t result;

  g_test->checkTest( taskId );
  const std::string prog = g_test->getStringValue( "program" );
  if ( prog.empty() ) {
    FWKSEVERE( "doRunProcessAndSleep(): program not specified." );
    return FWK_SEVERE;
  }
  const std::string args = g_test->getStringValue( "arguments" );
  const char * cargs = "";
  if ( !args.empty() ) {
    cargs = ( char * )args.c_str();
  }
  const std::string bgArg = g_test->getStringValue( "background" );
  int32_t backgroundWait = g_test->getIntValue( "backgroundWait" );

  if ( bgArg == "true" || bgArg == "TRUE" ) {
    if ( backgroundWait <= 0 ) {
      backgroundWait = 5;
    }
    result = g_test->doRunBgProcess( prog.c_str( ), cargs, backgroundWait );
  } else {
    result = g_test->doRunProcess( prog.c_str( ), cargs );
  }

  int32_t sleepTime = g_test->getIntValue( "sleepTime" );
  if ( sleepTime > 0 ) {
    perf::sleepSeconds( sleepTime );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doStopStartServer( const char * taskId ) {
  g_test->checkTest( taskId );

  static bool firstTimeIn = true;
  if ( firstTimeIn ) {
    g_test->bbIncrement( "Cacheservers", "ServerCount" );
    firstTimeIn = false;
  }

  const std::string op = g_test->getStringValue( "operation" );
  if ( op.empty() ) {
    FWKSEVERE( "doStopStartServer(): operation not specified." );
    return FWK_SEVERE;
  }

  const std::string id = g_test->getStringValue( "ServerId" );
  if ( id.empty() ) {
    FWKSEVERE( "doStopStartServer(): server id not specified." );
    return FWK_SEVERE;
  }

  //always use existing schemes... so getServer is hard coded with Zero.
  std::string args = g_test->getServerSecurityParams("0") + "  ";

  std::string startCmd = "startJavaServers ";
  startCmd.append( args );
  startCmd.append(g_test->getSslProperty(true));
  startCmd.append(" ");
  startCmd.append( id );
  std::string stopCmd;
  if ( op == "stop" ) {
    stopCmd = "stopJavaServers ";
    stopCmd.append( id );
  }
  else {
    stopCmd = "killJavaServers ";
    stopCmd.append( id );
    if ( op == "kill" ) {
      stopCmd.append( " 9" );
    }
  }

  int32_t sleepTime = g_test->getIntValue( "sleepTime" );
  if ( sleepTime > 0 ) {
    perf::sleepSeconds( sleepTime );
  }

  int32_t minServers = g_test->getIntValue( "minServers" );
  if ( minServers <= 0 ) minServers = 1;

  int32_t result = FWK_SUCCESS;
  bool done = false;
  while ( !done ) {
    int64_t scnt = g_test->bbDecrement( "Cacheservers", "ServerCount" );
    if ( scnt >= minServers ) {
      FWKINFO( "DEBUG CI count indicates shutdown permitted " << ( int32_t )scnt );
      //perf::sleepSeconds( 60 );
      FWKINFO( "DEBUG SS Begin shutdown" );
      result = g_test->doRunProcess( stopCmd.c_str(), " " );
      sleepTime = g_test->getIntValue( "sleepTime" );
      if ( sleepTime > 0 ) {
        perf::sleepSeconds( sleepTime );
      }
      result = g_test->doRunProcess( startCmd.c_str(), " ");
      FWKINFO( "DEBUG SS startup complete" );
      //perf::sleepSeconds( 60 );
      scnt = g_test->bbIncrement( "Cacheservers", "ServerCount" );
      FWKINFO( "DEBUG CI count indicates startup complete " << ( int32_t )scnt );
      done = true;
    }
    else {
      g_test->bbIncrement( "Cacheservers", "ServerCount" );
      sleepTime = g_test->getIntValue( "sleepTime" );
      if ( sleepTime > 0 ) {
        perf::sleepSeconds( sleepTime );
      }
    }
  }
  sleepTime = g_test->getIntValue( "sleepTime" );
  if ( sleepTime > 0 ) {
    perf::sleepSeconds( sleepTime );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK checkTimeDelta( const char * taskId ) {
  int32_t result = FWK_SUCCESS;

  g_test->checkTest( taskId );

//  int32_t tries = 31;
//
//  int32_t deltaMicros = 0;
//
//  while ( ( deltaMicros == 0 ) && ( tries-- > 0 ) ) {
//    perf::sleepSeconds( 2 );
//    deltaMicros = g_test->getDeltaMicros();
//  }
//
//  if ( ( deltaMicros > 500000 ) || ( deltaMicros < -500000 ) ) {
//    result = FWK_SEVERE;
//    FWKSEVERE( "Time delta between this machine and the Driver machine is too large." );
//  }
//
//  FWKINFO( "Time delta between this machine and the Driver machine is " << deltaMicros << " microseconds." );
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK checkTimeResolution( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  uint64_t diff = 0;
  const uint64_t max = 4000000;
  int32_t underflows = 0, overflows = 0;
  const uint32_t LIMIT = 20000;
  int32_t counts[LIMIT];

  memset( counts, 0, sizeof( counts ) );

  FWKINFO( "Beginning Time Resolution Check." );
  HRTimer tot, inter;
  tot.start();
  inter.start();

  while ( diff < max ) {
    diff = inter.elapsedMicros();
    inter.start();
    if ( diff < 0 ) {
      underflows += 1;
    }
    else {
      if ( diff > LIMIT ) {
        overflows += 1;
      }
      else {
        counts[diff] += 1;
      }
    }
    diff = tot.elapsedMicros();
  }

  if ( underflows > 0 ) {
    FWKINFO( "TimeRes Result: " << underflows << " intervals took less than 0 microseconds." );
  }
  if ( overflows > 0 ) {
    FWKINFO( "TimeRes Result: " << overflows << " intervals took more than " << LIMIT << " microseconds." );
  }

  uint64_t total = 0, low = 0, high = 0;
  for ( diff = 0; diff < LIMIT; diff++ ) {
    if ( counts[diff] > 0 ) {
      total += counts[diff];
      if ( diff < 10 ) low += counts[diff];
      if ( diff > 9000 ) high += counts[diff];
      FWKINFO( "TimeRes Result: " << counts[diff] << " intervals took " << diff << " microseconds." );
    }
  }

  uint64_t lowLim = ( total * 3 ) / 4;
  uint64_t highLim = total / 10;

  if ( ( low < lowLim ) || ( high > highLim ) ) {
    result = FWK_SEVERE;
    FWKSEVERE( "Time resolution on this machine may be too coarse for tests that time small intervals." );
  }

  FWKINFO( "Time Resolution Check complete." );
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doRandomStartStopServer( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  const std::string prog = g_test->getStringValue( "program" );
  if ( prog.empty() ) {
    FWKSEVERE( "doRandomStartStopServer(): program not specified." );
    return FWK_SEVERE;
  }
  const std::string args = g_test->getStringValue( "arguments" );
  char * cargs = NULL;
  if ( !args.empty() ) {
    cargs = ( char * )args.c_str();
  }
  int32_t secondsToRun = g_test->getTimeValue( "workTime" );
  secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;
  int32_t numServer = g_test->getIntValue( "numberOfServer" );
  numServer = ( secondsToRun < 1 ) ? 1 : numServer;
  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;
  char buf[2048];
  std::vector<char *> serverArguments;
  if (cargs != NULL)
  {
    cargs = strdup(cargs);
    char *token = strtok(cargs, " ");
    while(token)
    {
      serverArguments.push_back(token);
      token = strtok(NULL, " ");
    }
  }
  int32_t serverNum = 0;
  while ( now < end ) {
    uint32_t num;
    do {
      num = GsRandom::random(numServer) + 1;
    } while ( serverNum == (int32_t)num );
    sprintf( buf, "%s %s stop %s %d", serverArguments[0], serverArguments[1], serverArguments[3], (int32_t)num);
    result = g_test->doRunProcess(prog.c_str(), buf);
    perf::sleepSeconds(10);
    if (result != FWK_SUCCESS) {
      return result;
    }
    if(serverNum != 0 ) {
      sprintf( buf, "%s  %s start %s %s %d %d", serverArguments[0], serverArguments[1], serverArguments[2],serverArguments[3], 1, serverNum);
      result = g_test->doRunProcess(prog.c_str(), buf);
      perf::sleepSeconds(10);
      if (result != FWK_SUCCESS) {
        return result;
      }
    }
    serverNum = num;
    now = ACE_OS::gettimeofday();
  }
  return result;
}
//-----------------------------------------------------------------------------
TESTTASK doRunMultipleProcesses( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );

  std::vector<std::string> progs, args;
  std::string prog = g_test->getStringValue( "program" );
  while (!prog.empty()) {
    progs.push_back(prog);
    prog = g_test->getStringValue( "program" );
  }

  std::string arg = g_test->getStringValue( "arguments" );
  while (!arg.empty()) {
    args.push_back(arg);
    arg = g_test->getStringValue( "arguments" );
  }

  std::vector<int32_t> sleepTimes;
  int32_t sleepTime = g_test->getIntValue( "sleepTime" );
  while (sleepTime > 0) {
    sleepTimes.push_back(sleepTime);
    sleepTime = g_test->getIntValue( "sleepTime" );
  }

  int32_t numProgs = (int32_t)progs.size();
  for (int32_t i = 0; i < numProgs; ++i) {
    if (!sleepTimes.empty()) {
      perf::sleepSeconds( sleepTimes[i % sleepTimes.size()]);
    }
    const char * cargs = args.empty() ? "" :args[i % args.size()].c_str();
    result = g_test->doRunProcess(progs[i].c_str(), cargs);
    if (result != FWK_SUCCESS) {
      return result;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doSocketCount( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  perf::sleepSeconds( 10 );
  int32_t pid = ACE_OS::getpid();
  char buf[128];
  sprintf( buf, "bash -c \"ls -l /proc/%d | grep socket | wc -l\"", pid );
  FILE * pip = popen( buf, "r" );
  if ( pip == NULL ) {
    FWKSEVERE( "doSocketCount(): unable to count sockets." );
    result = FWK_SEVERE;
  }
  else {
    fgets( buf, 128, pip );
    int32_t cnt = ACE_OS::atoi( buf );
    pclose( pip );

    FWKINFO( "doSocketCount():  Socket count of Process " << pid <<
      ": " << cnt << " sockets." );
    FWKINFO( ":CSV Socket::::," << pid << "," << cnt << "," << 0 );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doThreadCount( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  const char * cmdf = "gdbcmd";
  FILE * gfil = fopen( cmdf, "wb" );
  if ( gfil == NULL ) {
    FWKSEVERE( "doThreadCount(): unable to write gdbcmd file." );
    result = FWK_SEVERE;
    return result;
  }
  fprintf( gfil, "info threads\n" );
  fclose( gfil );

  int32_t pid = ACE_OS::getpid();
  char buf[128];
  sprintf( buf, "bash -c \"gdb -batch -x %s -p %d | egrep -e '^[ 0-9]*Thread ' | wc -l\"", cmdf, pid );
  FILE * pip = popen(buf,"r");
  if ( pip == NULL ) {
    FWKSEVERE( "doThreadCount(): unable to count threads." );
    result = FWK_SEVERE;
  }
  else {
    fgets( buf, 128, pip );
    int32_t cnt = ACE_OS::atoi( buf );
    pclose( pip );

    FWKINFO( "doThreadCount():  Thread count of Process " << pid <<
      ": " << cnt << " threads." );
    FWKINFO( ":CSV Thread::::," << pid << "," << cnt << "," << 0 );
  }
  unlink( cmdf );
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doShowEndPoints( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  std::string bb ( "GFE_BB" );
  std::string key( "EndPoints" );
  std::string epts = g_test->bbGetString( bb, key );
  if ( ! epts.empty() ) {
    FWKINFO( "EndPoints are: " << epts );
  }
  else {
    FWKSEVERE( "EndPoints are not set in BB." );
    result = FWK_SEVERE;
  }

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doSizeCheck( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  perf::logSize();
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doPeriodicSizeCheck( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );

  int32_t runSeconds = g_test->getTimeValue( "workTime" );
  if ( runSeconds < 1 ) {
    runSeconds = 300;
  }

  int32_t secs = g_test->getTimeValue( "sleepTime" );
  if ( secs < 1 ) {
    secs = 30;
  }

  TimeLimit run( runSeconds );
  while ( !run.limitExceeded() ) {
    perf::sleepSeconds( secs );
    perf::logSize();
  }
  FWKINFO( "doPeriodicSizeCheck complete. " );

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doPaceCheck( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  int32_t ops = g_test->getIntValue( "operations" );
  if ( ops < 1 ) {
    ops = 1;
  }
  int32_t seconds = g_test->getTimeValue( "interval" );
  if ( seconds < 1 ) {
    seconds = 1;
  }
  int32_t runSeconds = g_test->getTimeValue( "workTime" );
  if ( runSeconds < 1 ) {
    runSeconds = 30;
  }

  PaceMeter pace( ops, seconds );
  TimeLimit run( runSeconds );
  int32_t cnt = 0;

  FWKINFO( "Op simulation starts." );
  while ( !run.limitExceeded() ) {
    FWKINFO( "Op simulated: " << ++cnt );
    pace.checkPace();
  }
  FWKINFO( "Op simulation ends. " << ( ( double )cnt / ( double )runSeconds ) );

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doBBExercise( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  int32_t cnt = g_test->getIntValue( "workIterations" );
  if ( cnt < 1 ) {
    cnt = 1000;
  }
  char * fqdn = ACE_OS::getenv( "GF_FQDN" );
  char buf[64];
  sprintf( buf, "%s:P%d:T%u", fqdn, ACE_OS::getpid(), ( uint32_t )( ACE_Thread::self() ) );
  int32_t hb = cnt / 10;
  std::string junk( "Random junk::" );
  for ( int32_t j = 0; j < 100; j++ ) junk += "More junk to make things big. ";
  FWKINFO( "Begin BB Exercise." );
  int64_t eval = 0;
  for ( int32_t cur = 0; cur < cnt; cur++ ) {
    int64_t cval = g_test->bbIncrement( "BBExercise", buf );
    eval++;
    if ( eval != cval ) {
      FWKSEVERE( "Expected value " << ( int32_t )eval << ", found value " << ( int32_t )cval );
    }
    g_test->bbSet( "BBExerciseBig", "BigValueKey", junk );
    if ( ( hb > 0 ) && ( ( cur % hb ) == 0 ) ) {
      FWKINFO( "Current val for " << buf << " is " << ( int32_t )cval );
    }
  }
  FWKINFO( "BB Exercise complete." );
  int32_t val = ( int32_t )g_test->bbGet( "BBExercise", buf );
  if ( val != cnt ) {
    FWKSEVERE( "Count does not match BB, " << cnt << " != " << val );
    result = FWK_SEVERE;
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doSleep( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest( taskId );
  int32_t secs = g_test->getTimeValue( "sleepTime" );
  if ( secs < 1 ) {
    secs = 30;
  }
  FWKINFO( "doSleep called for task: " << taskId <<
    " sleeping for " << secs << " seconds." );
  g_test->bbAdd( CLIENTBB, "SleepTimeUsed", secs );
  perf::sleepSeconds( secs );
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doClearSnapshot( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doClearSnapshot called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doClearSnapshot();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doClearSnapshot caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doPostSnapshot( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPostSnapshot called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doPostSnapshot();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPostSnapshot caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doValidateSnapshot( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateSnapshot called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doValidateSnapshot();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateSnapshot caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doRandom( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  int32_t range = 100;
  int32_t arr[101];
  g_test->checkTest( taskId );

  for ( int32_t i = 0; i < 102; i++ ) {
    arr[ i ] = 0;
  }

  for ( int32_t j = 0; j < 1000000; j++ ) {
    int32_t val = GsRandom::random( range );
    arr[ val ]++;
  }

  int32_t tot = 0;
  int32_t min = 1000000;
  int32_t max = 0;

  for ( int32_t i = 0; i < 102; i++ ) {
    FWKINFO( i << " : " << arr[i] );
    if ( ( arr[i] > 0 ) && ( min > arr[i] ) ) {
      min = arr[i];
    }
    if ( max < arr[i] ) {
      max = arr[i];
    }
    tot += arr[i];
  }
  FWKINFO( "Total: " << tot );
  FWKINFO( "  Min: " << min );
  FWKINFO( "  Max: " << max );
  FWKINFO( "  Avg: " << tot / range );


  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doFeed( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doFeed called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    g_test->doFeedPuts();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doFeed caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doEntryOperations( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doEntryOperations called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->doEntryOperations();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doEntryOperations caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

RegionPtr Utils::getRegion( const char * rname ) {
  RegionPtr regionPtr;
  if (m_cache == NULLPTR) {
    FWKEXCEPTION( "Utils::getRegion: Cache is NULL." );
  }
  std::string name;
  if ( rname != NULL ) {
    name = rname;
  }
  else {
    name = getStringValue( "regionName" );
  }
  if ( name.empty() ) {
    VectorOfRegion regVec;
    m_cache->rootRegions( regVec );
    int32_t siz = regVec.size();
    if ( siz == 0 ) {
      FWKEXCEPTION( "Utils::getRegion: No regions in cache." );
    }
    regionPtr = regVec.at( 0 );
  }
  else {
    regionPtr = m_cache->getRegion( name.c_str() );
  }
  if (regionPtr == NULLPTR) {
    FWKEXCEPTION( "Utils::getRegion: No region to operate on." );
  }
  return regionPtr;
}

// ----------------------------------------------------------------------------

CacheablePtr Utils::nextValue() {
  FWKINFO( "nextValue called." );

  CacheablePtr value;
  std::string valType = getStringValue( "valueType" );
  if ( valType.empty() ) valType = "CacheableBytes";
  if ( valType == "CacheableBytes" ) {
    int32_t valSize = getIntValue( "valueSizes" );
    valSize = ( ( valSize < 0) ? 32 : valSize );
    std::string val = GsRandom::getAlphanumericString( valSize - 1 );
    value = CacheableBytes::create( ( uint8_t *)val.c_str(), static_cast<int32_t>(val.length()) );
  }
  else if ( valType == "CacheableString" ) {
    int32_t valSize = getIntValue( "valueSizes" );
    valSize = ( ( valSize < 0) ? 32 : valSize );
    std::string val = GsRandom::getAlphanumericString( valSize - 1 );
    value = CacheableString::create( val.c_str() );
  }
  else {
    FWKEXCEPTION( "Unsupported value type " << valType << " requested." )
  }
  return value;
}

// ----------------------------------------------------------------------------

CacheableKeyPtr Utils::nextKey( int32_t cnt ) {
  FWKINFO( "nextKey called." );

  bool ownKeys = getBoolValue( "ownKeys" );
  int32_t entryCount = getIntValue( "entryCount" );
  if ( entryCount < 1 ) entryCount = 100;
  int32_t id = getClientId();

  CacheableKeyPtr key;
  std::string keyType = getStringValue( "keyType" );
  if ( keyType.empty() ) keyType = "CacheableString";
  if ( keyType == "CacheableString" ) {
    int32_t keySize = getIntValue( "keySizes" );
    keySize = ( ( keySize < 0) ? 10 : keySize );
    std::string kstr = GsRandom::getAlphanumericString( keySize - 1 );
    if ( ownKeys ) {
      char buff[32];
      sprintf( buff, "%u", id );
      int32_t idx = 0;
      while ( buff[ idx ] != 0 ) {
        kstr.at( idx ) = buff[ idx ];
        idx++;
      }
    }
    key = CacheableString::create( kstr.c_str() );
  }
  else if ( keyType == "CacheableInt32" ) {
    uint32_t lowEnd = 0;
    uint32_t highEnd = entryCount;
    uint32_t kval = ( cnt > -1 ) ? cnt : 0;
    uint32_t offset = 0;

    if ( ownKeys ) {
      offset = id * entryCount;
      lowEnd += offset;
      highEnd += offset;
    }

    if ( kval > 0 ) {
      kval += offset;
    }
    else {
      kval = GsRandom::random( lowEnd, highEnd );
    }
    key = CacheableInt32::create( kval );
    FWKINFO( "Creating key " << kval << "  cnt is " << cnt << " id is " << id << "  offset is " << offset << " low: " << lowEnd << "  high: " << highEnd );
  }
  else {
    FWKEXCEPTION( "Unsupported key type " << keyType << " requested." )
  }
  return key;
}

// ----------------------------------------------------------------------------

CacheableKeyPtr Utils::getKeyNotInCache() {
  RegionPtr regionPtr = getRegion();

  CacheableKeyPtr key;
  int32_t tries = 5;
  while ( tries-- > 0 ) {
    key = nextKey();
    if ( regionPtr->containsKey( key ) ) {
      if ( tries == 0 ) {
        regionPtr->localDestroy( key );
      }
    }
    else { tries = 0;
    }
  }
  return key;
}

// ----------------------------------------------------------------------------

CacheableKeyPtr Utils::getKeyInCache() {
  RegionPtr regionPtr = getRegion();

  VectorOfCacheableKey keyVec;
  regionPtr->keys( keyVec );
  uint32_t kcnt = keyVec.size();
  if ( kcnt == 0 ) {
    FWKEXCEPTION( "Utils::getKeyInCache(): No keys in region." );
  }
  uint32_t idx = GsRandom::random( kcnt );
  return keyVec.at( idx );
}

// ----------------------------------------------------------------------------

void Utils::add( CacheableKeyPtr key, CacheablePtr value ) {
  RegionPtr regionPtr = getRegion();

  try {
    regionPtr->create( key, value );
  } catch ( const EntryExistsException & e ) {
    FWKINFO( "Utils::add() caught: " << e.getMessage() );
  } catch ( const Exception & e ) {
    FWKSEVERE( "Utils::add() caught: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void Utils::add() {
  RegionPtr regionPtr = getRegion();

  try {
    CacheableKeyPtr key = getKeyNotInCache();
    CacheablePtr value = nextValue();
    regionPtr->create( key, value );
  } catch ( const EntryExistsException & e ) {
    FWKINFO( "Utils::add() caught: " << e.getMessage() );
  } catch ( const Exception & e ) {
    FWKSEVERE( "Utils::add() caught: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void Utils::read() {
  RegionPtr regionPtr = getRegion();

  try {
    CacheableKeyPtr key = getKeyInCache();
    CacheablePtr value = regionPtr->get( key );
    if (value == NULLPTR) {
      FWKWARN( "get returned a null value from cache." );
    }
  } catch ( const Exception & e ) {
    FWKSEVERE( "Utils::update() caught: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void Utils::update() {
  RegionPtr regionPtr = getRegion();

  try {
    CacheableKeyPtr key = getKeyInCache();
    CacheablePtr value = nextValue();
    regionPtr->put( key, value );
  } catch ( const Exception & e ) {
    FWKSEVERE( "Utils::update() caught: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void Utils::invalidate( bool local ) {
  RegionPtr regionPtr = getRegion();

  try {
    CacheableKeyPtr key = getKeyInCache();
    if ( local ) {
      regionPtr->localInvalidate( key );
    }
    else {
      regionPtr->invalidate( key );
    }
  } catch ( const Exception & e ) {
    FWKSEVERE( "Utils::update() caught: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void Utils::destroy( bool local ) {
  RegionPtr regionPtr = getRegion();

  try {
    CacheableKeyPtr key = getKeyInCache();
    if ( local ) {
      regionPtr->localDestroy( key );
    }
    else {
      regionPtr->destroy( key );
    }
  } catch ( const Exception & e ) {
    FWKSEVERE( "Utils::destroy() caught: " << e.getMessage() );
  }
}

// ----------------------------------------------------------------------------

void Utils::doFeedPuts() {
  FWKINFO( "doFeedPuts called." );

  int32_t opsSec = getIntValue( "opsSecond" );
  if ( opsSec < 1 ) opsSec = 0;

  int32_t entryCount = getIntValue( "entryCount" );
  if ( entryCount < 1 ) entryCount = 100;

  int32_t cnt = 0;

  PaceMeter meter( opsSec );
  while ( cnt < entryCount ) {
    add( nextKey( ++cnt ), nextValue() );
    meter.checkPace();
  }
}

// ----------------------------------------------------------------------------

int32_t Utils::doEntryOperations() {
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doEntryOperations called." );

  int32_t opsSec = getIntValue( "opsSecond" );
  if ( opsSec < 1 ) opsSec = 0;

  int32_t entryCount = getIntValue( "entryCount" );
  if ( entryCount < 1 ) entryCount = 100;

  int32_t secondsToRun = getTimeValue( "workTime" );
  if ( secondsToRun < 1 ) secondsToRun = 30;

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
  ACE_Time_Value now;

  int32_t creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, localDests = 0, localInvals = 0;

  FWKINFO( "doEntryOperations will work for " << FwkStrCvt::toTimeString( secondsToRun ) << "." );

  PaceMeter meter( opsSec );
  while ( now < end ) {
    std::string opcode;
    try {
      opcode = getStringValue( "entryOps" );
      if ( opcode.empty() ) opcode = "no-op";
      FWKINFO( "doEntryOperations(): opcode is " << opcode );

      if ( opcode == "add" ) {
        add();
        creates++;
      }
      else if ( opcode == "update" ) {
        update();
        puts++;
      }
      else if ( opcode == "invalidate" ) {
        invalidate();
        invals++;
      }
      else if ( opcode == "local-invalidate" ) {
        invalidate( true );
        localInvals++;
      }
      else if ( opcode == "destroy" ) {
        destroy();
        dests++;
      }
      else if ( opcode == "local-destroy" ) {
        destroy( true );
        localDests++;
      }
      else if ( opcode == "read" ) {
        read();
        gets++;
      }
      else {
        FWKSEVERE( "Invalid operation specified: " << opcode );
      }
    } catch ( TimeoutException &e ) {
      fwkResult = FWK_SEVERE;
      FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
        << " operation: " << e.getMessage() << " continuing with test." );
    } catch ( EntryExistsException & ) {
    } catch ( EntryNotFoundException &ignore ) { ignore.getMessage();
    } catch ( EntryDestroyedException &ignore ) { ignore.getMessage();
    } catch ( Exception &e ) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKSEVERE( "Caught unexpected exception during entry " << opcode
        << " operation: " << e.getMessage() << " exiting task." );
    }
    meter.checkPace();
    now = ACE_OS::gettimeofday();
  }

  FWKINFO( "doEntryOperations did " << creates << " creates, "
            << puts << " puts, " << gets << " gets, "
            << invals << " invalidates, " << localInvals << " local invalidates, "
            << dests << " destroys, " << localDests << " local destroys.");
  return fwkResult;
}

// ----------------------------------------------------------------------------

uint8_t * Utils::hexify( const uint8_t* buff, int32_t len ) {
  uint8_t * obuff = ( uint8_t * )malloc( ( len + 1 ) * 2 );
  for ( int32_t i = 0; i < len; i++ ) {
    sprintf( ( char * )( obuff + ( i * 2 ) ), "%02x", buff[i] );
  }
  return obuff;
}

// ----------------------------------------------------------------------------

int32_t Utils::doClearSnapshot() {
  int32_t result = FWK_SUCCESS;

  RegionPtr regionPtr = getRegion();

  std::string bbName = regionPtr->getName();

  std::string tag = getStringValue( "tag" );
  bbName += tag;

  FWKINFO( "Clear BB Name: " << bbName  );
  bbClear( bbName );

  return result;
}

// ----------------------------------------------------------------------------

int32_t Utils::doPostSnapshot() {
  int32_t result = FWK_SUCCESS;

  RegionPtr regionPtr = getRegion();

  std::string bbName = regionPtr->getName();

  VectorOfCacheableKey keyVec;
  regionPtr->keys( keyVec );
  int32_t kcnt = keyVec.size();

  std::string tag = getStringValue( "tag" );
  bbName += tag;

  FWKINFO( "BB Name: " << bbName << "BB Key: COUNT  BB Value: " << kcnt );
  bbSet( bbName, "COUNT", ( int64_t )kcnt );

  DataOutput keyDo;
  DataOutput valDo;

  for ( int32_t i = 0; i < kcnt; i++ ) {
    keyDo.reset();
    valDo.reset();

    keyVec.at( i )->toData( keyDo );
    if ( regionPtr->containsValueForKey( keyVec.at( i ) ) ) {
      CacheablePtr val = regionPtr->get( keyVec.at( i ) );
      val->toData( valDo );
      uint8_t * kHex = hexify( keyDo.getBuffer() , keyDo.getBufferLength() );
      uint8_t * vHex = hexify( valDo.getBuffer() , valDo.getBufferLength() );
      FWKINFO( "BB Name: " << bbName << "BB Key: " << ( char * )kHex << "  BB Value: " << ( char * )vHex );
      bbSet( bbName, ( char * )kHex, ( char * )vHex );
    }
    else {
      uint8_t * kHex = hexify( keyDo.getBuffer() , keyDo.getBufferLength() );
      FWKINFO( "BB Name: " << bbName << "BB Key: " << ( char * )kHex << "  BB Value: " << "NO_VALUE" );
      bbSet( bbName, ( char * )kHex, "NO_VALUE" );
    }
  }

  return result;
}

// ----------------------------------------------------------------------------

int32_t Utils::doValidateSnapshot() {
  int32_t result = FWK_SUCCESS;

  RegionPtr regionPtr = getRegion();

  std::string bbName = regionPtr->getName();

  VectorOfCacheableKey keyVec;
  regionPtr->keys( keyVec );
  int32_t kcnt = keyVec.size();

  FWKINFO( "Region: " << bbName << "Key Count: " << kcnt );

  std::string tag = getStringValue( "tag" );
  bbName += tag;

  int64_t pcnt = bbGet( bbName, "COUNT" );
  FWKINFO( "BB Name: " << bbName << "BB Key: COUNT  BB Value: " << pcnt );

  if ( ( int64_t )kcnt != pcnt ) {
    FWKSEVERE( "Region key count of " << kcnt << " does not match posted count of " << ( int32_t )pcnt );
    result = FWK_SEVERE;
  }

  DataOutput keyDo;
  DataOutput valDo;

  for ( int32_t idx = 0; idx < kcnt; idx++ ) {
    keyDo.reset();
    valDo.reset();

    keyVec.at( idx )->toData( keyDo );
    if ( regionPtr->containsValueForKey( keyVec.at( idx ) ) ) {
      CacheablePtr val = regionPtr->get( keyVec.at( idx ) );
      val->toData( valDo );
      uint8_t * kHex = hexify( keyDo.getBuffer() , keyDo.getBufferLength() );
      uint8_t * vHex = hexify( valDo.getBuffer() , valDo.getBufferLength() );
      std::string bbVal = bbGetString( bbName, ( char * )kHex );
      if ( memcmp( ( void * )bbVal.c_str(), vHex, valDo.getBufferLength() * 2 ) != 0 ) {
        FWKINFO( "   BB Content: " << bbName << "Key: " << ( char * )kHex << "  Value: " << bbVal );
        FWKINFO( "Cache Content: " << bbName << "Key: " << ( char * )kHex << "  Value: " << ( char * )vHex );
        FWKSEVERE( "NOT EQUAL" );
        result = FWK_SEVERE;
      }
    }
    else {
      uint8_t * kHex = hexify( keyDo.getBuffer() , keyDo.getBufferLength() );
      std::string bbVal = bbGetString( bbName, ( char * )kHex );
      if ( memcmp( ( void * )bbVal.c_str(), "NO_VALUE", 8 ) != 0 ) {
        FWKINFO( "   BB Content: " << bbName << "Key: " << ( char * )kHex << "  Value: " << bbVal );
        FWKINFO( "Cache Content: " << bbName << "Key: " << ( char * )kHex << "  Value: " << "NO_VALUE" );
        FWKSEVERE( "NOT EQUAL" );
        result = FWK_SEVERE;
      }
    }
  }

  return result;
}

// ----------------------------------------------------------------------------

const uint32_t AFTIP = 0xc0000142;
const char * AFTIP_MSG = "Application failed to initialize properly (0xc0000142)";

int32_t Utils::doRunProcess(const char * prog, const char* args)
{
  int32_t result = FWK_SUCCESS;
//  int32_t secs = g_test->getTimeValue( "sleepTime" );
//  if ( secs > 0 ) {
//    FWKINFO( "Task: " << getTaskId() << " sleeping for " << secs << " seconds." );
//    perf::sleepSeconds( secs );
//  }
  int32_t waitSecs = getWaitTime();
  if ( waitSecs == 0 ) waitSecs = 345600; // a long time, 4 days
  TimeLimit patience( waitSecs );

  char cmd[2048];
  char buf[2048];
  sprintf( cmd, "bash -c \"%s  %s\"", prog, args );
  FILE * pip = popen( cmd, "r" );
  if ( pip == NULL ) {
    FWKSEVERE( "doRunProcess(): unable to run " << prog );
    result = FWK_SEVERE;
  } else {
    std::string inp;
    while ( ( fgets( buf, 2048, pip ) != NULL ) && !patience.limitExceeded() ) {
      inp.append( buf );
      perf::sleepSeconds( 1 );
    }

    uint32_t retVal = pclose( pip );
    if ( retVal != 0 ) {
      if ( retVal == AFTIP ) {
        FWKSEVERE( "Attempt to execute: " << cmd << " resulted in: " << AFTIP_MSG );
      }
      else {
        FWKSEVERE( "Nonzero result: " << retVal << " returned from: " << cmd );
      }
      result = FWK_SEVERE;
    }

    FWKINFO( "doRunProcess() Output from executing " << cmd
      << "  :\n" << inp << "\n   End of output from " << prog );
//    FWKINFO( "doRunProcess() Output from executing " << prog << "  " << args
//      << ":\n" << inp << "\n   End of output from " << prog );
  }
  return result;

}

int32_t Utils::doRunBgProcess( const char * prog, const char* args, int32_t waitSeconds )
{
  int32_t result = FWK_SUCCESS;
  char cmd[2048];
  sprintf( cmd, "bash -c \"%s  %s > /dev/null 2>/dev/null\"", prog, args );
  FILE * pip = popen( cmd, "r" );
  if ( pip == NULL ) {
    FWKSEVERE( "doRunBgProcess(): unable to run " << prog );
    result = FWK_SEVERE;
  } else {
    perf::sleepSeconds( waitSeconds );
    /*
    if ( retVal != 0 ) {
      if ( retVal == AFTIP ) {
        FWKSEVERE( "Attempt to execute: " << cmd << " resulted in: " << AFTIP_MSG );
      }
      else {
        FWKSEVERE( "Nonzero result: " << retVal << " returned from: " << cmd );
      }
      result = FWK_SEVERE;
    }
    */
  }
  return result;
}

// ----------------------------------------------------------------------------

