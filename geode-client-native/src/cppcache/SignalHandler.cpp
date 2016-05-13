/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"

#include "SignalHandler.hpp"
#include "ExceptionTypes.hpp"

#include <ace/ACE.h>
#include <ace/OS_NS_unistd.h>
#include <string>


namespace
{
  // crash dump related properties
  bool g_crashDumpEnabled = true;
  std::string g_crashDumpLocation;
  std::string g_crashDumpPrefix = "gemfire_nativeclient";
}

namespace gemfire {

int SignalHandler::s_waitSeconds = 345600; // 4 days
#ifdef _WIN32
  void * SignalHandler::s_pOldHandler = NULL;
#endif

void SignalHandler::waitForDebugger( )
{
  const char dbg[128] =    "***         WAITING FOR DEBUGGER          ***\n";
  const char stars[128] =  "*********************************************\n";

  fprintf(stdout, "%s\n%s\n%s\n   Waiting %d seconds for debugger in Process Id %d\n",
      stars, dbg, stars, s_waitSeconds, ACE_OS::getpid());
  fflush( stdout );
//  int done = 0;
  int max = s_waitSeconds / 10;
//  while( ! done ) {
  while( max-- > 0 ) {
    ACE_OS::sleep( 10 );
  }
  fprintf( stdout, "%s   Waited %d seconds for debugger, Process Id %d exiting.\n%s",
    stars, s_waitSeconds, ACE_OS::getpid(), stars );
  fflush( stdout );
  exit( -1 );
}

void SignalHandler::init(bool crashDumpEnabled, const char* crashDumpLocation,
    const char* crashDumpPrefix)
{
  g_crashDumpEnabled = crashDumpEnabled;
  if (crashDumpLocation != NULL) {
    g_crashDumpLocation = crashDumpLocation;
  }
  g_crashDumpPrefix = crashDumpPrefix;
  if (!g_crashDumpEnabled) {
    SignalHandler::removeBacktraceHandler();
  }
}

bool SignalHandler::getCrashDumpEnabled()
{
  return g_crashDumpEnabled;
}

const char* SignalHandler::getCrashDumpLocation()
{
  return g_crashDumpLocation.c_str();
}

const char* SignalHandler::getCrashDumpPrefix()
{
  return g_crashDumpPrefix.c_str();
}


// Implemented in Log.cpp
// [sumedh] below is unused; Log has its own formatLogLine member method
//std::string formatLogLine(Log::LogLevel level);

}

#ifdef _WIN32
  #include "impl/WindowsSignalHandler.hpp"
#else
  #include "impl/UnixSignalHandler.hpp"
#endif
