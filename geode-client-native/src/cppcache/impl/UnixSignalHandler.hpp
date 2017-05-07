/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <signal.h>
#include <memory.h>
#include <pthread.h>
#include <stdlib.h>
#include <limits.h>

#include "StackTrace.hpp"
#include "Utils.hpp"
#include "AutoDelete.hpp"

namespace gemfire {

extern "C" void backtraceHandler(int);

void SignalHandler::installBacktraceHandler( )
{
  std::string waitSeconds = Utils::getEnv( "GF_DEBUG_WAIT" );
  if ( ! waitSeconds.empty( ) ) {
    int waitS = atoi( waitSeconds.c_str() );
    if ( waitS > 0 ) s_waitSeconds = waitS;
  }

  struct sigaction sa;

  sa.sa_handler = gemfire::backtraceHandler;
  sigemptyset(&(sa.sa_mask));
  sa.sa_flags = SA_RESTART;

  /* make sure we don't recur */
  sigaddset( &(sa.sa_mask), SIGBUS );
  sigaddset( &(sa.sa_mask), SIGSEGV );
  sigaddset( &(sa.sa_mask), SIGABRT );
  sigaddset( &(sa.sa_mask), SIGILL );
  pthread_sigmask( SIG_UNBLOCK, &(sa.sa_mask), NULL );

  sigaction(SIGBUS, &sa, NULL);
  sigaction(SIGSEGV, &sa, NULL);
  sigaction(SIGABRT, &sa, NULL);
  sigaction(SIGILL, &sa, NULL);
}

void SignalHandler::removeBacktraceHandler()
{
  struct sigaction sa;

  sa.sa_handler = SIG_DFL;
  sigemptyset(&(sa.sa_mask));
  sa.sa_flags = SA_RESTART;

  sigaddset(&(sa.sa_mask), SIGBUS);
  sigaddset(&(sa.sa_mask), SIGSEGV);
  sigaddset(&(sa.sa_mask), SIGABRT);
  sigaddset(&(sa.sa_mask), SIGILL);

  sigaction(SIGBUS, &sa, NULL);
  sigaction(SIGSEGV, &sa, NULL);
  sigaction(SIGABRT, &sa, NULL);
  sigaction(SIGILL, &sa, NULL);
}

extern "C" void backtraceHandler( int sig ) {
 
  std::string msg = "";

  switch ( sig ) {
    case SIGSYS:
      msg = "SIGSYS";
      break;
    case SIGTRAP:
      msg = "SIGTRAP";
      break;
    case SIGVTALRM:
      msg = "SIGVTALRM";
      break;
    case SIGXCPU:
      msg = "SIGXCPU";
      break;
    case SIGXFSZ:
      msg = "SIGXFSZ";
      break;
#ifdef HAVE_SIGSTKFLT
    case SIGSTKFLT:
      msg = "SIGSTKFLT";
      break;
#endif
    case SIGIO:
      msg = "SIGIO";
      break;
#ifdef HAVE_SIGPWR
    case SIGPWR:
      msg = "SIGPWR";
      break;
#endif
    case SIGPROF:
      msg = "SIGPROF";
      break;
    case SIGBUS:
      msg = "SIGBUS";
      break;
    case SIGTTOU:
      msg = "SIGTTOU";
      break;
    case SIGTTIN:
      msg = "SIGTTIN";
      break;
    case SIGTSTP:
      msg = "SIGTSTP";
      break;
    // cleanup if possible, and chain...
    case SIGTERM:
      msg = "SIGTERM";
      break;
    // use preexisting behavior
    case SIGALRM:
      msg = "SIGALRM";
      break;
    // chain.
    case SIGPIPE:
      msg = "SIGPIPE";
      break;
    // idicate a crash.
    case SIGSEGV:
      msg = "SIGSEGV";
      break;
    case SIGFPE:
      msg = "SIGFPE";
      break;
    case SIGABRT:
      msg = "SIGABRT";
      break;
    case SIGILL:
      msg = "SIGILL";
      break;
    // typical user interaction... ignore or handle...
    case SIGQUIT:
      msg = "SIGQUIT";
      break;
    case SIGINT:
      msg = "SIGINT";
      break;
    case SIGHUP:
      msg = "SIGHUP";
      break;
    // not ours..
    case SIGUSR2:
      msg = "SIGUSR2";
      break;
    case SIGUSR1:
      msg = "SIGUSR1";
      break;
    default:
      msg = "Unknown signal";
      break;
  }

  int pid = ACE_OS::getpid();
  LOGERROR("Received signal %2d  %s in pid %5d\n",
      sig, msg.c_str(), pid);

  gemfire::StackTrace trace;
  trace.print();
 
#ifdef DEBUG
  gemfire::SignalHandler::waitForDebugger();
#else
  LOGERROR("Dumping core for signal %2d %s in pid %5d and exiting\n",
      sig, msg.c_str(), pid);
  char dumpFile[PATH_MAX];
  SignalHandler::dumpStack(dumpFile, PATH_MAX - 1);
#endif

  exit( 1 );
}

void SignalHandler::dumpStack(char* dumpFile, size_t maxLen)
{
  int pid = ACE_OS::getpid();
  int CMD_MAX = maxLen + 30;

  char* dumpCmd = new char[CMD_MAX];
  DeleteArray<char> delCmd(dumpCmd);

  char* dumpFileStart = dumpFile;
  int dumpFileEnd;
  const char* crashDumpLocation = getCrashDumpLocation();
  const char* crashDumpPrefix = getCrashDumpPrefix();
  // gcore always appends the pid so no need to add pid to filename explicitly
  if (crashDumpLocation != NULL && crashDumpLocation[0] != '\0') {
    dumpFileEnd = ACE_OS::snprintf(dumpFile, maxLen, "%s/%s-%ld.core",
        crashDumpLocation, crashDumpPrefix, time(NULL));
  }
  else {
    char cwd[PATH_MAX];
    ACE_OS::getcwd(cwd, PATH_MAX - 1);
    dumpFileEnd = ACE_OS::snprintf(dumpFile, maxLen, "%s/%s-%ld.core",
        cwd, crashDumpPrefix, time(NULL));
  }
  ACE_OS::snprintf(dumpCmd, CMD_MAX, "gcore -o %s %d >/dev/null",
      dumpFile, pid);

  dumpFile += dumpFileEnd;
  maxLen -= dumpFileEnd;
  dumpFileEnd = ACE_OS::snprintf(dumpFile, maxLen, ".%d", pid);

  LOGERROR("Generating core dump in %s; executing: %s", dumpFileStart, dumpCmd);

  if (ACE_OS::system(dumpCmd) < 0) {
    LOGERROR("dumpStack: failed to execute gcore");
    dumpFile += dumpFileEnd;
    maxLen -= dumpFileEnd;
    ACE_OS::snprintf(dumpFile, maxLen, " (failed to open file)");
  }
}

}

