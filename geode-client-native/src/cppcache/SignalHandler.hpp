#ifndef _GEMFIRE_SIGNALHANDLER_HPP_
#define _GEMFIRE_SIGNALHANDLER_HPP_
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "Exception.hpp"

/** @file
*/

#ifdef _WIN32
struct _EXCEPTION_POINTERS;
typedef _EXCEPTION_POINTERS EXCEPTION_POINTERS;
#endif

namespace gemfire
{

  class DistributedSystem;

  /** Represents a signal handler used for dumping stacks and 
   * attaching a debugger */
  class CPPCACHE_EXPORT SignalHandler
  {
  private:
    static int s_waitSeconds;

    static void init(bool crashDumpEnabled, const char* crashDumpLocation,
        const char* crashDumpPrefix);

    friend class DistributedSystem;

  public:
    /**
     * Register the GemFire backtrace signal handler for signals
     * that would otherwise core dump.
     */
    static void installBacktraceHandler();

    /**
     * Remove the GemFire backtrace signal handler for signals that were
     * previously registered.
     */
    static void removeBacktraceHandler();
    /**
    * Returns whether CrashDump is enabled or not.
    */
    static bool getCrashDumpEnabled();
    /**
    * Returns CrashDump location.
    */
    static const char* getCrashDumpLocation();
    /**
    * Returns CrashDump Prefix.
    */
    static const char* getCrashDumpPrefix();

    /**
     * Dump an image of the current process (core in UNIX, minidump in Windows)
     * and filling the path of dump in provided buffer of given max size.
     */
    static void dumpStack(char* dumpFile, size_t maxLen);

#ifdef _WIN32

    /**
     * Dump an image of the current process (core in UNIX, minidump in Windows)
     * and filling the path of dump in provided buffer of given max size.
     */
    static void dumpStack(unsigned int expCode, EXCEPTION_POINTERS* pExp,
        char* dumpFile, size_t maxLen);

    static void * s_pOldHandler;

#endif

    /** wait in a loop to allow a debugger to be manuallly attached 
     * and done is set to 1. */
    static void waitForDebugger();
  };

}

#endif

