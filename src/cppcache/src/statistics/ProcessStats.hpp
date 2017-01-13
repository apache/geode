#ifndef _GEMFIRE_STATISTICS_PROCESSSTATS_HPP_
#define _GEMFIRE_STATISTICS_PROCESSSTATS_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/Statistics.hpp>
using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {

/**
 * Abstracts the process statistics that are common on all platforms.
 * This is necessary for monitoring the health of GemFire components.
 *
 */
class CPPCACHE_EXPORT ProcessStats {
 public:
  /**
   * Creates a new <code>ProcessStats</code> that wraps the given
   * <code>Statistics</code>.
   */
  ProcessStats();

  /**
   * Returns the CPU Usage
   */
  virtual int32 getCpuUsage() = 0;

  /**
   * Returns Number of threads
   */
  virtual int32 getNumThreads() = 0;

  /**
   * Returns the size of this process (resident set on UNIX or working
   * set on Windows) in megabytes
   */
  virtual int64 getProcessSize() = 0;

  /**
   * Close Underline Statistics
   */
  virtual void close() = 0;
  virtual int64 getCPUTime() = 0;

  /**
   * Returns the CPU time which is sum of userTime and systemTime
   */
  virtual int64 getAllCpuTime() = 0;

  /**
   * Destructor
   */
  virtual ~ProcessStats();
};
};
#endif  // _GEMFIRE_STATISTICS_PROCESSSTATS_HPP_
