#ifndef __GEMFIRE_CQ_STATISTICS_H__
#define __GEMFIRE_CQ_STATISTICS_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @cacheserver
 * Querying is only supported for native clients.
 * @endcacheserver
 * @class CqStatistics CqStatistics.hpp
 *
 * This class provides methods to get statistical information about a registered
 * Continuous Query (CQ)
 * represented by the CqQuery object.
 *
 */
class CPPCACHE_EXPORT CqStatistics : public SharedBase {
 public:
  /**
   * Get number of Insert events qualified by this CQ.
   * @return number of inserts.
   */
  virtual uint32_t numInserts() const = 0;

  /**
   * Get number of Delete events qualified by this CQ.
   * @return number of deletes.
   */
  virtual uint32_t numDeletes() const = 0;

  /**
   * Get number of Update events qualified by this CQ.
   * @return number of updates.
   */
  virtual uint32_t numUpdates() const = 0;

  /**
   * Get total of all the events qualified by this CQ.
   * @return total number of events.
   */
  virtual uint32_t numEvents() const = 0;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_STATISTICS_H__
