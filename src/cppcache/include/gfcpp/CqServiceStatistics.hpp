#ifndef __GEMFIRE_CQ_SERVICE_STATISTICS_H__
#define __GEMFIRE_CQ_SERVICE_STATISTICS_H__
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
 * @class CqServiceStatistics CqServiceStatistics.hpp
 *
 * This class provides methods to get aggregate statistical information
 * about the CQs of a client.
 */
class CPPCACHE_EXPORT CqServiceStatistics : public SharedBase {
 public:
  /**
   * Get the number of CQs currently active.
   * Active CQs are those which are executing (in running state).
   * @return number of CQs
   */
  virtual uint32_t numCqsActive() const = 0;

  /**
   * Get the total number of CQs created. This is a cumulative number.
   * @return number of CQs created.
   */
  virtual uint32_t numCqsCreated() const = 0;

  /**
   * Get the total number of closed CQs. This is a cumulative number.
   * @return number of CQs closed.
   */
  virtual uint32_t numCqsClosed() const = 0;

  /**
   * Get the number of stopped CQs currently.
   * @return number of CQs stopped.
   */
  virtual uint32_t numCqsStopped() const = 0;

  /**
   * Get number of CQs that are currently active or stopped.
   * The CQs included in this number are either running or stopped (suspended).
   * Closed CQs are not included.
   * @return number of CQs on client.
   */
  virtual uint32_t numCqsOnClient() const = 0;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_SERVICE_STATISTICS_H__
