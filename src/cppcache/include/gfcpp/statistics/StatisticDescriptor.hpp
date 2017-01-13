#ifndef _GEMFIRE_STATISTICS_STATISTICDESCRIPTOR_HPP_
#define _GEMFIRE_STATISTICS_STATISTICDESCRIPTOR_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {

/**
 * Describes an individual statistic whose value is updated by an
 * application and may be archived by GemFire.  These descriptions are
 * gathered together in a {@link StatisticsType}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 * <P>
 * StatisticDescriptors are naturally ordered by their name.
 *
 */

class CPPCACHE_EXPORT StatisticDescriptor {
 public:
  /**
    * Returns the id of this statistic in a {@link StatisticsType
    * }. The id is initialized when its statistics
    * type is created.
    */
  virtual int32 getId() = 0;

  /**
   * Returns the name of this statistic
   */
  virtual const char* getName() = 0;

  /**
   * Returns a description of this statistic
   */
  virtual const char* getDescription() = 0;

  /**
   * Returns true if this statistic is a counter; false if its a gauge.
   * Counter statistics have values that always increase.
   * Gauge statistics have unconstrained values.
   */
  virtual int8 isCounter() = 0;

  /**
   *  Returns true if a larger statistic value indicates better performance.
   */
  virtual int8 isLargerBetter() = 0;

  /**
   *  Returns the unit in which this statistic is measured
   */
  virtual const char* getUnit() = 0;

  /*
   * Destructor
   */
  virtual ~StatisticDescriptor() {}

};  // class

};  // namespace

#endif  // _GEMFIRE_STATISTICS_STATISTICDESCRIPTOR_HPP_
