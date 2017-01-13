#ifndef _GEMFIRE_STATISTICS_STATISTICSTYPE_HPP_
#define _GEMFIRE_STATISTICS_STATISTICSTYPE_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>

using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {

/**
 * Used to describe a logical collection of StatisticDescriptors. These
 * descriptions
 * are used to create an instance of {@link Statistics}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 *
 */

class CPPCACHE_EXPORT StatisticsType {
 public:
  /**
   * Returns the name of this statistics type.
   */
  virtual const char* getName() = 0;

  /**
   * Returns a description of this statistics type.
   */
  virtual const char* getDescription() = 0;

  /**
   * Returns descriptions of the statistics that this statistics type
   * gathers together.
   */
  virtual StatisticDescriptor** getStatistics() = 0;

  /**
   * Returns the id of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   */
  virtual int32 nameToId(const char* name) = 0;
  /**
   * Returns the descriptor of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   */
  virtual StatisticDescriptor* nameToDescriptor(const char* name) = 0;

  /**
   * Returns the total number of statistics descriptors in the type.
   */
  virtual int32 getDescriptorsCount() = 0;

  // protected:
  /**
   * Destructor
   */
  virtual ~StatisticsType() {}

};  // class

};  // namespace

#endif  // ifndef __GEMFIRE_STATISTICSTYPE_H__
