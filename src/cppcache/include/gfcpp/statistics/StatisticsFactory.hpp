#ifndef _GEMFIRE_STATISTICS_STATISTICSFACTORY_HPP_
#define _GEMFIRE_STATISTICS_STATISTICSFACTORY_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include "../ExceptionTypes.hpp"
#include <gfcpp/statistics/Statistics.hpp>

/** @file
*/

namespace gemfire_statistics {

/**
 * Instances of this interface provide methods that create instances
 * of {@link StatisticDescriptor} and {@link StatisticsType}.
 * Every {@link StatisticsFactory} is also a type factory.
 *
 * <P>
 *
 * A <code>StatisticsFactory</code> can create a {@link
 * StatisticDescriptor statistic} of three numeric types:
 * <code>int</code>, <code>long</code>, and <code>double</code>.  A
 * statistic (<code>StatisticDescriptor</code>) can either be a
 * <I>gauge</I> meaning that its value can increase and decrease or a
 * <I>counter</I> meaning that its value is strictly increasing.
 * Marking a statistic as a counter allows the GemFire Manager Console
 * to properly display a statistics whose value "wraps around" (that
 * is, exceeds its maximum value).
 *
 */

class CPPCACHE_EXPORT StatisticsFactory {
 protected:
  StatisticsFactory() {}
  StatisticsFactory(const StatisticsFactory&) {}

 public:
  /**
   * Return a pre-existing statistics factory. Typically configured through
   * creation of a distributed system.
   */
  static StatisticsFactory* getExistingInstance();

  virtual ~StatisticsFactory() {}

  /**
   * Creates and returns a long counter {@link StatisticDescriptor}
   * with the given <code>name</code>, <code>description</code>,
   * <code>units</code>,and with larger values indicating better performance.
   */

  virtual StatisticDescriptor* createIntCounter(const char* name,
                                                const char* description,
                                                const char* units,
                                                int8 largerBetter = true) = 0;

  /**
   * Creates and returns a double counter {@link StatisticDescriptor}
   * with the given <code>name</code>, <code>description</code>,
   *<code>units</code>, and with larger values indicating better performance.
   */

  virtual StatisticDescriptor* createLongCounter(const char* name,
                                                 const char* description,
                                                 const char* units,
                                                 int8 largerBetter = true) = 0;

  /**
    * Creates and returns an int gauge {@link StatisticDescriptor}
    * with the given <code>name</code>, <code>description</code>,
    * <code>units</code>,  and with smaller values indicating better
   * performance.
    */

  virtual StatisticDescriptor* createDoubleCounter(
      const char* name, const char* description, const char* units,
      int8 largerBetter = true) = 0;

  /**
   * Creates and returns an int gauge {@link StatisticDescriptor}
   * with the given <code>name</code>, <code>description</code>,
   * <code>units</code>,  and with smaller values indicating better performance.
   */
  virtual StatisticDescriptor* createIntGauge(const char* name,
                                              const char* description,
                                              const char* units,
                                              int8 largerBetter = false) = 0;

  /**
   * Creates and returns an long gauge {@link StatisticDescriptor}
   * with the given <code>name</code>, <code>description</code>,
   * <code>units</code>,  and with smaller values indicating better performance.
   */
  virtual StatisticDescriptor* createLongGauge(const char* name,
                                               const char* description,
                                               const char* units,
                                               int8 largerBetter = false) = 0;

  /**
   * Creates and returns an double gauge {@link StatisticDescriptor}
   * with the given <code>name</code>, <code>description</code>,
   * <code>units</code>,  and with smaller values indicating better performance.
   */
  virtual StatisticDescriptor* createDoubleGauge(const char* name,
                                                 const char* description,
                                                 const char* units,
                                                 int8 largerBetter = false) = 0;

  /**
   * Creates  and returns a {@link StatisticsType}
   * with the given <code>name</code>, <code>description</code>,
   * and {@link StatisticDescriptor}.
   * @throws IllegalArgumentException
   * if a type with the given <code>name</code> already exists.
   */
  virtual StatisticsType* createType(const char* name, const char* description,
                                     StatisticDescriptor** stats,
                                     int32 statsLength) = 0;

  /**
   * Finds and returns an already created {@link StatisticsType}
   * with the given <code>name</code>.
   * Returns <code>null</code> if the type does not exist.
   */
  virtual StatisticsType* findType(const char* name) = 0;

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link
   * StatisticsType type} with default ids.
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  virtual Statistics* createStatistics(StatisticsType* type) = 0;

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link
   * StatisticsType type}, <code>textId</code>, and with a default numeric id.
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  virtual Statistics* createStatistics(StatisticsType* type,
                                       const char* textId) = 0;

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link
   * StatisticsType type}, <code>textId</code>, and <code>numericId</code>.
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  virtual Statistics* createStatistics(StatisticsType* type, const char* textId,
                                       int64 numericId) = 0;

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link
   * StatisticsType type} with default ids.
   * <p>
   * The created instance will be {@link Statistics#isAtomic atomic}.
   */
  virtual Statistics* createAtomicStatistics(StatisticsType* type) = 0;

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link
   * StatisticsType type}, <code>textId</code>, and with a default numeric id.
   * <p>
   * The created instance will be {@link Statistics#isAtomic atomic}.
   */
  virtual Statistics* createAtomicStatistics(StatisticsType* type,
                                             const char* textId) = 0;

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link
   * StatisticsType type}, <code>textId</code>, and <code>numericId</code>.
   * <p>
   * The created instance will be {@link Statistics#isAtomic atomic}.
   */
  virtual Statistics* createAtomicStatistics(StatisticsType* type,
                                             const char* textId,
                                             int64 numericId) = 0;

  /** Return the first instance that matches the type, or NULL */
  virtual Statistics* findFirstStatisticsByType(StatisticsType* type) = 0;

  /**
   * Returns a name that can be used to identify the manager
   */
  virtual const char* getName() = 0;

  /**
   * Returns a numeric id that can be used to identify the manager
   */
  virtual int64 getId() = 0;

 private:
  const StatisticsFactory& operator=(const StatisticsFactory&);

};  // class
};  // namespace

#endif  //  _GEMFIRE__STATISTICS_STATISTICSFACTORY_HPP_
