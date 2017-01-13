#ifndef _GEMFIRE_STATISTICS_STATISTICSTYPEIMPL_HPP_
#define _GEMFIRE_STATISTICS_STATISTICSTYPEIMPL_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <map>
#include <string>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "StatsDef.hpp"

/** @file
*/

namespace gemfire_statistics {

/**
 * Gathers together a number of {@link StatisticDescriptor statistics}
 * into one logical type.
 *
 */

typedef std::map<std::string, StatisticDescriptor*> StatisticsDescMap;

class StatisticsTypeImpl : public StatisticsType {
 private:
  int32 statsLength;            // Total number of descriptors in the type
  std::string name;             // The name of this statistics type
  std::string description;      // The description of this statistics type
  StatisticDescriptor** stats;  // The array of stat descriptors id order
  StatisticsDescMap statsDescMap;
  int32 intStatCount;  // Contains the number of 32-bit statistics in this type.
  int32 longStatCount;  // Contains the number of long statistics in this type.
  int32
      doubleStatCount;  // Contains the number of double statistics in this type

 public:
  StatisticsTypeImpl(const char* name, const char* description,
                     StatisticDescriptor** stats, int32 statsLength);

  ~StatisticsTypeImpl();

  ////////////////  StatisticsType(Base class) Methods ///////////////////

  const char* getName();

  const char* getDescription();

  StatisticDescriptor** getStatistics();

  int32 nameToId(const char* name);

  StatisticDescriptor* nameToDescriptor(const char* name);

  //////////////////////  Instance Methods  //////////////////////

  /**
   *  Gets the number of statistics in this type that are ints.
   */
  int32 getIntStatCount();

  /*
   * Gets the number of statistics in this type that are longs.
   */
  int32 getLongStatCount();

  /*
   * Gets the number of statistics that are doubles.
   */
  int32 getDoubleStatCount();

  /*
   * Gets the total number of statistic descriptors in the Type
   */
  int32 getDescriptorsCount();

  // static StatisticsType[] fromXml(Reader reader,
  //                                      StatisticsTypeFactory factory);

};  // class
};  // namespace

#endif  // ifndef _GEMFIRE_STATISTICS_STATISTICSTYPEIMPL_HPP_
