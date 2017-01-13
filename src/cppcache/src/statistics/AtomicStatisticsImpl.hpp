#ifndef _GEMFIRE_STATISTICS_ATOMICSTATISTICSIMPL_HPP_
#define _GEMFIRE_STATISTICS_ATOMICSTATISTICSIMPL_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Atomic_Op_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <HostAsm.hpp>
#include <gfcpp/statistics/Statistics.hpp>
#include "StatisticsTypeImpl.hpp"
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include <string>

#include <NonCopyable.hpp>

using namespace gemfire;

/** @file
*/

namespace gemfire_statistics {

/**
 * An implementation of {@link Statistics} that stores its statistics
 * in local memory and support atomic operations
 *
 */

/* adongre
 * CID 28732: Other violation (MISSING_COPY)
 * Class "gemfire_statistics::AtomicStatisticsImpl" owns resources that are
 * managed
 * in its constructor and destructor but has no user-written copy constructor.
 *
 * CID 28718: Other violation (MISSING_ASSIGN)
 * Class "gemfire_statistics::AtomicStatisticsImpl" owns resources that are
 * managed in its constructor and destructor but has no user-written assignment
 * operator.
 *
 * FIX : Make the class non-copyable
 */
class AtomicStatisticsImpl : public Statistics, private NonCopyable {
 private:
  /**********varbs originally kept in statisticsimpl class*****************/
  /** The type of this statistics instance */
  StatisticsTypeImpl* statsType;

  /** The display name of this statistics instance */
  const char* textId;
  std::string textIdStr;

  /** Numeric information display with these statistics */
  int64 numericId;

  /** Are these statistics closed? */
  bool closed;

  /** Uniquely identifies this instance */
  int64 uniqueId;

  /****************************************************************************/
  /** An array containing the values of the int32 statistics */
  ACE_Atomic_Op<ACE_Recursive_Thread_Mutex, int32>* intStorage;

  /** An array containing the values of the int64 statistics */
  ACE_Atomic_Op<ACE_Recursive_Thread_Mutex, int64>* longStorage;

  /** An array containing the values of the double statistics */
  ACE_Atomic_Op<ACE_Recursive_Thread_Mutex, double>* doubleStorage;

  ///////////////////////Private Methods//////////////////////////
  bool isOpen();

  int32 getIntId(StatisticDescriptor* descriptor);

  int32 getLongId(StatisticDescriptor* descriptor);

  int32 getDoubleId(StatisticDescriptor* descriptor);

  //////////////////////  Static private Methods  //////////////////////

  int64 calcNumericId(StatisticsFactory* system, int64 userValue);

  const char* calcTextId(StatisticsFactory* system, const char* userValue);

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new statistics instance of the given type
   *
   * @param type
   *        A description of the statistics
   * @param textId
   *        Text that identifies this statistic when it is monitored
   * @param numericId
   *        A number that displayed when this statistic is monitored
   * @param uniqueId
   *        A number that uniquely identifies this instance
   * @param system
   *        The distributed system that determines whether or not these
   *        statistics are stored (and collected) in local memory
   */

 public:
  AtomicStatisticsImpl(StatisticsType* type, const char* textId,
                       int64 numericId, int64 uniqueId,
                       StatisticsFactory* system);

  //////////////////////  Instance Methods  //////////////////////
  ~AtomicStatisticsImpl();

  bool usesSystemCalls();

  int32 nameToId(const char* name);

  StatisticDescriptor* nameToDescriptor(const char* name);

  bool isClosed();

  bool isShared();

  bool isAtomic();

  void close();
  /////////////////////////Attribute methods//////////////////////////

  StatisticsType* getType();

  const char* getTextId();

  int64 getNumericId();

  int64 getUniqueId();

  ////////////////////////  set() Methods  ///////////////////////

  void setInt(char* name, int32 value);

  void setInt(StatisticDescriptor* descriptor, int32 value);

  void setInt(int32 offset, int32 value);

  void setLong(char* name, int64 value);

  void setLong(StatisticDescriptor* descriptor, int64 value);

  void setLong(int32 id, int64 value);

  void setDouble(char* name, double value);

  void setDouble(StatisticDescriptor* descriptor, double value);

  void setDouble(int32 id, double value);

  ///////////////////////  get() Methods  ///////////////////////

  int32 getInt(char* name);

  int32 getInt(StatisticDescriptor* descriptor);

  int32 getInt(int32 offset);

  int64 getLong(char* name);

  int64 getLong(StatisticDescriptor* descriptor);

  int64 getLong(int32 id);

  double getDouble(char* name);

  double getDouble(StatisticDescriptor* descriptor);

  double getDouble(int32 id);

  int64 getRawBits(StatisticDescriptor* descriptor);

  int64 getRawBits(char* name);

  ////////////////////////  inc() Methods  ////////////////////////

  int32 incInt(char* name, int32 delta);

  int32 incInt(StatisticDescriptor* descriptor, int32 delta);

  int32 incInt(int32 offset, int32 delta);

  int64 incLong(char* name, int64 delta);

  int64 incLong(StatisticDescriptor* descriptor, int64 delta);

  int64 incLong(int32 id, int64 delta);

  double incDouble(char* name, double delta);

  double incDouble(StatisticDescriptor* descriptor, double delta);

  double incDouble(int32 id, double delta);

 protected:
  void _setInt(int32 offset, int32 value);

  void _setLong(int32 offset, int64 value);

  void _setDouble(int32 offset, double value);

  int32 _getInt(int32 offset);

  int64 _getLong(int32 offset);

  double _getDouble(int32 offset);

  /**
   * Returns the bits that represent the raw value of the
   * specified statistic descriptor.
   */
  int64 _getRawBits(StatisticDescriptor* stat);

  int32 _incInt(int32 offset, int32 delta);

  int64 _incLong(int32 offset, int64 delta);

  double _incDouble(int32 offset, double delta);

};  // class

};  // namespace

#endif  //  _GEMFIRE_STATISTICS_ATOMICSTATISTICSIMPL_HPP_
