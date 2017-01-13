#ifndef _GEMFIRE_STATISTICS_STATISTICS_HPP_
#define _GEMFIRE_STATISTICS_STATISTICS_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>

/** @file
*/

namespace gemfire_statistics {
/**
 * An instantiation of an existing <code>StatisticsType</code> object with
 * methods for
 * setting, incrementing and getting individual <code>StatisticDescriptor</code>
 * values.
 */
class CPPCACHE_EXPORT Statistics {
 public:
  /**
   * Closes these statistics.  After statistics have been closed, they
   * are no longer archived.
   * A value access on a closed statistics always results in zero.
   * A value modification on a closed statistics is ignored.
   */
  virtual void close() = 0;

  ////////////////////////  accessor Methods  ///////////////////////

  /**
   * Returns the id of the statistic with the given name in this
   * statistics instance.
   *
   * @param name statistic name
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   *
   * @see StatisticsType#nameToDescriptor
   */
  virtual int32 nameToId(const char* name) = 0;

  /**
   * Returns the descriptor of the statistic with the given name in this
   * statistics instance.
   *
   * @param name statistic name
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   *
   * @see StatisticsType#nameToId
   */
  virtual StatisticDescriptor* nameToDescriptor(const char* name) = 0;

  /**
   * Gets a value that uniquely identifies this statistics.
   */
  virtual int64 getUniqueId() = 0;

  /**
   * Gets the {@link StatisticsType} of this instance.
   */
  virtual StatisticsType* getType() = 0;
  /**
   * Gets the text associated with this instance that helps identify it.
   */
  virtual const char* getTextId() = 0;
  /**
   * Gets the number associated with this instance that helps identify it.
   */

  virtual int64 getNumericId() = 0;
  /**
   * Returns true if modifications are atomic. This means that multiple threads
   * can safely modify this instance without additional synchronization.
   * <p>
   * Returns false if modifications are not atomic. This means that
   * modifications
   * to this instance are cheaper but not thread safe.
   * <P>
   * Note that all instances that are {@link #isShared shared} are also atomic.
   */
  virtual bool isAtomic() = 0;
  /**
   * Returns true if the data for this instance is stored in shared memory.
   * Returns false if the data is store in local memory.
   * <P>
   * Note that all instances that are {@link #isShared shared} are also atomic.
   */
  virtual bool isShared() = 0;
  /**
   * Returns true if the instance has been {@link #close closed}.
   */
  virtual bool isClosed() = 0;

  ////////////////////////  set() Methods  ///////////////////////

  /**
    * Sets the value of a statistic with the given <code>id</code>
   * whose type is <code>int</code>.
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param value value to set
   *
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual void setInt(int32 id, int32 value) = 0;

  /**
   * Sets the value of a named statistic of type <code>int</code>
   *
   * @param name statistic name
   * @param value value to set
   * @throws IllegalArgumentException
   *         If no statistic exists named <code>name</code> or
   *         if the statistic with name <code>name</code> is not of
   *         type <code>int</code>.
   */
  virtual void setInt(char* name, int32 value) = 0;

  /**
   * Sets the value of a described statistic of type <code>int</code>
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @param value value to set
   * @throws IllegalArgumentException
   *         If no statistic exists for the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>int</code>.
   */
  virtual void setInt(StatisticDescriptor* descriptor, int32 value) = 0;

  /**
   * Sets the value of a statistic with the given <code>id</code>
   * whose type is <code>long</code>.
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param value value to set
   *
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */

  virtual void setLong(int32 id, int64 value) = 0;
  /**
   * Sets the value of a described statistic of type <code>long</code>
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @param value value to set
   * @throws IllegalArgumentException
   *         If no statistic exists for the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>long</code>.
   */
  virtual void setLong(StatisticDescriptor* descriptor, int64 value) = 0;

  /**
   * Sets the value of a named statistic of type <code>long</code>.
   *
   * @param name statistic name
   * @param value value to set
   * @throws IllegalArgumentException
   *         If no statistic exists named <code>name</code> or
   *         if the statistic with name <code>name</code> is not of
   *         type <code>long</code>.
   */
  virtual void setLong(char* name, int64 value) = 0;

  /**
   * Sets the value of a statistic with the given <code>id</code>
   * whose type is <code>double</code>.
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param value value to set
   *
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual void setDouble(int32 id, double value) = 0;

  /**
   * Sets the value of a described statistic of type <code>double</code>
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @param value value to set
   * @throws IllegalArgumentException
   *         If no statistic exists for the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>double</code>.
   */
  virtual void setDouble(StatisticDescriptor* descriptor, double value) = 0;
  /**
   * Sets the value of a named statistic of type <code>double</code>.
   *
   * @param name statistic name
   * @param value value to set
   * @throws IllegalArgumentException
   *         If no statistic exists named <code>name</code> or
   *         if the statistic with name <code>name</code> is not of
   *         type <code>double</code>.
   */
  virtual void setDouble(char* name, double value) = 0;

  ///////////////////////  get() Methods  ///////////////////////

  /**
   * Returns the value of the identified statistic of type <code>int</code>.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual int32 getInt(int32 id) = 0;

  /**
   * Returns the value of the described statistic of type <code>int</code>.
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If no statistic exists with the specified <code>descriptor</code>
   * or
   *         if the described statistic is not of
   *         type <code>int</code>.
   */
  virtual int32 getInt(StatisticDescriptor* descriptor) = 0;
  /**
   * Returns the value of the statistic of type <code>int</code> at
   * the given name.
   * @param name statistic name
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>int</code>.
   */
  virtual int32 getInt(char* name) = 0;

  /**
   * Returns the value of the identified statistic of type <code>long</code>.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual int64 getLong(int32 id) = 0;

  /**
   * Returns the value of the described statistic of type <code>long</code>.
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If no statistic exists with the specified <code>descriptor</code>
   * or
   *         if the described statistic is not of
   *         type <code>long</code>.
   */
  virtual int64 getLong(StatisticDescriptor* descriptor) = 0;
  /**
   * Returns the value of the statistic of type <code>long</code> at
   * the given name.
   *
   * @param name statistic name
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>long</code>.
   */
  virtual int64 getLong(char* name) = 0;

  /**
   * Returns the value of the identified statistic of type <code>double</code>.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual double getDouble(int32 id) = 0;

  /**
   * Returns the value of the described statistic of type <code>double</code>.
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If no statistic exists with the specified <code>descriptor</code>
   * or
   *         if the described statistic is not of
   *         type <code>double</code>.
   */
  virtual double getDouble(StatisticDescriptor* descriptor) = 0;
  /**
   * Returns the value of the statistic of type <code>double</code> at
   * the given name.
   * @param name statistic name
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>double</code>.
   */
  virtual double getDouble(char* name) = 0;

  /**
   * Returns the value of the identified statistic.
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If the described statistic does not exist
   */
  //   virtual Number get(StatisticDescriptor* descriptor)=0;

  /**
   * Returns the value of the named statistic.
   *
   * @throws IllegalArgumentException
   *         If the named statistic does not exist
   */
  //   virtual Number get(char* name)=0;

  /**
   * Returns the bits that represent the raw value of the described statistic.
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If the described statistic does not exist
   */
  virtual int64 getRawBits(StatisticDescriptor* descriptor) = 0;

  /**
   * Returns the bits that represent the raw value of the named statistic.
   *
   * @throws IllegalArgumentException
   *         If the named statistic does not exist
   */
  // virtual double getRawBits(char* name)=0;

  ////////////////////////  inc() Methods  ////////////////////////

  /**
   * Increments the value of the identified statistic of type <code>int</code>
   * by the given amount.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param delta change value to be added
   *
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual int32 incInt(int32 id, int32 delta) = 0;

  /**
   * Increments the value of the described statistic of type <code>int</code>
   * by the given amount.
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @param delta change value to be added
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>int</code>.
   */
  virtual int32 incInt(StatisticDescriptor* descriptor, int32 delta) = 0;

  /**
   * Increments the value of the statistic of type <code>int</code> with
   * the given name by a given amount.
   * @param name statistic name
   * @param delta change value to be added
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>int</code>.
   */
  virtual int32 incInt(char* name, int32 delta) = 0;

  /**
   * Increments the value of the identified statistic of type <code>long</code>
   * by the given amount.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param delta change value to be added
   *
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual int64 incLong(int32 id, int64 delta) = 0;

  /**
   * Increments the value of the described statistic of type <code>long</code>
   * by the given amount.
   *
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @param delta change value to be added
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>long</code>.
   */
  virtual int64 incLong(StatisticDescriptor* descriptor, int64 delta) = 0;
  /**
   * Increments the value of the statistic of type <code>long</code> with
   * the given name by a given amount.
   *
   * @param name statistic name
   * @param delta change value to be added
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>long</code>.
   */
  virtual int64 incLong(char* name, int64 delta) = 0;

  /**
   * Increments the value of the identified statistic of type
   * <code>double</code>
   * by the given amount.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param delta change value to be added
   *
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If the id is invalid.
   */
  virtual double incDouble(int32 id, double delta) = 0;

  /**
   * Increments the value of the described statistic of type <code>double</code>
   * by the given amount.
   * @param descriptor a statistic descriptor obtained with {@link
   * #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @param delta change value to be added
   *
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>double</code>.
   */
  virtual double incDouble(StatisticDescriptor* descriptor, double delta) = 0;
  /**
   * Increments the value of the statistic of type <code>double</code> with
   * the given name by a given amount.
   * @param name statistic name
   * @param delta change value to be added
   *
   * @return The value of the statistic after it has been incremented
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>double</code>.
   */
  virtual double incDouble(char* name, double delta) = 0;

 protected:
  /**
  *  Destructor is protected to prevent direct deletion. Use close().
  */
  virtual ~Statistics() = 0;
};  // class

};  // namespace

#endif  // _GEMFIRE_STATISTICS_STATISTICS_HPP_
