#ifndef _GEMFIRE_STATISTICS_STATISTICDESCRIPTORIMPL_HPP_
#define _GEMFIRE_STATISTICS_STATISTICDESCRIPTORIMPL_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
*/

#include <string>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>

/** @file
*/

namespace gemfire_statistics {

typedef enum { INT_TYPE = 5, LONG_TYPE = 6, DOUBLE_TYPE = 8 } FieldType;

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

class StatisticDescriptorImpl : public StatisticDescriptor {
 private:
  /** The name of the statistic */
  std::string name;

  /** A description of the statistic */
  std::string description;

  /** The unit of the statistic */
  std::string unit;

  /** Is the statistic a counter? */
  int8 isStatCounter;

  /** Do larger values of the statistic indicate better performance? */
  int8 isStatLargerBetter;

  /** The physical offset used to access the data that stores the
   * value for this statistic in an instance of {@link Statistics}
   */
  int32 id;

  /**
   * Creates a new description of a statistic.
   *
   * @param name
   *        The name of the statistic (for example,
   *        <code>"numDatabaseLookups"</code>)
   * @param descriptorType
   *        The type of the statistic.  This must be either
   *        <code>FieldType::INT_TYPE</code>, <code>FieldType::LONG_TYPE</code>,
   * or
   *        <code>FieldType::DOUBLE_TYPE</code>.
   * @param description
   *        A description of the statistic (for example, <code>"The
   *        number of database lookups"</code>
   * @param unit
   *        The units that this statistic is measured in (for example,
   *        <code>"milliseconds"</code>)
   * @param isCounter
   *        Is this statistic a counter?  That is, does its value
   *        change monotonically (always increases or always
   *        decreases)?
   * @param isLargerBetter
   *        True if larger values indicate better performance.
   *
   */
  StatisticDescriptorImpl(const char* statName, FieldType statDescriptorType,
                          const char* statDescription, const char* statUnit,
                          int8 statIsCounter, int8 statIsLargerBetter);

 public:
  /** GfFieldType defined in gemfire.h.
   * It describes the date type of an individual descriptor.
   * Supported date types are INT, LONG, and DOUBLE.
   */
  FieldType descriptorType;

  /**
   * Destructor
   */
  ~StatisticDescriptorImpl();

  /////////////////////////// Static Methods////////////////////////////////////

  /**
   * Returns the name of the given type code
   * Returns "int" for int_t, "long" for Long, "double" for Double
   * @throws IllegalArgumentException
   * <code>code</code> is an unknown type
   */
  static const char* getTypeCodeName(FieldType code) throw(
      IllegalArgumentException);

  /**
   * Returns the number of bits needed to represent a value of the given type
   * Currently the supported types and their values are int_t :32 , Long :64,
   * Double:64
   * @throws IllegalArgumentException
   *         <code>code</code> is an unknown type
   */
  static int32 getTypeCodeBits(FieldType code) throw(IllegalArgumentException);

  ///////////////////////////Create methods ////////////////////////////////////

  /**
   * Creates a descriptor of Integer type
   * whose value behaves like a counter
   * @throws OutOfMemoryException
   */
  static StatisticDescriptor* createIntCounter(
      const char* name, const char* description, const char* units,
      int8 isLargerBetter) throw(OutOfMemoryException);
  /**
   * Creates a descriptor of Long type
   * whose value behaves like a counter
   * @throws OutOfMemoryException
   */

  static StatisticDescriptor* createLongCounter(
      const char* name, const char* description, const char* units,
      int8 isLargerBetter) throw(OutOfMemoryException);

  /**
   * Creates a descriptor of Double type
   * whose value behaves like a counter
   * @throws OutOfMemoryException
   */
  static StatisticDescriptor* createDoubleCounter(
      const char* name, const char* description, const char* units,
      int8 isLargerBetter) throw(OutOfMemoryException);

  /**
   * Creates a descriptor of Integer type
   * whose value behaves like a gauge
   * @throws OutOfMemoryException
   */
  static StatisticDescriptor* createIntGauge(
      const char* name, const char* description, const char* units,
      int8 isLargerBetter) throw(OutOfMemoryException);

  /**
   * Creates a descriptor of Long type
   * whose value behaves like a gauge
   * @throws OutOfMemoryException
   */
  static StatisticDescriptor* createLongGauge(
      const char* name, const char* description, const char* units,
      int8 isLargerBetter) throw(OutOfMemoryException);

  /**
   * Creates a descriptor of Double type
   * whose value behaves like a gauge
   * @throws OutOfMemoryException
   */
  static StatisticDescriptor* createDoubleGauge(
      const char* name, const char* description, const char* units,
      int8 isLargerBetter) throw(OutOfMemoryException);

  /////////////////  StatisticDescriptor(Base class) Methods
  ///////////////////////

  const char* getName();

  const char* getDescription();

  int32 getStorageBits();

  int8 isCounter();

  int8 isLargerBetter();

  const char* getUnit();

  int32 getId() throw(IllegalStateException);

  ///////////////////////////// Instance Methods  ////////////////////////////

  /**
   * Returns the type code of this statistic
   * Possible values are:
   * GF_FIELDTYPE_INT
   * GF_FIELDTYPE_LONG
   * GF_FIELDTYPE_DOUBLE
   */
  FieldType getTypeCode();

  /**
   * Sets the id of this descriptor
   * An uninitialized id will be -1
   */
  void setId(int32 statId);
  ///////////////////////////// Check methods ///////////////////////////////

  /**
   *  Checks whether the descriptor is of type int and returns the id if it is
   *  @throws IllegalArgumentException
   */
  int32 checkInt() throw(IllegalArgumentException);

  /**
   *  Checks whether the descriptor is of type long and returns the id if it is
   *  @throws IllegalArgumentException
   */
  int32 checkLong() throw(IllegalArgumentException);

  /**
   *  Checks whether the descriptor is of type double and returns the id if it i
s
   *  @throws IllegalArgumentException
   */
  int32 checkDouble() throw(IllegalArgumentException);

 private:
  static const char* IntTypeName;
  static const char* LongTypeName;
  static const char* DoubleTypeName;

};  // class

};  // namespace

#endif  //  _GEMFIRE_STATISTICS_STATISTICDESCRIPTORIMPL_HPP_
