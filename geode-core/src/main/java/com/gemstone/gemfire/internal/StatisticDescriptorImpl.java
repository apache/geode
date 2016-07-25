/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Describes an individual statistic whose value is updated by an
 * application and may be archived by GemFire.  These descriptions are
 * gathered together in a {@link StatisticsType}.
 *
 * @see Statistics
 *
 *
 * @since GemFire 3.0
 */
public final class StatisticDescriptorImpl implements StatisticDescriptor {

  /** A constant for an <code>byte</code> type */
  static final byte BYTE = (byte) 3;

  /** A constant for an <code>short</code> type */
  static final byte SHORT = (byte) 4;

  /** A constant for an <code>int</code> type */
  static final byte INT = (byte) 5;

  /** A constant for an <code>long</code> type */
  static final byte LONG = (byte) 6;

  /** A constant for an <code>float</code> type */
  static final byte FLOAT = (byte) 7;

  /** A constant for an <code>double</code> type */
  static final byte DOUBLE = (byte) 8;

  ////////////////////  Instance Fields  ////////////////////

  /** An unitialized offset */
  private int INVALID_OFFSET = -1;

  /** The name of the statistic */
  private final String name;

  /** The type code of this statistic */
  private final byte typeCode;

  /** A description of the statistic */
  private final String description;

  /** The unit of the statistic */
  private final String unit;

  /** Is the statistic a counter? */
  private final boolean isCounter;

  /** Do larger values of the statistic indicate better performance? */
  private final boolean isLargerBetter;

  /** The physical offset used to access the data that stores the
   * value for this statistic in an instance of {@link Statistics}
   */
  private int id = INVALID_OFFSET;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Returns the name of the given type code
   *
   * @throws IllegalArgumentException
   *         <code>code</code> is an unknown type
   */
  public final static String getTypeCodeName(int code) {
    switch (code) {
    case BYTE: 
      return "byte";
    case SHORT: 
      return "short";
    case FLOAT: 
      return "float";
    case INT: 
      return "int";
    case LONG:
      return "long";
    case DOUBLE:
      return "double";
    default:
      throw new IllegalArgumentException(LocalizedStrings.StatisticDescriptorImpl_UNKNOWN_TYPE_CODE_0.toLocalizedString(Integer.valueOf(code)));
    }
  }

  /**
   * Returns the number of bits needed to represent a value of the given type
   *
   * @throws IllegalArgumentException
   *         <code>code</code> is an unknown type
   */
  public final static int getTypeCodeBits(int code) {
    switch (code) {
    case BYTE: 
      return 8;
    case SHORT: 
      return 16;
    case FLOAT: 
      return 32;
    case INT: 
      return 32;
    case LONG:
      return 64;
    case DOUBLE:
      return 64;
    default:
      throw new IllegalArgumentException(LocalizedStrings.StatisticDescriptorImpl_UNKNOWN_TYPE_CODE_0.toLocalizedString(Integer.valueOf(code)));
    }
  }

  /**
   * Returns the class of the given type code
   *
   * @throws IllegalArgumentException
   *         <code>code</code> is an unknown type
   */
  public final static Class<?> getTypeCodeClass(byte code) {
    switch (code) {
    case BYTE: 
      return byte.class;
    case SHORT: 
      return short.class;
    case FLOAT: 
      return float.class;
    case INT: 
      return int.class;
    case LONG:
      return long.class;
    case DOUBLE:
      return double.class;
    default:
      throw new IllegalArgumentException(LocalizedStrings.StatisticDescriptorImpl_UNKNOWN_TYPE_CODE_0.toLocalizedString(Integer.valueOf(code)));
    }
  }

  public static StatisticDescriptor createIntCounter(String name, String description,
                                              String units, boolean isLargerBetter) {
    return new StatisticDescriptorImpl(name, INT, description, units, true, isLargerBetter);
  }
  public static StatisticDescriptor createLongCounter(String name, String description,
                                               String units, boolean isLargerBetter) {
    return new StatisticDescriptorImpl(name, LONG, description, units, true, isLargerBetter);
  }
  public static StatisticDescriptor createDoubleCounter(String name, String description,
                                                 String units, boolean isLargerBetter) {
    return new StatisticDescriptorImpl(name, DOUBLE, description, units, true, isLargerBetter);
  }
  public static StatisticDescriptor createIntGauge(String name, String description,
                                            String units, boolean isLargerBetter) {
    return new StatisticDescriptorImpl(name, INT, description, units, false, isLargerBetter);
  }
  public static StatisticDescriptor createLongGauge(String name, String description,
                                             String units, boolean isLargerBetter) {
    return new StatisticDescriptorImpl(name, LONG, description, units, false, isLargerBetter);
  }
  public static StatisticDescriptor createDoubleGauge(String name, String description,
                                               String units, boolean isLargerBetter) {
    return new StatisticDescriptorImpl(name, DOUBLE, description, units, false, isLargerBetter);
  }

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new description of a statistic.
   *
   * @param name
   *        The name of the statistic (for example,
   *        <code>"numDatabaseLookups"</code>) 
   * @param typeCode
   *        The type of the statistic.  This must be either
   *        <code>int.class</code>, <code>long.class</code>, or
   *        <code>double.class</code>.
   * @param description
   *        A description of the statistic (for example, <code>"The
   *        number of database lookups"</code>
   * @param unit
   *        The units that this statistic is measure in (for example,
   *        <code>"milliseconds"</code>) 
   * @param isCounter
   *        Is this statistic a counter?  That is, does its value
   *        change monotonically (always increases or always
   *        decreases)? 
   * @param isLargerBetter
   *        True if larger values indicate better performance.
   *
   * @throws IllegalArgumentException
   *         <code>type</code> is not one of <code>int.class</code>,
   *         <code>long.class</code>, or <code>double.class</code>.
   */
  private StatisticDescriptorImpl(String name, byte typeCode,
                                  String description, String unit,
                                  boolean isCounter,
                                  boolean isLargerBetter) {
    this.name = name;
    this.typeCode = typeCode;
    if (description == null) {
      this.description = "";

    } else {
      this.description = description;
    }

    if (unit == null) {
      this.unit = "";

    } else {
      this.unit = unit;
    }
    this.isCounter = isCounter;
    this.isLargerBetter = isLargerBetter;
  }

  ////////////////////  StatisticDescriptor Methods  ////////////////////

  public final String getName() {
    return this.name;
  }

  public final String getDescription() {
    return this.description;
  }

  public final Class<?> getType() {
    return getTypeCodeClass(this.typeCode);
  }

  public final int getStorageBits() {
    return getTypeCodeBits(this.typeCode);
  }

  public final boolean isCounter() {
    return this.isCounter;
  }
  
  public final boolean isLargerBetter() {
    return this.isLargerBetter;
  }
  
  public final String getUnit() {
    return this.unit;
  }

  public final int getId() {
//     if (this.id == INVALID_OFFSET) {
//       String s = "The id has not been initialized yet.";
//       throw new IllegalStateException(s);
//     }

    //Assert.assertTrue(this.id >= 0);
    return this.id;
  }
  
  public final Number getNumberForRawBits(long bits) {
    switch (this.typeCode) {
    case StatisticDescriptorImpl.INT:
      return (int)bits;
    case StatisticDescriptorImpl.LONG:
      return bits;
    case StatisticDescriptorImpl.DOUBLE:
      return Double.longBitsToDouble(bits);
    default:
      throw new RuntimeException(LocalizedStrings.StatisticsImpl_UNEXPECTED_STAT_DESCRIPTOR_TYPE_CODE_0.toLocalizedString(Byte.valueOf(this.typeCode)));
    }
  }
  
  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the type code of this statistic
   */
  public final byte getTypeCode() {
    return this.typeCode;
  }

  /**
   * Sets the id of this descriptor
   */
  final void setId(int id) {
    //Assert.assertTrue(id >= 0);
    this.id = id;
  }

  ////////////////////  Comparable Methods  ////////////////////
  /**
   * <code>StatisticDescriptor</code>s are naturally ordered by their
   * name. 
   *
   * @throws IllegalArgumentException
   *         <code>o</code> is not a <code>StatisticDescriptor</code>
   *
   * @see #getName
   */
  public int compareTo(StatisticDescriptor o) {
    return this.getName().compareTo(o.getName());
  }

  public final int checkInt() {
    if (this.typeCode != INT) {
      throw new IllegalArgumentException(LocalizedStrings.StatisticDescriptorImpl_THE_STATISTIC_0_WITH_ID_1_IS_OF_TYPE_2_AND_IT_WAS_EXPECTED_TO_BE_AN_INT.toLocalizedString(new Object[] {getName(), Integer.valueOf(getId()), StatisticDescriptorImpl.getTypeCodeName(getTypeCode())}));
    }
    return this.id;
  }

  public final int checkLong() {
    if (this.typeCode != LONG) {
      StringBuffer sb = new StringBuffer();
      sb.append("The statistic " + getName() + " with id ");
      sb.append(getId());
      sb.append(" is of type ");
      sb.append(StatisticDescriptorImpl.getTypeCodeName(getTypeCode()));
      
      sb.append(" and it was expected to be a long");
      throw new IllegalArgumentException(sb.toString());
    }
    return this.id;
  }

  public final int checkDouble() {
    if (this.typeCode != DOUBLE) {
      throw new IllegalArgumentException(LocalizedStrings.StatisticDescriptorImpl_THE_STATISTIC_0_WITH_ID_1_IS_OF_TYPE_2_AND_IT_WAS_EXPECTED_TO_BE_A_DOUBLE.toLocalizedString(new Object[] {getName(), Integer.valueOf(getId()), StatisticDescriptorImpl.getTypeCodeName(getTypeCode())}));
    }
    return this.id;
  }


  @Override // GemStoneAddition
  public int hashCode() {
    return getName().hashCode();
  }
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof StatisticDescriptorImpl)) {
      return false;
    }
    StatisticDescriptorImpl other = (StatisticDescriptorImpl)o;
    if (getId() != other.getId()) {
      return false;
    }
    if (!getName().equals(other.getName())) {
      return false;
    }
    if (isCounter() != other.isCounter()) {
      return false;
    }
    if (isLargerBetter() != other.isLargerBetter()) {
      return false;
    }
    if (!getType().equals(other.getType())) {
      return false;
    }
    if (!getUnit().equals(other.getUnit())) {
      return false;
    }
    if (!getDescription().equals(other.getDescription())) {
      return false;
    }
    return true;
  }
}
