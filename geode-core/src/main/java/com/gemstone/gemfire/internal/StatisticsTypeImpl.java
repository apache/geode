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

import java.io.*;
import java.util.*;

/**
 * Gathers together a number of {@link StatisticDescriptor statistics}
 * into one logical type.
 *
 * @see Statistics
 *
 *
 * @since GemFire 3.0
 */
public class StatisticsTypeImpl implements StatisticsType {

  /** The name of this statistics type */
  private final String name;

  /** The description of this statistics type */
  private final String description;

  /** The descriptions of the statistics in id order */
  private final StatisticDescriptor[] stats;

  /** Maps a stat name to its StatisticDescriptor */
  private final HashMap statsMap;

  /** Contains the number of 32-bit statistics in this type. */
  private final int intStatCount;

  /** Contains the number of long statistics in this type. */
  private final int longStatCount;

  /** Contains the number of double statistics in this type. */
  private final int doubleStatCount;

  /////////////////////  Static Methods  /////////////////////

  /**
   * @see StatisticsTypeXml#read(Reader, StatisticsTypeFactory)
   */
  public static StatisticsType[] fromXml(Reader reader,
                                         StatisticsTypeFactory factory)
    throws IOException {
    return (new StatisticsTypeXml()).read(reader, factory);
  }

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>StatisticsType</code> with the given name,
   * description, and statistics.
   *
   * @param name
   *        The name of this statistics type (for example,
   *        <code>"DatabaseStatistics"</code>)
   * @param description
   *        A description of this statistics type (for example,
   *        "Information about the application's use of the
   *        database").
   * @param stats
   *        Descriptions of the individual statistics grouped together
   *        in this statistics type.
   *
   * @throws NullPointerException
   *         If either <code>name</code> or <code>stats</code> is
   *         <code>null</code>.
   */
  public StatisticsTypeImpl(String name, String description,
                            StatisticDescriptor[] stats) {
    this(name, description, stats, false);
  }

  /**
   * Creates a new <code>StatisticsType</code> with the given name,
   * description, and statistics.
   *
   * @param name
   *        The name of this statistics type (for example,
   *        <code>"DatabaseStatistics"</code>)
   * @param description
   *        A description of this statistics type (for example,
   *        "Information about the application's use of the
   *        database").
   * @param stats
   *        Descriptions of the individual statistics grouped together
   *        in this statistics type.
   * @param wrapsSharedClass
   *        True if this type is a wrapper around a SharedClass??.
   *        False if its a dynamic type created at run time.        
   *
   * @throws NullPointerException
   *         If either <code>name</code> or <code>stats</code> is
   *         <code>null</code>.
   */
  public StatisticsTypeImpl(String name, String description,
                            StatisticDescriptor[] stats, boolean wrapsSharedClass) {
    if (name == null) {
      throw new NullPointerException(LocalizedStrings.StatisticsTypeImpl_CANNOT_HAVE_A_NULL_STATISTICS_TYPE_NAME.toLocalizedString());
    }

    if (stats == null) {
      throw new NullPointerException(LocalizedStrings.StatisticsTypeImpl_CANNOT_HAVE_A_NULL_STATISTIC_DESCRIPTORS.toLocalizedString());
    }
    if (stats.length > StatisticsTypeFactory.MAX_DESCRIPTORS_PER_TYPE) {
      throw new IllegalArgumentException(LocalizedStrings.StatisticsTypeImpl_THE_REQUESTED_DESCRIPTOR_COUNT_0_EXCEEDS_THE_MAXIMUM_WHICH_IS_1.toLocalizedString(new Object[] {Integer.valueOf(stats.length), Integer.valueOf(StatisticsTypeFactory.MAX_DESCRIPTORS_PER_TYPE)}));
    }

    this.name = name;
    this.description = description;
    this.stats = stats;
    this.statsMap = new HashMap(stats.length*2);
    int intCount = 0;
    int longCount = 0;
    int doubleCount = 0;
    for (int i=0; i < stats.length; i++) {
      StatisticDescriptorImpl sd = (StatisticDescriptorImpl)stats[i];
      if (sd.getTypeCode() == StatisticDescriptorImpl.INT) {
        if (!wrapsSharedClass) {
          sd.setId(intCount);
        }
        intCount++;
      } else if (sd.getTypeCode() == StatisticDescriptorImpl.LONG) {
        if (!wrapsSharedClass) {
          sd.setId(longCount);
        }
        longCount++;
      } else if (sd.getTypeCode() == StatisticDescriptorImpl.DOUBLE) {
        if (!wrapsSharedClass) {
          sd.setId(doubleCount);
        }
        doubleCount++;
      }
      Object previousValue = statsMap.put(stats[i].getName(), sd);
      if (previousValue != null) {
        throw new IllegalArgumentException(LocalizedStrings.StatisticsTypeImpl_DUPLICATE_STATISTICDESCRIPTOR_NAMED_0.toLocalizedString(stats[i].getName()));
      }
    }
    this.intStatCount = intCount;
    this.longStatCount = longCount;
    this.doubleStatCount = doubleCount;
  }

  //////////////////////  StatisticsType Methods  //////////////////////

  public final String getName() {
    return this.name;
  }

  public final String getDescription() {
    return this.description;
  }

  public final StatisticDescriptor[] getStatistics() {
    return this.stats;
  }
  
  public final int nameToId(String name) {
    return nameToDescriptor(name).getId();
  }

  public final StatisticDescriptor nameToDescriptor(String name) {
    StatisticDescriptorImpl stat = (StatisticDescriptorImpl)statsMap.get(name);
    if (stat == null) {
      throw new IllegalArgumentException(LocalizedStrings.StatisticsTypeImpl_THERE_IS_NO_STATISTIC_NAMED_0.toLocalizedString(name));
    }
    return stat;
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Gets the number of statistics in this type that are ints.
   */
  public final int getIntStatCount() {
    return this.intStatCount;
  }
  /**
   * Gets the number of statistics in this type that are longs.
   */
  public final int getLongStatCount() {
    return this.longStatCount;
  }
  /**
   * Gets the number of statistics that are doubles.
   */
  public final int getDoubleStatCount() {
    return this.doubleStatCount;
  }

//  @Override
//  public String toString() {
//    return "StatisticType with " + this.stats.length + " stats";
//  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("name=").append(this.name);
    sb.append(", description=").append(this.description);
    sb.append(", stats.length=").append(this.stats.length);
    sb.append("}");
    return sb.toString();
  }
  
  @Override
  public int hashCode() {
    return getName().hashCode();
  }
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof StatisticsType)) {
      return false;
    }
    StatisticsType other = (StatisticsType)o;
    if (!getName().equals(other.getName())) {
      return false;
    }
    if (!getDescription().equals(other.getDescription())) {
      return false;
    }
    StatisticDescriptor[] myStats = getStatistics();
    StatisticDescriptor[] yourStats = other.getStatistics();
    if (myStats.length != yourStats.length) {
      return false;
    }
    for (int i=0; i < myStats.length; i++) {
      if (!myStats[i].equals(yourStats[i])) {
        return false;
      }
    }
    return true;
  }
}
