/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import java.util.*;

/**
 * The implementation of {@link StatisticsTypeFactory}.
 * Each VM can any have a single instance of this class which can
 * be accessed by calling {@link #singleton()}.
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 * @author Darrel Schneider
 *
 * @since 3.0
 *
 */
public class StatisticsTypeFactoryImpl implements StatisticsTypeFactory {
  // static fields
  private final static StatisticsTypeFactoryImpl singleton = new StatisticsTypeFactoryImpl();

  // static methods
  /**
   * Returns the single instance of this class.
   */
  public final static StatisticsTypeFactory singleton() {
    return singleton;
  }
  
  protected final static void clear() {
    singleton.statTypes.clear();
  }

  // constructors
  private StatisticsTypeFactoryImpl() {
  }
  
  // instance fields
  private final HashMap statTypes = new HashMap();

  // instance methods
  /**
   * Adds an already created type. If the type has already been
   * added and is equal to the new one then the new one is dropped and the existing one returned.
   * @throws IllegalArgumentException if the type already exists and is not equal to the new type
   */
  public StatisticsType addType(StatisticsType t) {
    StatisticsType result = t;
    synchronized (this.statTypes) {
      StatisticsType currentValue = findType(result.getName());
      if (currentValue == null) {
        this.statTypes.put(result.getName(), result);
      } else if (result.equals(currentValue)) {
        result = currentValue;
      } else {
        throw new IllegalArgumentException(LocalizedStrings.StatisticsTypeFactoryImpl_STATISTICS_TYPE_NAMED_0_ALREADY_EXISTS.toLocalizedString(result.getName()));
      }
    }
    return result;
  }
  public StatisticsType createType(String name, String description,
                                   StatisticDescriptor[] stats) {
    return addType(new StatisticsTypeImpl(name, description, stats));
  }
  public StatisticsType findType(String name) {
    return (StatisticsType)this.statTypes.get(name);
  }
  public StatisticsType[] createTypesFromXml(Reader reader)
    throws IOException {
    return StatisticsTypeImpl.fromXml(reader, this);
  }

  public StatisticDescriptor createIntCounter(String name, String description,
                                              String units) {
    return StatisticDescriptorImpl.createIntCounter(name, description, units, true);
  }
  public StatisticDescriptor createLongCounter(String name, String description,
                                               String units) {
    return StatisticDescriptorImpl.createLongCounter(name, description, units, true);
  }
  public StatisticDescriptor createDoubleCounter(String name, String description,
                                                 String units) {
    return StatisticDescriptorImpl.createDoubleCounter(name, description, units, true);
  }
  public StatisticDescriptor createIntGauge(String name, String description,
                                            String units) {
    return StatisticDescriptorImpl.createIntGauge(name, description, units, false);
  }
  public StatisticDescriptor createLongGauge(String name, String description,
                                             String units) {
    return StatisticDescriptorImpl.createLongGauge(name, description, units, false);
  }
  public StatisticDescriptor createDoubleGauge(String name, String description,
                                               String units) {
    return StatisticDescriptorImpl.createDoubleGauge(name, description, units, false);
  }

  public StatisticDescriptor createIntCounter(String name, String description,
                                              String units, boolean largerBetter) {
    return StatisticDescriptorImpl.createIntCounter(name, description, units, largerBetter);
  }
  public StatisticDescriptor createLongCounter(String name, String description,
                                               String units, boolean largerBetter) {
    return StatisticDescriptorImpl.createLongCounter(name, description, units, largerBetter);
  }
  public StatisticDescriptor createDoubleCounter(String name, String description,
                                                 String units, boolean largerBetter) {
    return StatisticDescriptorImpl.createDoubleCounter(name, description, units, largerBetter);
  }
  public StatisticDescriptor createIntGauge(String name, String description,
                                            String units, boolean largerBetter) {
    return StatisticDescriptorImpl.createIntGauge(name, description, units, largerBetter);
  }
  public StatisticDescriptor createLongGauge(String name, String description,
                                             String units, boolean largerBetter) {
    return StatisticDescriptorImpl.createLongGauge(name, description, units, largerBetter);
  }
  public StatisticDescriptor createDoubleGauge(String name, String description,
                                               String units, boolean largerBetter) {
    return StatisticDescriptorImpl.createDoubleGauge(name, description, units, largerBetter);
  }
}
