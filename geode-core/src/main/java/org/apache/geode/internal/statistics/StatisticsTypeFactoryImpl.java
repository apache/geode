/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.statistics;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;

/**
 * The implementation of {@link StatisticsTypeFactory}. Each VM can any have a single instance of
 * this class which can be accessed by calling {@link #singleton()}.
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 *
 */
public class StatisticsTypeFactoryImpl implements StatisticsTypeFactory {
  // static fields
  @Immutable
  private static final StatisticsTypeFactoryImpl singleton = new StatisticsTypeFactoryImpl();

  // static methods
  /**
   * Returns the single instance of this class.
   */
  public static StatisticsTypeFactory singleton() {
    return singleton;
  }

  protected static void clear() {
    singleton.statTypes.clear();
  }

  // constructors
  private StatisticsTypeFactoryImpl() {}

  // instance fields
  private final HashMap statTypes = new HashMap();

  // instance methods
  /**
   * Adds an already created type. If the type has already been added and is equal to the new one
   * then the new one is dropped and the existing one returned.
   *
   * @throws IllegalArgumentException if the type already exists and is not equal to the new type
   */
  public StatisticsType addType(StatisticsType t) {
    StatisticsType result = t;
    synchronized (statTypes) {
      StatisticsType currentValue = findType(result.getName());
      if (currentValue == null) {
        statTypes.put(result.getName(), result);
      } else if (result.equals(currentValue)) {
        result = currentValue;
      } else {
        throw new IllegalArgumentException(
            String.format("Statistics type named %s already exists.",
                result.getName()));
      }
    }
    return result;
  }

  @Override
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return addType(new StatisticsTypeImpl(name, description, stats));
  }

  @Override
  public StatisticsType findType(String name) {
    return (StatisticsType) statTypes.get(name);
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return StatisticsTypeImpl.fromXml(reader, this);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return StatisticDescriptorImpl.createLongCounter(name, description, units, true);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return StatisticDescriptorImpl.createDoubleCounter(name, description, units, true);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return StatisticDescriptorImpl.createLongGauge(name, description, units, false);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return StatisticDescriptorImpl.createDoubleGauge(name, description, units, false);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units,
      boolean largerBetter) {
    return createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units,
      boolean largerBetter) {
    return StatisticDescriptorImpl.createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
      boolean largerBetter) {
    return StatisticDescriptorImpl.createDoubleCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units,
      boolean largerBetter) {
    return createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units,
      boolean largerBetter) {
    return StatisticDescriptorImpl.createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
      boolean largerBetter) {
    return StatisticDescriptorImpl.createDoubleGauge(name, description, units, largerBetter);
  }
}
