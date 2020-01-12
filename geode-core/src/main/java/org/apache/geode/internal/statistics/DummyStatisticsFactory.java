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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.annotations.Immutable;

/**
 * A StatisticsFactory that creates disconnected statistics
 */
public class DummyStatisticsFactory implements StatisticsFactory {

  @Immutable
  private static final StatisticsTypeFactoryImpl tf =
      (StatisticsTypeFactoryImpl) StatisticsTypeFactoryImpl.singleton();

  /** Creates a new instance of DummyStatisticsFactory */
  public DummyStatisticsFactory() {}

  // StatisticsFactory methods
  @Override
  public Statistics createStatistics(StatisticsType type) {
    return createStatistics(type, null, 1);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId) {
    return createStatistics(type, textId, 1);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    Statistics result = new LocalStatisticsImpl(type, textId, numericId, 1, false, 0, null);
    return result;
  }

  // /** for internal use only. Its called by {@link LocalStatisticsImpl#close}. */
  // public void destroyStatistics(Statistics stats) {
  // if (statsList.remove(stats)) {
  // statsListModCount++;
  // }
  // }
  //
  @Override
  public Statistics createAtomicStatistics(StatisticsType type) {
    return createAtomicStatistics(type, null, 1);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return createAtomicStatistics(type, textId, 1);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    Statistics result = new LocalStatisticsImpl(type, textId, numericId, 1, true, 0, null);
    return result;
  }

  @Override
  public Statistics[] findStatisticsByType(StatisticsType type) {
    return new Statistics[0];
  }

  @Override
  public Statistics[] findStatisticsByTextId(String textId) {
    return new Statistics[0];
  }

  @Override
  public Statistics[] findStatisticsByNumericId(long numericId) {
    return new Statistics[0];
  }

  @Override
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return tf.createType(name, description, stats);
  }

  @Override
  public StatisticsType findType(String name) {
    return tf.findType(name);
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return tf.createTypesFromXml(reader);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return tf.createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return tf.createDoubleCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return tf.createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return tf.createDoubleGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units,
      boolean largerBetter) {
    return createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units,
      boolean largerBetter) {
    return tf.createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
      boolean largerBetter) {
    return tf.createDoubleCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units,
      boolean largerBetter) {
    return createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units,
      boolean largerBetter) {
    return tf.createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
      boolean largerBetter) {
    return tf.createDoubleGauge(name, description, units, largerBetter);
  }

}
