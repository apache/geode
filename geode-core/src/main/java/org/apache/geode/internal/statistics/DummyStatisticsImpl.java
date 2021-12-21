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

import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

/**
 * An implementation of {@link Statistics} that does nothing. Setting the "gemfire.statsDisabled" to
 * true causes it to be used.
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 *
 */
public class DummyStatisticsImpl implements Statistics {

  private final StatisticsType type;
  private final String textId;
  private final long numericId;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new statistics instance of the given type
   *
   * @param type A description of the statistics
   */
  public DummyStatisticsImpl(StatisticsType type, String textId, long numericId) {
    this.type = type;
    this.textId = textId;
    this.numericId = numericId;
  }

  @Override
  public void close() {}

  //////////////////////// accessor Methods ///////////////////////

  @Override
  public int nameToId(String name) {
    return type.nameToId(name);
  }

  @Override
  public StatisticDescriptor nameToDescriptor(String name) {
    return type.nameToDescriptor(name);
  }

  @Override
  public long getUniqueId() {
    return 0;
  }

  @Override
  public StatisticsType getType() {
    return type;
  }

  @Override
  public String getTextId() {
    return textId;
  }

  @Override
  public long getNumericId() {
    return numericId;
  }

  @Override
  public boolean isAtomic() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  //////////////////////// set() Methods ///////////////////////

  @Override
  public void setInt(int id, int value) {}

  @Override
  public void setInt(StatisticDescriptor descriptor, int value) {}

  @Override
  public void setInt(String name, int value) {}

  @Override
  public void setLong(int id, long value) {}

  @Override
  public void setLong(StatisticDescriptor descriptor, long value) {}

  @Override
  public void setLong(String name, long value) {}

  @Override
  public void setDouble(int id, double value) {}

  @Override
  public void setDouble(StatisticDescriptor descriptor, double value) {}

  @Override
  public void setDouble(String name, double value) {}

  /////////////////////// get() Methods ///////////////////////

  @Override
  public int getInt(int id) {
    return 0;
  }

  @Override
  public int getInt(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public int getInt(String name) {
    return 0;
  }

  @Override
  public long getLong(int id) {
    return 0;
  }

  @Override
  public long getLong(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public long getLong(String name) {
    return 0;
  }

  @Override
  public double getDouble(int id) {
    return 0.0;
  }

  @Override
  public double getDouble(StatisticDescriptor descriptor) {
    return 0.0;
  }

  @Override
  public double getDouble(String name) {
    return 0.0;
  }

  private static final Number dummyNumber = Integer.valueOf(0);

  @Override
  public Number get(StatisticDescriptor descriptor) {
    return dummyNumber;
  }

  @Override
  public Number get(String name) {
    return dummyNumber;
  }

  @Override
  public long getRawBits(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public long getRawBits(String name) {
    return 0;
  }

  //////////////////////// inc() Methods ////////////////////////

  @Override
  public void incInt(int id, int delta) {}

  @Override
  public void incInt(StatisticDescriptor descriptor, int delta) {}

  @Override
  public void incInt(String name, int delta) {}

  @Override
  public void incLong(int id, long delta) {}

  @Override
  public void incLong(StatisticDescriptor descriptor, long delta) {}

  @Override
  public void incLong(String name, long delta) {}

  @Override
  public void incDouble(int id, double delta) {}

  @Override
  public void incDouble(StatisticDescriptor descriptor, double delta) {}

  @Override
  public void incDouble(String name, double delta) {}

  @Override
  public IntSupplier setIntSupplier(final int id, final IntSupplier supplier) {
    return null;
  }

  @Override
  public IntSupplier setIntSupplier(final String name, final IntSupplier supplier) {
    return null;
  }

  @Override
  public IntSupplier setIntSupplier(final StatisticDescriptor descriptor,
      final IntSupplier supplier) {
    return null;
  }

  @Override
  public LongSupplier setLongSupplier(final int id, final LongSupplier supplier) {
    return null;
  }

  @Override
  public LongSupplier setLongSupplier(final String name, final LongSupplier supplier) {
    return null;
  }

  @Override
  public LongSupplier setLongSupplier(final StatisticDescriptor descriptor,
      final LongSupplier supplier) {
    return null;
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final int id, final DoubleSupplier supplier) {
    return null;
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final String name, final DoubleSupplier supplier) {
    return null;
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final StatisticDescriptor descriptor,
      final DoubleSupplier supplier) {
    return null;
  }
}
