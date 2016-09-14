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
package org.apache.geode.internal.statistics;

import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.apache.geode.*;

/**
 * An implementation of {@link Statistics} that does nothing.
 * Setting the "gemfire.statsDisabled" to true causes it to be used.
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

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new statistics instance of the given type
   *
   * @param type
   *        A description of the statistics
   */
  public DummyStatisticsImpl(StatisticsType type, String textId, long numericId) {
    this.type = type;
    this.textId = textId;
    this.numericId = numericId;
  }

  public final void close() {
  }

  ////////////////////////  accessor Methods  ///////////////////////

  public final int nameToId(String name) {
    return this.type.nameToId(name);
  }

  public final StatisticDescriptor nameToDescriptor(String name) {
    return this.type.nameToDescriptor(name);
  }

  public final long getUniqueId() {
    return 0;
  }

  public final StatisticsType getType() {
    return this.type;
  }

  public final String getTextId() {
    return this.textId;
  }
  public final long getNumericId() {
    return this.numericId;
  }
  public final boolean isAtomic() {
    return true;
  }
  public final boolean isClosed() {
    return false;
  }
  
  ////////////////////////  set() Methods  ///////////////////////

  public final void setInt(int id, int value) {
  }

  public final void setInt(StatisticDescriptor descriptor, int value) {
  }

  public final void setInt(String name, int value) {
  }

  public final void setLong(int id, long value) {
  }

  public final void setLong(StatisticDescriptor descriptor, long value) {
  }

  public final void setLong(String name, long value) {
  }

  public final void setDouble(int id, double value) {
  }

  public final void setDouble(StatisticDescriptor descriptor, double value) {
  }

  public final void setDouble(String name, double value) {
  }

  ///////////////////////  get() Methods  ///////////////////////

  public final int getInt(int id) {
    return 0;
  }

  public final int getInt(StatisticDescriptor descriptor) {
    return 0;
  }

  public final int getInt(String name) {
    return 0;
  }

  public final long getLong(int id) {
    return 0;
  }

  public final long getLong(StatisticDescriptor descriptor) {
    return 0;
  }

  public final long getLong(String name) {
    return 0;
  }

  public final double getDouble(int id) {
    return 0.0;
  }

  public final double getDouble(StatisticDescriptor descriptor) {
    return 0.0;
  }

  public final double getDouble(String name) {
    return 0.0;
  }

  private static final Number dummyNumber = Integer.valueOf(0);

  public final Number get(StatisticDescriptor descriptor) {
    return dummyNumber; 
  }

  public final Number get(String name) {
    return dummyNumber; 
  }

  public final long getRawBits(StatisticDescriptor descriptor) {
    return 0;
  }

  public final long getRawBits(String name) {
    return 0;
  }

  ////////////////////////  inc() Methods  ////////////////////////

  public final void incInt(int id, int delta) {
  }

  public final void incInt(StatisticDescriptor descriptor, int delta) {
  }

  public final void incInt(String name, int delta) {
  }

  public final void incLong(int id, long delta) {
  }

  public final void incLong(StatisticDescriptor descriptor, long delta) {
  }

  public final void incLong(String name, long delta) {
  }

  public final void incDouble(int id, double delta) {
  }

  public final void incDouble(StatisticDescriptor descriptor, double delta) {
  }

  public final void incDouble(String name, double delta) {
  }

  @Override
  public IntSupplier setIntSupplier(final int id, final IntSupplier supplier) {
    return null;
  }

  @Override
  public IntSupplier setIntSupplier(final String name, final IntSupplier supplier) {
    return null;
  }

  @Override
  public IntSupplier setIntSupplier(final StatisticDescriptor descriptor, final IntSupplier supplier) {
    return null;
  }

  @Override public LongSupplier setLongSupplier(final int id, final LongSupplier supplier) {
    return null;
  }

  @Override public LongSupplier setLongSupplier(final String name, final LongSupplier supplier) {
    return null;
  }

  @Override public LongSupplier setLongSupplier(final StatisticDescriptor descriptor, final LongSupplier supplier) {
    return null;
  }

  @Override public DoubleSupplier setDoubleSupplier(final int id, final DoubleSupplier supplier) {
    return null;
  }

  @Override public DoubleSupplier setDoubleSupplier(final String name, final DoubleSupplier supplier) {
    return null;
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final StatisticDescriptor descriptor, final DoubleSupplier supplier) {
    return null;
  }
}
