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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;

/**
 * Statistics implementation base on {@link LongAdder} and {@link DoubleAdder} for counters and
 * {@link AtomicLong} for gauges.
 *
 * Care should be taken to not call any of the set methods on counters or {@link
 * IllegalArgumentException} will be thrown.
 */
public final class FastStatisticsImpl extends StatisticsImpl {

  private interface LongMeter {
    void set(long value);

    void add(long delta);

    long get();
  }

  private interface DoubleMeter {
    void set(double value);

    void add(double delta);

    double get();
  }

  private static final class LongGauge implements LongMeter {
    final AtomicLong value = new AtomicLong();

    @Override
    public void set(final long value) {
      this.value.set(value);
    }

    @Override
    public void add(final long delta) {
      value.addAndGet(delta);
    }

    @Override
    public long get() {
      return value.get();
    }
  }

  private static final class DoubleGauge implements DoubleMeter {
    final AtomicLong value = new AtomicLong();

    @Override
    public void set(final double value) {
      this.value.set(Double.doubleToLongBits(value));
    }

    @Override
    public void add(final double delta) {
      value.updateAndGet(
          current -> Double.doubleToLongBits(Double.longBitsToDouble(current) + delta));
    }

    @Override
    public double get() {
      return Double.longBitsToDouble(value.get());
    }
  }

  private static final class LongCounter implements LongMeter {
    final LongAdder value = new LongAdder();

    @Override
    public void set(final long value) {
      throw new IllegalArgumentException("Counters cannot be set.");
    }

    @Override
    public void add(final long delta) {
      value.add(delta);
    }

    @Override
    public long get() {
      return value.sum();
    }
  }

  private static final class DoubleCounter implements DoubleMeter {
    final DoubleAdder value = new DoubleAdder();

    @Override
    public void set(final double value) {
      throw new IllegalArgumentException("Counters cannot be set.");
    }

    @Override
    public void add(final double delta) {
      value.add(delta);
    }

    @Override
    public double get() {
      return value.sum();
    }
  }

  private final LongMeter[] intMeters;
  private final LongMeter[] longMeters;
  private final DoubleMeter[] doubleMeters;

  public FastStatisticsImpl(final StatisticsType type, final String textId, final long numericId,
      final long uniqueId, final StatisticsManager statisticsManager) {
    super(type, textId, numericId, uniqueId, 0, statisticsManager);

    final StatisticsTypeImpl realType = (StatisticsTypeImpl) type;

    intMeters = new LongMeter[realType.getIntStatCount()];
    longMeters = new LongMeter[realType.getLongStatCount()];
    doubleMeters = new DoubleMeter[realType.getDoubleStatCount()];

    for (final StatisticDescriptor statisticDescriptor : realType.getStatistics()) {
      if (statisticDescriptor.getType() == int.class) {
        intMeters[statisticDescriptor.getId()] = createLongMeter(statisticDescriptor);
      } else if (statisticDescriptor.getType() == long.class) {
        longMeters[statisticDescriptor.getId()] = createLongMeter(statisticDescriptor);
      } else if (statisticDescriptor.getType() == double.class) {
        doubleMeters[statisticDescriptor.getId()] = createDoubleMeter(statisticDescriptor);
      }
    }
  }

  private LongMeter createLongMeter(final StatisticDescriptor statisticDescriptor) {
    return statisticDescriptor.isCounter() ? new LongCounter() : new LongGauge();
  }

  private DoubleMeter createDoubleMeter(final StatisticDescriptor statisticDescriptor) {
    return statisticDescriptor.isCounter() ? new DoubleCounter() : new DoubleGauge();
  }

  @Override
  public boolean isAtomic() {
    return true;
  }

  @Override
  protected void _setInt(final int offset, final int value) {
    intMeters[offset].set(value);
  }

  @Override
  protected void _setLong(final int offset, final long value) {
    longMeters[offset].set(value);
  }

  @Override
  protected void _setDouble(final int offset, final double value) {
    doubleMeters[offset].set(value);
  }

  @Override
  protected int _getInt(final int offset) {
    return (int) intMeters[offset].get();
  }

  @Override
  protected long _getLong(final int offset) {
    return longMeters[offset].get();
  }

  @Override
  protected double _getDouble(final int offset) {
    return doubleMeters[offset].get();
  }

  @Override
  protected void _incInt(final int offset, final int delta) {
    intMeters[offset].add(delta);
  }

  @Override
  protected void _incLong(final int offset, final long delta) {
    longMeters[offset].add(delta);
  }

  @Override
  protected void _incDouble(final int offset, final double delta) {
    doubleMeters[offset].add(delta);
  }
}
