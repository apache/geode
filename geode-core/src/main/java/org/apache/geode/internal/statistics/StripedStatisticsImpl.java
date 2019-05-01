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

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import org.apache.geode.StatisticsType;

/**
 * Stripes statistic counters across threads to reduce contention using {@link LongAdder} and
 * {@link DoubleAdder}.
 */
public class StripedStatisticsImpl extends StatisticsImpl {

  private final LongAdder[] intAdders;
  private final LongAdder[] longAdders;
  private final DoubleAdder[] doubleAdders;

  public StripedStatisticsImpl(StatisticsType type, String textId, long numericId,
      long uniqueId, StatisticsManager statisticsManager) {
    super(type, textId, numericId, uniqueId, 0, statisticsManager);

    StatisticsTypeImpl realType = (StatisticsTypeImpl) type;

    this.intAdders =
        Stream.generate(LongAdder::new)
            .limit(realType.getIntStatCount())
            .toArray(LongAdder[]::new);
    this.longAdders =
        Stream.generate(LongAdder::new)
            .limit(realType.getLongStatCount())
            .toArray(LongAdder[]::new);
    this.doubleAdders =
        Stream.generate(DoubleAdder::new)
            .limit(realType.getDoubleStatCount())
            .toArray(DoubleAdder[]::new);
  }

  @Override
  public boolean isAtomic() {
    return true;
  }

  @Override
  protected void _setInt(int offset, int value) {
    synchronized (intAdders[offset]) {
      intAdders[offset].reset();
      intAdders[offset].add(value);
    }
  }

  @Override
  protected void _setLong(int offset, long value) {
    synchronized (longAdders[offset]) {
      longAdders[offset].reset();
      longAdders[offset].add(value);
    }
  }

  @Override
  protected void _setDouble(int offset, double value) {
    synchronized (doubleAdders[offset]) {
      doubleAdders[offset].reset();
      doubleAdders[offset].add(value);
    }
  }

  @Override
  protected int _getInt(int offset) {
    synchronized (intAdders[offset]) {
      return intAdders[offset].intValue();
    }
  }

  @Override
  protected long _getLong(int offset) {
    synchronized (longAdders[offset]) {
      return longAdders[offset].sum();
    }
  }

  @Override
  protected double _getDouble(int offset) {
    synchronized (doubleAdders[offset]) {
      return doubleAdders[offset].sum();
    }
  }

  @Override
  protected void _incInt(int offset, int delta) {
    intAdders[offset].add(delta);
  }

  @Override
  protected void _incLong(int offset, long delta) {
    longAdders[offset].add(delta);
  }

  @Override
  protected void _incDouble(int offset, double delta) {
    doubleAdders[offset].add(delta);
  }
}
