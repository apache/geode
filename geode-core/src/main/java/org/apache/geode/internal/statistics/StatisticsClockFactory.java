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

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.apache.geode.annotations.Immutable;

public class StatisticsClockFactory {

  @Immutable
  public static final String ENABLE_CLOCK_STATS_PROPERTY = "enableClockStats";

  @Immutable
  public static final boolean enableClockStats = Boolean.getBoolean(ENABLE_CLOCK_STATS_PROPERTY);

  /**
   * TODO: delete getTimeIfEnabled
   */
  @Deprecated
  public static long getTimeIfEnabled() {
    return enableClockStats ? getTime() : 0;
  }

  public static long getTime() {
    return System.nanoTime();
  }

  /**
   * Creates new {@code StatisticsClock} using {@code enableClockStats} system property.
   */
  public static StatisticsClock clock() {
    return clock(Boolean.getBoolean(ENABLE_CLOCK_STATS_PROPERTY));
  }

  /**
   * Creates new {@code StatisticsClock} using specified boolean value.
   */
  public static StatisticsClock clock(boolean enabled) {
    if (enabled) {
      return enabledClock(StatisticsClockFactory::getTime);
    }
    return disabledClock();
  }

  public static StatisticsClock enabledClock() {
    return clock(StatisticsClockFactory::getTime, () -> true);
  }

  public static StatisticsClock enabledClock(LongSupplier time) {
    return clock(time::getAsLong, () -> true);
  }

  public static StatisticsClock disabledClock() {
    return clock(() -> 0, () -> false);
  }

  public static StatisticsClock clock(LongSupplier time, BooleanSupplier isEnabled) {
    return new StatisticsClock() {
      @Override
      public long getTime() {
        return time.getAsLong();
      }

      @Override
      public boolean isEnabled() {
        return isEnabled.getAsBoolean();
      }
    };
  }

}
