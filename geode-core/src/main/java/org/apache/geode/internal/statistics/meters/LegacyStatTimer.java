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

package org.apache.geode.internal.statistics.meters;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;

import org.apache.geode.Statistics;

/**
 * Wraps a {@code Timer} to increment and read associated stats as well as updating the
 * timer.
 */
public class LegacyStatTimer implements Timer {
  private final Clock clock;
  private final Timer underlyingTimer;
  private final StatisticBinding countStatisticBinding;
  private final StatisticBinding timeStatisticBinding;

  /**
   * Creates a {@code LegacyStatTimer} that wraps the given timer to add the ability
   * to increment and read associated stats.
   *
   * @param clock the clock to use to determine the current time
   * @param underlyingTimer the timer to wrap
   * @param countStatisticBinding associates the timer's count with a stat
   * @param timeStatisticBinding associates the timer's total time with a stat
   */
  private LegacyStatTimer(Clock clock, Timer underlyingTimer,
      StatisticBinding countStatisticBinding,
      StatisticBinding timeStatisticBinding) {
    this.clock = clock;
    this.underlyingTimer = underlyingTimer;
    this.countStatisticBinding = countStatisticBinding;
    this.timeStatisticBinding = timeStatisticBinding;
  }

  /**
   * Returns the ID of the underlying timer.
   */
  @Override
  public Id getId() {
    return underlyingTimer.getId();
  }

  /**
   * Returns the base time unit of the underlying timer.
   */
  @Override
  public TimeUnit baseTimeUnit() {
    return underlyingTimer.baseTimeUnit();
  }

  /**
   * Records the event to both the underlying timer and the associated stats.
   */
  @Override
  public void record(long amount, TimeUnit unit) {
    underlyingTimer.record(amount, unit);
    countStatisticBinding.add(1);
    timeStatisticBinding.add(NANOSECONDS.convert(amount, unit));
  }

  /**
   * Executes the given supplier, records its duration, and returns the supplied value.
   */
  @Override
  public <T> T record(Supplier<T> supplier) {
    final long s = clock.monotonicTime();
    try {
      return supplier.get();
    } finally {
      final long e = clock.monotonicTime();
      record(e - s, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Executes the given callable, records its duration, and returns the supplied value.
   */
  @Override
  public <T> T recordCallable(Callable<T> callable) throws Exception {
    final long s = clock.monotonicTime();
    try {
      return callable.call();
    } finally {
      final long e = clock.monotonicTime();
      record(e - s, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Executes the given runnable and records its duration.
   */
  @Override
  public void record(Runnable runnable) {
    final long s = clock.monotonicTime();
    try {
      runnable.run();
    } finally {
      final long e = clock.monotonicTime();
      record(e - s, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Returns the value of the associated count stat.
   */
  @Override
  public long count() {
    return countStatisticBinding.longValue();
  }

  /**
   * Returns the value of the associated time stat, converted to the given time unit.
   */
  @Override
  public double totalTime(TimeUnit unit) {
    return unit.convert(timeStatisticBinding.longValue(), NANOSECONDS);
  }

  /**
   * This method is not supported in this implementation.
   */
  @Override
  public double max(TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method is not supported in this implementation.
   */
  @Override
  public HistogramSnapshot takeSnapshot() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a builder that can associate a pair of stats with the eventual {@code
   * LegacyStatTimer}. To associate the timer's count with a stat, call one of
   * {@link Builder#doubleCountStatistic(Statistics, int)} or
   * {@link Builder#longCountStatistic(Statistics, int)}. To associate the
   * timer's total time with a stat, call one of
   * {@link Builder#doubleTimeStatistic(Statistics, int)} or
   * {@link Builder#longTimeStatistic(Statistics, int)}.
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  public static class Builder {
    private final Timer.Builder builder;
    private StatisticBinding countStatisticBinding = StatisticBinding.noOp();
    private StatisticBinding timeStatisticBinding = StatisticBinding.noOp();

    private Builder(String name) {
      builder = Timer.builder(name);
    }

    public Builder description(String description) {
      builder.description((description));
      return this;
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatTimer}'s count with the specified
     * {@code double} stat. The given {@code Statistics} and {@code statId} must identify a
     * {@code double} stat.
     */
    public Builder doubleCountStatistic(Statistics statistics, int statId) {
      countStatisticBinding = new DoubleStatisticBinding(statistics, statId);
      return this;
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatTimer}'s count with the specified
     * {@code long} stat. The given {@code Statistics} and {@code statId} must identify a {@code
     * long} stat.
     */
    public Builder longCountStatistic(Statistics statistics, int statId) {
      countStatisticBinding = new LongStatisticBinding(statistics, statId);
      return this;
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatTimer}'s total time with the specified
     * {@code double} stat. The given {@code Statistics} and {@code statId} must identify a
     * {@code double} stat.
     */
    public Builder doubleTimeStatistic(Statistics statistics, int statId) {
      timeStatisticBinding = new DoubleStatisticBinding(statistics, statId);
      return this;
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatTimer}'s total time with the specified
     * {@code long} stat. The given {@code Statistics} and {@code statId} must identify a {@code
     * long} stat.
     */
    public Builder longTimeStatistic(Statistics statistics, int statId) {
      timeStatisticBinding = new LongStatisticBinding(statistics, statId);
      return this;
    }

    public Builder tags(Iterable<Tag> tags) {
      builder.tags(tags);
      return this;
    }

    public Builder tag(String name, String value) {
      builder.tag(name, value);
      return this;
    }

    public Builder tags(String... tags) {
      builder.tags(tags);
      return this;
    }

    /**
     * Registers a {@code Timer} with the given registry, and returns a {@code LegacyStatTimer}
     * that wraps the timer to update and read the associated stats. Note that the returned
     * {@code LegacyStatTimer} is not registered with the registry, but it has the same ID, so
     * it can be used to remove the registered timer from the registry.
     */
    public Timer register(MeterRegistry registry) {
      Clock clock = registry.config().clock();
      Timer underlyingTimer = builder.register(registry);
      return new LegacyStatTimer(clock, underlyingTimer, countStatisticBinding,
          timeStatisticBinding);
    }
  }
}
