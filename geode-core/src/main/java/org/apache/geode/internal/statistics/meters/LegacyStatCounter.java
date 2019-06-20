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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import org.apache.geode.Statistics;

/**
 * Wraps a {@code Counter} to increment and read an associated stat as well as incrementing the
 * counter.
 */
public class LegacyStatCounter implements Counter {
  private final Counter underlyingCounter;
  private final StatisticBinding statisticBinding;

  /**
   * Creates a {@code LegacyStatCounter} that wraps the given counter to add the ability
   * to increment and read an associated stat.
   *
   * @param underlyingCounter the counter to wrap
   * @param statisticBinding associates the {@code LegacyStatCounter} with a stat
   */
  private LegacyStatCounter(Counter underlyingCounter,
      StatisticBinding statisticBinding) {
    this.underlyingCounter = underlyingCounter;
    this.statisticBinding = statisticBinding;
  }

  /**
   * Increments both the underlying counter and the associated stat by the given amount.
   */
  @Override
  public void increment(double amount) {
    underlyingCounter.increment(amount);
    statisticBinding.add(amount);
  }

  /**
   * Returns the value of the associated stat.
   */
  @Override
  public double count() {
    return statisticBinding.doubleValue();
  }

  /**
   * Returns the ID of the underlying counter.
   */
  @Override
  public Id getId() {
    return underlyingCounter.getId();
  }

  /**
   * Returns a builder that can associate a stat with the eventual {@code LegacyStatCounter}. To
   * associate the counter with a stat, call one of
   * {@link Builder#doubleStatistic(Statistics, int)} or
   * {@link Builder#longStatistic(Statistics, int)}.
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  public static class Builder {
    private final Counter.Builder builder;
    private StatisticBinding statisticBinding = StatisticBinding.noOp();

    private Builder(String name) {
      builder = Counter.builder(name);
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatCounter} with the specified {@code
     * double} stat. The given {@code Statistics} and {@code statId} must identify a {@code
     * double} stat.
     */
    public Builder doubleStatistic(Statistics statistics, int statId) {
      statisticBinding = new DoubleStatisticBinding(statistics, statId);
      return this;
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatCounter} with the specified {@code
     * int} stat. The given {@code Statistics} and {@code statId} must identify an {@code int}
     * stat.
     */
    public Builder intStatistic(Statistics statistics, int statId) {
      statisticBinding = new IntStatisticBinding(statistics, statId);
      return this;
    }

    /**
     * Prepares to associate the eventual {@code LegacyStatCounter} with the specified {@code
     * long} stat. The given {@code Statistics} and {@code statId} must identify a {@code long}
     * stat.
     */
    public Builder longStatistic(Statistics statistics, int statId) {
      statisticBinding = new LongStatisticBinding(statistics, statId);
      return this;
    }

    public Builder baseUnit(String unit) {
      builder.baseUnit(unit);
      return this;
    }

    public Builder description(String description) {
      builder.description(description);
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

    public Builder tags(Iterable<Tag> tags) {
      builder.tags(tags);
      return this;
    }

    /**
     * Registers a {@code Counter} with the given registry, and returns a {@code LegacyStatCounter}
     * that wraps the counter to increment and read the associated stat. Note that the returned
     * {@code LegacyStatCounter} is not registered with the registry, but it has the same ID, so
     * it can be used to remove the registered counter from the registry.
     */
    public LegacyStatCounter register(MeterRegistry registry) {
      Counter underlyingCounter = builder.register(registry);
      return new LegacyStatCounter(underlyingCounter, statisticBinding);
    }
  }
}
