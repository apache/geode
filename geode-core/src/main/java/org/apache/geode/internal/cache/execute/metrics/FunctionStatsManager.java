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
package org.apache.geode.internal.cache.execute.metrics;

import static org.apache.geode.internal.lang.JavaWorkarounds.computeIfAbsent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.statistics.DummyStatisticsImpl;
import org.apache.geode.metrics.internal.NoopMeterRegistry;

public class FunctionStatsManager {

  private final boolean statsDisabled;
  private final StatisticsFactory statisticsFactory;
  private final FunctionServiceStats functionServiceStats;
  private final Supplier<MeterRegistry> meterRegistrySupplier;
  private final Map<String, FunctionStats> functionExecutionStatsMap;
  private final Statistics dummyStatistics;
  private final MeterRegistry noopMeterRegistry;
  private final FunctionStats dummyFunctionStats;

  public FunctionStatsManager(boolean statsDisabled, StatisticsFactory statisticsFactory,
      Supplier<MeterRegistry> meterRegistrySupplier) {
    this(statsDisabled, statisticsFactory,
        new FunctionServiceStats(statisticsFactory, "FunctionExecution"), meterRegistrySupplier);
  }

  @VisibleForTesting
  FunctionStatsManager(boolean statsDisabled, StatisticsFactory statisticsFactory,
      FunctionServiceStats functionServiceStats, Supplier<MeterRegistry> meterRegistrySupplier) {
    this.statsDisabled = statsDisabled;
    this.statisticsFactory = statisticsFactory;
    this.functionServiceStats = functionServiceStats;
    this.meterRegistrySupplier = meterRegistrySupplier;

    functionExecutionStatsMap = new ConcurrentHashMap<>();
    dummyStatistics = createDummyStatistics();
    noopMeterRegistry = createNoopMeterRegistry();
    dummyFunctionStats = createDummyFunctionStats(noopMeterRegistry, dummyStatistics);
  }

  public FunctionServiceStats getFunctionServiceStats() {
    return functionServiceStats;
  }

  public FunctionStats getFunctionStatsByName(String name) {
    MeterRegistry meterRegistryFromSupplier = meterRegistrySupplier.get();

    if (statsDisabled && meterRegistryFromSupplier == null) {
      return dummyFunctionStats;
    }

    return computeIfAbsent(functionExecutionStatsMap, name,
        key -> create(key, meterRegistryFromSupplier));
  }

  public void close() {
    for (FunctionStats functionstats : functionExecutionStatsMap.values()) {
      functionstats.close();
    }
    functionServiceStats.close();
  }

  private FunctionStats create(String name, MeterRegistry meterRegistryFromSupplier) {
    MeterRegistry meterRegistry = meterRegistryFromSupplier == null
        ? noopMeterRegistry
        : meterRegistryFromSupplier;

    Statistics statistics = statsDisabled
        ? dummyStatistics
        : statisticsFactory.createAtomicStatistics(FunctionStatsImpl.getStatisticsType(), name);

    return new FunctionStatsImpl(name, meterRegistry, statistics, functionServiceStats);
  }

  /**
   * Returns the Function Stats for the given function
   *
   * @param functionID represents the function for which we are returning the function Stats
   * @param ds represents the Distributed System
   * @return object of the FunctionStats
   */
  public static FunctionStats getFunctionStats(String functionID, InternalDistributedSystem ds) {
    return ds.getFunctionStatsManager().getFunctionStatsByName(functionID);
  }

  public static FunctionStats getFunctionStats(String functionID) {
    InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds == null) {
      return createDummyFunctionStats();
    }
    return ds.getFunctionStatsManager().getFunctionStatsByName(functionID);
  }

  @VisibleForTesting
  FunctionStats getDummyFunctionStats() {
    return dummyFunctionStats;
  }

  @VisibleForTesting
  Statistics getDummyStatistics() {
    return dummyStatistics;
  }

  @VisibleForTesting
  MeterRegistry getNoopMeterRegistry() {
    return noopMeterRegistry;
  }

  private static FunctionStats createDummyFunctionStats() {
    return createDummyFunctionStats(createNoopMeterRegistry(), createDummyStatistics());
  }

  private static FunctionStats createDummyFunctionStats(MeterRegistry meterRegistry,
      Statistics statistics) {
    return new FunctionStatsImpl("", meterRegistry, statistics, FunctionServiceStats.createDummy());
  }

  private static DummyStatisticsImpl createDummyStatistics() {
    return new DummyStatisticsImpl(FunctionStatsImpl.getStatisticsType(), null, 0);
  }

  private static NoopMeterRegistry createNoopMeterRegistry() {
    return new NoopMeterRegistry();
  }

  public interface Factory {
    FunctionStatsManager create(boolean statsDisabled, StatisticsFactory statisticsFactory,
        Supplier<MeterRegistry> meterRegistrySupplier);
  }
}
