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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.process.ProcessUtils;

/**
 * A {@link StatisticsManager} that delegates creation to specified factories and notifies listeners
 * when statistics are created or destroyed.
 */
public class StatisticsRegistry implements StatisticsManager {

  private final StatisticsTypeFactory typeFactory;
  private final AtomicStatisticsFactory atomicStatisticsFactory;
  private final OsStatisticsFactory osStatisticsFactory;
  private final IntSupplier pidSupplier;
  private final String name;
  private final long startTime;
  private final List<Statistics> instances = new CopyOnWriteArrayList<>();
  private final AtomicLong nextUniqueId = new AtomicLong(1);

  private int modificationCount;

  /**
   * Creates an instance of OS Statistics for this registry.
   */
  public interface OsStatisticsFactory {

    Statistics create(StatisticsType type, String textId, long numericId, long uniqueId,
        int osStatFlags, StatisticsManager manager);
  }

  /**
   * Creates an instance of Atomic Statistics for this registry.
   */
  public interface AtomicStatisticsFactory {
    Statistics create(StatisticsType type, String textId, long numericId, long uniqueId,
        StatisticsManager manager);
  }

  /**
   * Creates a {@code StatisticsRegistry} that uses the default factories to create {@link
   * StatisticsType}s and {@link Statistics} instances.
   *
   * The default factories are:
   * <ol>
   * <li>For {@code StatisticsType}s: the {@link StatisticsTypeFactoryImpl} singleton.</li>
   * <li>For atomic {@code Statistics}s: {@link StatisticsImpl#createAtomicNoOS}.</li>
   * <li>For non-atomic {@code Statistics}s: the {@link LocalStatisticsImpl} constructor.</li>
   * </ol>
   *
   * @param name the name of the registry
   * @param startTime the time at which the registry was started
   */
  public StatisticsRegistry(String name, long startTime) {
    this(name, startTime, StatisticsTypeFactoryImpl.singleton(),
        LocalStatisticsImpl::createNonAtomic, StatisticsImpl::createAtomicNoOS,
        ProcessUtils::identifyPidAsUnchecked);
  }

  /**
   * Creates a {@code StatisticsRegistry} that uses the given factories to create {@code
   * StatisticsType}s and {@code Statistics} instances.
   *
   * @param name the name of the registry
   * @param startTime the time at which the registry was started
   * @param typeFactory the factory to create {@code StatisticsType}s
   * @param osStatisticsFactory the factory to create OS statistics
   * @param atomicStatisticsFactory the factory to create atomic statistics
   * @param pidSupplier the IntSupplier to return this pid
   */
  StatisticsRegistry(String name, long startTime, StatisticsTypeFactory typeFactory,
      OsStatisticsFactory osStatisticsFactory, AtomicStatisticsFactory atomicStatisticsFactory,
      IntSupplier pidSupplier) {
    this.name = name;
    this.startTime = startTime;
    this.typeFactory = typeFactory;
    this.osStatisticsFactory = osStatisticsFactory;
    this.atomicStatisticsFactory = atomicStatisticsFactory;
    this.pidSupplier = pidSupplier;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getPid() {
    return pidSupplier.getAsInt();
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public List<Statistics> getStatsList() {
    return instances;
  }

  @Override
  public int getStatListModCount() {
    return modificationCount;
  }

  @Override
  public Statistics[] getStatistics() {
    return getStatsList().toArray(new Statistics[0]);
  }

  @Override
  public int getStatisticsCount() {
    return getStatsList().size();
  }

  @Override
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return typeFactory.createType(name, description, stats);
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return typeFactory.createTypesFromXml(reader);
  }

  @Override
  public StatisticsType findType(String name) {
    return typeFactory.findType(name);
  }

  @Override
  public Statistics createStatistics(StatisticsType type) {
    return createOsStatistics(type, null, 0, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type) {
    return createAtomicStatistics(type, null, 0);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId) {
    return createOsStatistics(type, textId, 0, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return createAtomicStatistics(type, textId, 0);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    return createOsStatistics(type, textId, numericId, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    long uniqueId = nextUniqueId.getAndIncrement();
    return newAtomicStatistics(type, uniqueId, numericId, textId);
  }

  @Override
  public Statistics createOsStatistics(StatisticsType type, String textId, long numericId,
      int osStatFlags) {
    long uniqueId = nextUniqueId.getAndIncrement();
    return newOsStatistics(type, uniqueId, numericId, textId, osStatFlags);
  }

  @Override
  public void destroyStatistics(Statistics statisticsToDestroy) {
    deregisterDestroyedStatistics(statisticsToDestroy);
  }

  @Override
  public boolean statisticsExists(long uniqueId) {
    return anyStatisticsInstance(withUniqueId(uniqueId))
        .isPresent();
  }

  @Override
  public Statistics findStatisticsByUniqueId(long uniqueId) {
    return anyStatisticsInstance(withUniqueId(uniqueId))
        .orElse(null);
  }

  @Override
  public Statistics[] findStatisticsByNumericId(long numericId) {
    return allStatisticsInstances(withNumericId(numericId))
        .toArray(Statistics[]::new);
  }

  @Override
  public Statistics[] findStatisticsByTextId(String textId) {
    return allStatisticsInstances(withTextId(textId))
        .toArray(Statistics[]::new);
  }

  @Override
  public Statistics[] findStatisticsByType(StatisticsType type) {
    return allStatisticsInstances(withStatisticsType(type))
        .toArray(Statistics[]::new);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return typeFactory.createIntCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return typeFactory.createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return typeFactory.createDoubleCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return typeFactory.createIntGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return typeFactory.createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return typeFactory.createDoubleGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units,
      boolean largerBetter) {
    return typeFactory.createIntCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units,
      boolean largerBetter) {
    return typeFactory.createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
      boolean largerBetter) {
    return typeFactory.createDoubleCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units,
      boolean largerBetter) {
    return typeFactory.createIntGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units,
      boolean largerBetter) {
    return typeFactory.createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
      boolean largerBetter) {
    return typeFactory.createDoubleGauge(name, description, units, largerBetter);
  }

  protected Statistics newAtomicStatistics(StatisticsType type, long uniqueId, long numericId,
      String textId) {
    Statistics statistics =
        atomicStatisticsFactory.create(type, textId, numericId, uniqueId, this);
    registerNewStatistics(statistics);
    return statistics;
  }

  protected Statistics newOsStatistics(StatisticsType type, long uniqueId, long numericId,
      String textId, int osStatFlags) {
    Statistics statistics = osStatisticsFactory.create(type, textId, numericId, uniqueId,
        osStatFlags, this);
    registerNewStatistics(statistics);
    return statistics;
  }

  private Stream<Statistics> allStatisticsInstances(Predicate<? super Statistics> predicate) {
    return getStatsList().stream().filter(predicate);
  }

  private Optional<Statistics> anyStatisticsInstance(Predicate<? super Statistics> predicate) {
    return allStatisticsInstances(predicate).findAny();
  }

  private static Predicate<Statistics> withNumericId(long numericId) {
    return statistics -> statistics.getNumericId() == numericId;
  }

  private static Predicate<Statistics> withStatisticsType(StatisticsType type) {
    return statistics -> statistics.getType() == type;
  }

  private static Predicate<Statistics> withTextId(String textId) {
    return statistics -> textId.equals(statistics.getTextId());
  }

  private static Predicate<Statistics> withUniqueId(long uniqueId) {
    return statistics -> statistics.getUniqueId() == uniqueId;
  }

  private void registerNewStatistics(Statistics newStatistics) {
    synchronized (instances) {
      instances.add(newStatistics);
      modificationCount++;
    }
  }

  private void deregisterDestroyedStatistics(Statistics destroyedStatistics) {
    synchronized (instances) {
      if (instances.remove(destroyedStatistics)) {
        modificationCount++;
      }
    }
  }
}
