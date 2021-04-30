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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * An object that maintains the values of various application-defined statistics. The statistics
 * themselves are described by an instance of {@link StatisticsType}.
 *
 * <p>
 * For optimal statistic access, each statistic may be referred to by its {@link #nameToId id} in
 * the statistics object.
 *
 * @since GemFire 3.0
 */
public abstract class StatisticsImpl implements SuppliableStatistics {

  private static final Logger logger = LogService.getLogger();

  /** The type of this statistics instance */
  protected final ValidatingStatisticsType type;

  /** The display name of this statistics instance */
  private final String textId;

  /** Numeric information display with these statistics */
  private final long numericId;

  /** Non-zero if stats values come from operating system system calls */
  private final int osStatFlags;

  /** Uniquely identifies this instance */
  private final long uniqueId;

  /** The StatisticsFactory that created this instance */
  private final StatisticsManager statisticsManager;

  /**
   * Suppliers of int sample values to be sampled every sample-interval
   */
  private final CopyOnWriteHashMap<Integer, IntSupplier> intSuppliers = new CopyOnWriteHashMap<>();
  /**
   * Suppliers of long sample values to be sampled every sample-interval
   */
  private final CopyOnWriteHashMap<Integer, LongSupplier> longSuppliers =
      new CopyOnWriteHashMap<>();
  /**
   * Suppliers of double sample values to be sampled every sample-interval
   */
  private final CopyOnWriteHashMap<Integer, DoubleSupplier> doubleSuppliers =
      new CopyOnWriteHashMap<>();

  /**
   * Suppliers that have previously failed. Tracked to avoid logging many messages about a failing
   * supplier
   */
  private final Set<Object> flakySuppliers = new HashSet<>();

  private final StatisticsLogger statisticsLogger;

  /** Are these statistics closed? */
  private volatile boolean closed;

  /**
   * factory method to create a class that implements Statistics
   */
  static Statistics createAtomicNoOS(StatisticsType type, String textId, long numericId,
      long uniqueId, StatisticsManager statisticsManager) {
    return new StripedStatisticsImpl(type, textId, numericId, uniqueId, statisticsManager);
  }

  /**
   * Creates a new statistics instance of the given type and unique id
   *
   * @param type A description of the statistics
   * @param textId Text that helps identifies this instance
   * @param numericId A number that helps identify this instance
   * @param uniqueId A number that uniquely identifies this instance
   * @param osStatFlags Non-zero if stats require system calls to collect them; for internal use
   *        only
   * @param statisticsManager The StatisticsManager responsible for creating this instance
   */
  StatisticsImpl(StatisticsType type, String textId, long numericId, long uniqueId,
      int osStatFlags, StatisticsManager statisticsManager) {
    this(type, textId, numericId, uniqueId, osStatFlags, statisticsManager,
        logger::warn);
  }

  /**
   * Creates a new statistics instance of the given type and unique id
   *
   * @param type A description of the statistics
   * @param textId Text that helps identifies this instance
   * @param numericId A number that helps identify this instance
   * @param uniqueId A number that uniquely identifies this instance
   * @param osStatFlags Non-zero if stats require system calls to collect them; for internal use
   *        only
   * @param statisticsManager The StatisticsManager responsible for creating this instance
   * @param statisticsLogger The StatisticsLogger to log warning about flaky suppliers
   */
  StatisticsImpl(StatisticsType type, String textId, long numericId, long uniqueId,
      int osStatFlags, StatisticsManager statisticsManager, StatisticsLogger statisticsLogger) {
    this.type = (ValidatingStatisticsType) type;
    this.textId = StringUtils.isEmpty(textId) ? statisticsManager.getName() : textId;
    this.numericId = numericId == 0 ? statisticsManager.getPid() : numericId;
    this.uniqueId = uniqueId;
    this.osStatFlags = osStatFlags;
    this.statisticsManager = statisticsManager;
    this.statisticsLogger = statisticsLogger;
    closed = false;
  }

  @Override
  public int nameToId(String name) {
    return type.nameToId(name);
  }

  @Override
  public StatisticDescriptor nameToDescriptor(String name) {
    return type.nameToDescriptor(name);
  }

  @Override
  public void close() {
    if (statisticsManager != null) {
      statisticsManager.destroyStatistics(this);
    }
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
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

  /**
   * Gets the unique id for this resource
   */
  @Override
  public long getUniqueId() {
    return uniqueId;
  }

  @Override
  public void setInt(String name, int value) {
    setLong(name, value);
  }

  @Override
  public void setInt(StatisticDescriptor descriptor, int value) {
    setLong(descriptor, value);
  }

  @Override
  public void setInt(int id, int value) {
    setLong(id, value);
  }

  @Override
  public void setLong(String name, long value) {
    setLong(nameToDescriptor(name), value);
  }

  @Override
  public void setLong(StatisticDescriptor descriptor, long value) {
    setLong(getLongId(descriptor), value);
  }

  @Override
  public void setLong(int id, long value) {
    if (isOpen()) {
      _setLong(id, value);
    }
  }

  @Override
  public void setDouble(String name, double value) {
    setDouble(nameToDescriptor(name), value);
  }

  @Override
  public void setDouble(StatisticDescriptor descriptor, double value) {
    setDouble(getDoubleId(descriptor), value);
  }

  @Override
  public void setDouble(int id, double value) {
    if (isOpen()) {
      _setDouble(id, value);
    }
  }

  @Override
  public int getInt(String name) {
    return (int) getLong(name);
  }

  @Override
  public int getInt(StatisticDescriptor descriptor) {
    return (int) getLong(descriptor);
  }

  @Override
  public int getInt(int id) {
    return (int) getLong(id);
  }

  @Override
  public long getLong(String name) {
    return getLong(nameToDescriptor(name));
  }

  @Override
  public long getLong(StatisticDescriptor descriptor) {
    return getLong(getLongId(descriptor));
  }

  @Override
  public long getLong(int id) {
    if (isOpen()) {
      if (!type.isValidLongId(id)) {
        throw new IllegalArgumentException("Id, " + id + ", is not a long statistic.");
      }
      return _getLong(id);
    } else {
      return 0;
    }
  }

  @Override
  public double getDouble(String name) {
    return getDouble(nameToDescriptor(name));
  }

  @Override
  public double getDouble(StatisticDescriptor descriptor) {
    return getDouble(getDoubleId(descriptor));
  }

  @Override
  public double getDouble(int id) {
    if (isOpen()) {
      return _getDouble(id);
    } else {
      return 0.0;
    }
  }

  @Override
  public Number get(StatisticDescriptor descriptor) {
    if (isOpen()) {
      return _get((StatisticDescriptorImpl) descriptor);
    } else {
      return 0;
    }
  }

  @Override
  public Number get(String name) {
    return get(nameToDescriptor(name));
  }

  @Override
  public long getRawBits(StatisticDescriptor descriptor) {
    if (isOpen()) {
      return _getRawBits((StatisticDescriptorImpl) descriptor);
    } else {
      return 0;
    }
  }

  @Override
  public long getRawBits(String name) {
    return getRawBits(nameToDescriptor(name));
  }

  @Override
  public void incInt(String name, int delta) {
    incLong(name, delta);
  }

  @Override
  public void incInt(StatisticDescriptor descriptor, int delta) {
    incLong(descriptor, delta);
  }

  @Override
  public void incInt(int id, int delta) {
    incLong(id, delta);
  }

  @Override
  public void incLong(String name, long delta) {
    incLong(nameToDescriptor(name), delta);
  }

  @Override
  public void incLong(StatisticDescriptor descriptor, long delta) {
    incLong(getLongId(descriptor), delta);
  }

  @Override
  public void incLong(int id, long delta) {
    if (isOpen()) {
      _incLong(id, delta);
    }
  }

  @Override
  public void incDouble(String name, double delta) {
    incDouble(nameToDescriptor(name), delta);
  }

  @Override
  public void incDouble(StatisticDescriptor descriptor, double delta) {
    incDouble(getDoubleId(descriptor), delta);
  }

  @Override
  public void incDouble(int id, double delta) {
    if (isOpen()) {
      _incDouble(id, delta);
    }
  }

  @Override
  public IntSupplier setIntSupplier(final int id, final IntSupplier supplier) {
    // setIntSupplier is deprecated but it is too much of a pain to wrap the IntSupplier
    // in a LongSupplier. So the implementation continues to store IntSupplier instances
    // but all the checks and actions are long based instead of int based.
    if (!type.isValidLongId(id)) {
      throw new IllegalArgumentException("Id " + id + " is not in range for stat" + type);
    }
    return intSuppliers.put(id, supplier);
  }

  @Override
  public IntSupplier setIntSupplier(final String name, final IntSupplier supplier) {
    return setIntSupplier(nameToId(name), supplier);
  }

  @Override
  public IntSupplier setIntSupplier(final StatisticDescriptor descriptor,
      final IntSupplier supplier) {
    return setIntSupplier(getLongId(descriptor), supplier);
  }

  @Override
  public LongSupplier setLongSupplier(final int id, final LongSupplier supplier) {
    if (!type.isValidLongId(id)) {
      throw new IllegalArgumentException("Id " + id + " is not in range for stat" + type);
    }
    return longSuppliers.put(id, supplier);
  }

  @Override
  public LongSupplier setLongSupplier(final String name, final LongSupplier supplier) {
    return setLongSupplier(nameToId(name), supplier);
  }

  @Override
  public LongSupplier setLongSupplier(final StatisticDescriptor descriptor,
      final LongSupplier supplier) {
    return setLongSupplier(getLongId(descriptor), supplier);
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final int id, final DoubleSupplier supplier) {
    if (!type.isValidDoubleId(id)) {
      throw new IllegalArgumentException("Id " + id + " is not in range for stat" + type);
    }
    return doubleSuppliers.put(id, supplier);
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final String name, final DoubleSupplier supplier) {
    return setDoubleSupplier(nameToId(name), supplier);
  }

  @Override
  public DoubleSupplier setDoubleSupplier(final StatisticDescriptor descriptor,
      final DoubleSupplier supplier) {
    return setDoubleSupplier(getDoubleId(descriptor), supplier);
  }

  @Override
  public int hashCode() {
    return (int) uniqueId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof StatisticsImpl)) {
      return false;
    }
    StatisticsImpl other = (StatisticsImpl) obj;
    return uniqueId == other.getUniqueId();
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + System.identityHashCode(this) + "{"
        + "uniqueId=" + uniqueId
        + ", numericId=" + numericId
        + ", textId=" + textId
        + ", type=" + type.getName()
        + ", closed=" + closed
        + "}";
  }

  @Override
  public abstract boolean isAtomic();

  /**
   * Sets the value of a statistic of type {@code long} at the given id, but performs no
   * type checking.
   */
  protected abstract void _setLong(int id, long value);

  /**
   * Sets the value of a statistic of type {@code double} at the given id, but performs no
   * type checking.
   */
  protected abstract void _setDouble(int id, double value);

  /**
   * Returns the value of the statistic of type {@code long} at the given id, but performs
   * no type checking.
   */
  protected abstract long _getLong(int id);

  /**
   * Returns the value of the statistic of type {@code double} at the given id, but
   * performs no type checking.
   */
  protected abstract double _getDouble(int id);

  /**
   * Increments the value of the statistic of type {@code long} at the given id by a given
   * amount, but performs no type checking.
   */
  protected abstract void _incLong(int id, long delta);

  /**
   * Increments the value of the statistic of type {@code double} at the given id by a
   * given amount, but performs no type checking.
   */
  protected abstract void _incDouble(int id, double delta);

  /**
   * For internal use only. Tells the implementation to prepare the data in this instance for
   * sampling.
   *
   * @since GemFire 5.1
   */
  void prepareForSample() {
    // nothing needed in this impl.
  }

  @Override
  public int updateSuppliedValues() {
    int errors = 0;
    for (Map.Entry<Integer, IntSupplier> entry : intSuppliers.entrySet()) {
      try {
        _setLong(entry.getKey(), entry.getValue().getAsInt());
      } catch (Throwable t) {
        logSupplierError(t, entry.getKey(), entry.getValue());
        errors++;
      }
    }
    for (Map.Entry<Integer, LongSupplier> entry : longSuppliers.entrySet()) {
      try {
        _setLong(entry.getKey(), entry.getValue().getAsLong());
      } catch (Throwable t) {
        logSupplierError(t, entry.getKey(), entry.getValue());
        errors++;
      }
    }
    for (Map.Entry<Integer, DoubleSupplier> entry : doubleSuppliers.entrySet()) {
      try {
        _setDouble(entry.getKey(), entry.getValue().getAsDouble());
      } catch (Throwable t) {
        logSupplierError(t, entry.getKey(), entry.getValue());
        errors++;
      }
    }

    return errors;
  }

  /**
   * @return the number of statistics that are measured using supplier callbacks
   */
  int getSupplierCount() {
    return intSuppliers.size() + doubleSuppliers.size() + longSuppliers.size();
  }

  boolean usesSystemCalls() {
    return osStatFlags != 0;
  }

  int getOsStatFlags() {
    return osStatFlags;
  }

  private void logSupplierError(final Throwable throwable, int statId, Object supplier) {
    if (flakySuppliers.add(supplier)) {
      statisticsLogger.logWarning("Error invoking supplier for stat {}, id {}", getTextId(), statId,
          throwable);
    }
  }

  private boolean isOpen() {
    return !closed;
  }

  /**
   * Returns the value of the specified statistic descriptor.
   */
  private Number _get(StatisticDescriptorImpl descriptor) {
    switch (descriptor.getTypeCode()) {
      case StatisticDescriptorImpl.LONG:
        return _getLong(descriptor.getId());
      case StatisticDescriptorImpl.DOUBLE:
        return _getDouble(descriptor.getId());
      default:
        throw new RuntimeException(
            String.format("unexpected stat descriptor type code: %s",
                descriptor.getTypeCode()));
    }
  }

  /**
   * Returns the bits that represent the raw value of the specified statistic descriptor.
   */
  private long _getRawBits(StatisticDescriptorImpl descriptor) {
    switch (descriptor.getTypeCode()) {
      case StatisticDescriptorImpl.LONG:
        return _getLong(descriptor.getId());
      case StatisticDescriptorImpl.DOUBLE:
        return Double.doubleToRawLongBits(_getDouble(descriptor.getId()));
      default:
        throw new RuntimeException(
            String.format("unexpected stat descriptor type code: %s",
                descriptor.getTypeCode()));
    }
  }

  private static int getLongId(StatisticDescriptor descriptor) {
    return ((StatisticDescriptorImpl) descriptor).checkLong();
  }

  private static int getDoubleId(StatisticDescriptor descriptor) {
    return ((StatisticDescriptorImpl) descriptor).checkDouble();
  }

  interface StatisticsLogger {
    void logWarning(String message, String textId, int statId, Throwable throwable);
  }
}
