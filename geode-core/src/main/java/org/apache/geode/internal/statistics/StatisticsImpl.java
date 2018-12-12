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

import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.concurrent.Atomics;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;

// @todo darrel Add statistics instances to archive when they are created.
/**
 * An object that maintains the values of various application-defined statistics. The statistics
 * themselves are described by an instance of {@link StatisticsType}.
 *
 * <P>
 *
 * For optimal statistic access, each statistic may be referred to by its {@link #nameToId id} in
 * the statistics object.
 *
 * <P>
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 */
public abstract class StatisticsImpl implements Statistics {
  /** logger - not private for tests */
  static Logger logger = LogService.getLogger();

  /** The type of this statistics instance */
  private final StatisticsTypeImpl type;

  /** The display name of this statistics instance */
  private final String textId;

  /** Numeric information display with these statistics */
  private final long numericId;

  /** Non-zero if stats values come from operating system system calls */
  private final int osStatFlags;

  /** Are these statistics closed? */
  private boolean closed;

  /** Uniquely identifies this instance */
  private long uniqueId;

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
  private final Set<Object> flakySuppliers = new HashSet<Object>();

  /////////////////////// Constructors ///////////////////////

  /**
   * factory method to create a class that implements Statistics
   */
  public static Statistics createAtomicNoOS(StatisticsType type, String textId, long numericId,
      long uniqueId, StatisticsManager mgr) {
    return Atomics.createAtomicStatistics(type, textId, numericId, uniqueId, mgr);
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
   */
  public StatisticsImpl(StatisticsType type, String textId, long numericId, long uniqueId,
      int osStatFlags) {
    this.type = (StatisticsTypeImpl) type;
    this.textId = textId;
    this.numericId = numericId;
    this.uniqueId = uniqueId;
    this.osStatFlags = osStatFlags;
    closed = false;
  }

  ////////////////////// Instance Methods //////////////////////

  public boolean usesSystemCalls() {
    return this.osStatFlags != 0;
  }

  public int getOsStatFlags() {
    return this.osStatFlags;
  }

  public int nameToId(String name) {
    return this.type.nameToId(name);
  }

  public StatisticDescriptor nameToDescriptor(String name) {
    return this.type.nameToDescriptor(name);
  }

  public void close() {
    this.closed = true;
  }

  public boolean isClosed() {
    return this.closed;
  }

  public abstract boolean isAtomic();

  private boolean isOpen() { // fix for bug 29973
    return !this.closed;
  }

  //////////////////////// attribute Methods ///////////////////////

  public StatisticsType getType() {
    return this.type;
  }

  public String getTextId() {
    return this.textId;
  }

  public long getNumericId() {
    return this.numericId;
  }

  /**
   * Gets the unique id for this resource
   */
  public long getUniqueId() {
    return this.uniqueId;
  }

  /**
   * Sets a unique id for this resource.
   */
  public void setUniqueId(long uid) {
    this.uniqueId = uid;
  }

  //////////////////////// set() Methods ///////////////////////

  public void setInt(String name, int value) {
    setInt(nameToDescriptor(name), value);
  }

  public void setInt(StatisticDescriptor descriptor, int value) {
    setInt(getIntId(descriptor), value);
  }

  public void setInt(int id, int value) {
    if (isOpen()) {
      _setInt(id, value);
    }
  }

  /**
   * Sets the value of a statistic of type <code>int</code> at the given offset, but performs no
   * type checking.
   */
  protected abstract void _setInt(int offset, int value);

  public void setLong(String name, long value) {
    setLong(nameToDescriptor(name), value);
  }

  public void setLong(StatisticDescriptor descriptor, long value) {
    setLong(getLongId(descriptor), value);
  }

  public void setLong(int id, long value) {
    if (isOpen()) {
      _setLong(id, value);
    }
  }

  /**
   * Sets the value of a statistic of type <code>long</code> at the given offset, but performs no
   * type checking.
   */
  protected abstract void _setLong(int offset, long value);

  public void setDouble(String name, double value) {
    setDouble(nameToDescriptor(name), value);
  }

  public void setDouble(StatisticDescriptor descriptor, double value) {
    setDouble(getDoubleId(descriptor), value);
  }

  public void setDouble(int id, double value) {
    if (isOpen()) {
      _setDouble(id, value);
    }
  }

  /**
   * Sets the value of a statistic of type <code>double</code> at the given offset, but performs no
   * type checking.
   */
  protected abstract void _setDouble(int offset, double value);

  /////////////////////// get() Methods ///////////////////////

  public int getInt(String name) {
    return getInt(nameToDescriptor(name));
  }

  public int getInt(StatisticDescriptor descriptor) {
    return getInt(getIntId(descriptor));
  }

  public int getInt(int id) {
    if (isOpen()) {
      return _getInt(id);
    } else {
      return 0;
    }
  }

  /**
   * Returns the value of the statistic of type <code>int</code> at the given offset, but performs
   * no type checking.
   */
  protected abstract int _getInt(int offset);


  public long getLong(String name) {
    return getLong(nameToDescriptor(name));
  }

  public long getLong(StatisticDescriptor descriptor) {
    return getLong(getLongId(descriptor));
  }

  public long getLong(int id) {
    if (isOpen()) {
      return _getLong(id);
    } else {
      return 0;
    }
  }


  /**
   * Returns the value of the statistic of type <code>long</code> at the given offset, but performs
   * no type checking.
   */
  protected abstract long _getLong(int offset);

  public double getDouble(String name) {
    return getDouble(nameToDescriptor(name));
  }

  public double getDouble(StatisticDescriptor descriptor) {
    return getDouble(getDoubleId(descriptor));
  }

  public double getDouble(int id) {
    if (isOpen()) {
      return _getDouble(id);
    } else {
      return 0.0;
    }
  }

  /**
   * Returns the value of the statistic of type <code>double</code> at the given offset, but
   * performs no type checking.
   */
  protected abstract double _getDouble(int offset);

  public Number get(StatisticDescriptor descriptor) {
    if (isOpen()) {
      return _get((StatisticDescriptorImpl) descriptor);
    } else {
      return Integer.valueOf(0);
    }
  }

  public Number get(String name) {
    return get(nameToDescriptor(name));
  }

  public long getRawBits(StatisticDescriptor descriptor) {
    if (isOpen()) {
      return _getRawBits((StatisticDescriptorImpl) descriptor);
    } else {
      return 0;
    }
  }

  public long getRawBits(String name) {
    return getRawBits(nameToDescriptor(name));
  }

  //////////////////////// inc() Methods ////////////////////////

  public void incInt(String name, int delta) {
    incInt(nameToDescriptor(name), delta);
  }

  public void incInt(StatisticDescriptor descriptor, int delta) {
    incInt(getIntId(descriptor), delta);
  }

  public void incInt(int id, int delta) {
    if (isOpen()) {
      _incInt(id, delta);
    }
  }

  /**
   * Increments the value of the statistic of type <code>int</code> at the given offset by a given
   * amount, but performs no type checking.
   */
  protected abstract void _incInt(int offset, int delta);

  public void incLong(String name, long delta) {
    incLong(nameToDescriptor(name), delta);
  }

  public void incLong(StatisticDescriptor descriptor, long delta) {
    incLong(getLongId(descriptor), delta);
  }

  public void incLong(int id, long delta) {
    if (isOpen()) {
      _incLong(id, delta);
    }
  }

  /**
   * Increments the value of the statistic of type <code>long</code> at the given offset by a given
   * amount, but performs no type checking.
   */
  protected abstract void _incLong(int offset, long delta);

  public void incDouble(String name, double delta) {
    incDouble(nameToDescriptor(name), delta);
  }

  public void incDouble(StatisticDescriptor descriptor, double delta) {
    incDouble(getDoubleId(descriptor), delta);
  }

  public void incDouble(int id, double delta) {
    if (isOpen()) {
      _incDouble(id, delta);
    }
  }

  /**
   * Increments the value of the statistic of type <code>double</code> at the given offset by a
   * given amount, but performs no type checking.
   */
  protected abstract void _incDouble(int offset, double delta);

  /**
   * For internal use only. Tells the implementation to prepare the data in this instance for
   * sampling.
   *
   * @since GemFire 5.1
   */
  public void prepareForSample() {
    // nothing needed in this impl.
  }

  /**
   * Invoke sample suppliers to retrieve the current value for the suppler controlled sets and
   * update the stats to reflect the supplied values.
   *
   * @return the number of callback errors that occurred while sampling stats
   */
  public int invokeSuppliers() {
    int errors = 0;
    for (Map.Entry<Integer, IntSupplier> entry : intSuppliers.entrySet()) {
      try {
        _setInt(entry.getKey(), entry.getValue().getAsInt());
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

  private void logSupplierError(final Throwable t, int statId, Object supplier) {
    if (flakySuppliers.add(supplier)) {
      logger.warn("Error invoking supplier for stat {}, id {}", this.getTextId(), statId, t);
    }
  }

  /**
   * @return the number of statistics that are measured using supplier callbacks
   */
  public int getSupplierCount() {
    return intSuppliers.size() + doubleSuppliers.size() + longSuppliers.size();
  }

  @Override
  public IntSupplier setIntSupplier(final int id, final IntSupplier supplier) {
    if (id >= type.getIntStatCount()) {
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
    return setIntSupplier(getIntId(descriptor), supplier);
  }

  @Override
  public LongSupplier setLongSupplier(final int id, final LongSupplier supplier) {
    if (id >= type.getLongStatCount()) {
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
    if (id >= type.getDoubleStatCount()) {
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
    return (int) this.uniqueId;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof StatisticsImpl)) {
      return false;
    }
    StatisticsImpl other = (StatisticsImpl) o;
    return this.uniqueId == other.getUniqueId();
  }

  private static int getIntId(StatisticDescriptor descriptor) {
    return ((StatisticDescriptorImpl) descriptor).checkInt();
  }

  private static int getLongId(StatisticDescriptor descriptor) {
    return ((StatisticDescriptorImpl) descriptor).checkLong();
  }

  private static int getDoubleId(StatisticDescriptor descriptor) {
    return ((StatisticDescriptorImpl) descriptor).checkDouble();
  }

  /**
   * Returns the value of the specified statistic descriptor.
   */
  private Number _get(StatisticDescriptorImpl stat) {
    switch (stat.getTypeCode()) {
      case StatisticDescriptorImpl.INT:
        return Integer.valueOf(_getInt(stat.getId()));
      case StatisticDescriptorImpl.LONG:
        return Long.valueOf(_getLong(stat.getId()));
      case StatisticDescriptorImpl.DOUBLE:
        return Double.valueOf(_getDouble(stat.getId()));
      default:
        throw new RuntimeException(
            String.format("unexpected stat descriptor type code: %s",
                Byte.valueOf(stat.getTypeCode())));
    }
  }

  /**
   * Returns the bits that represent the raw value of the specified statistic descriptor.
   */
  private long _getRawBits(StatisticDescriptorImpl stat) {
    switch (stat.getTypeCode()) {
      case StatisticDescriptorImpl.INT:
        return _getInt(stat.getId());
      case StatisticDescriptorImpl.LONG:
        return _getLong(stat.getId());
      case StatisticDescriptorImpl.DOUBLE:
        return Double.doubleToRawLongBits(_getDouble(stat.getId()));
      default:
        throw new RuntimeException(
            String.format("unexpected stat descriptor type code: %s",
                Byte.valueOf(stat.getTypeCode())));
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("uniqueId=").append(this.uniqueId);
    sb.append(", numericId=").append(this.numericId);
    sb.append(", textId=").append(this.textId);
    sb.append(", type=").append(this.type.getName());
    sb.append(", closed=").append(this.closed);
    sb.append("}");
    return sb.toString();
  }
}
