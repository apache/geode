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
package com.gemstone.gemfire;

//import com.gemstone.gemfire.distributed.DistributedSystem;

import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Instances of this interface maintain the values of various application-defined
 * statistics.  The statistics themselves are described by an instance
 * of {@link StatisticsType}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 * <P>
 *
 * For improved performance, each statistic may be referred to by
 * its {@link #nameToDescriptor descriptor}.
 * <P>
 * For optimal performance, each statistic may be referred to by
 * its {@link #nameToId id} in the statistics object. Note that
 * ids can not be mapped back to their name and methods that take ids
 * are unsafe. It is important to call the correct type of method
 * for the given id. For example if your stat is a long then incLong
 * must be called instead of incInt.
 * <p>Note that as of the 5.1 release the <code>incInt</code>,
 * <code>incLong</code>, and <code>incDouble</code> methods no longer
 * return the new value of the statistic. They now return <code>void</code>.
 * This incompatible change was made
 * to allow for a more efficient concurrent increment implementation.
 * <P>
 *
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 *
 */
public interface Statistics {

  /**
   * Closes these statistics.  After statistics have been closed, they
   * are no longer archived.
   * A value access on a closed statistics always results in zero.
   * A value modification on a closed statistics is ignored.
   */
  public void close();

  ////////////////////////  accessor Methods  ///////////////////////

  /**
   * Returns the id of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   *
   * @see StatisticsType#nameToId
   */
  public int nameToId(String name);

  
  /**
   * Returns the descriptor of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   *
   * @see StatisticsType#nameToDescriptor
   */
  public StatisticDescriptor nameToDescriptor(String name);

  /**
   * Gets a value that uniquely identifies this statistics.
   */
  public long getUniqueId();

  /**
   * Gets the {@link StatisticsType} of this instance.
   */
  public StatisticsType getType();
  /**
   * Gets the text associated with this instance that helps identify it.
   */
  public String getTextId();
  /**
   * Gets the number associated with this instance that helps identify it.
   */
  public long getNumericId();
  /**
   * Returns true if modifications are atomic. This means that multiple threads,
   * can safely modify this instance without extra synchronization.
   * <p>
   * Returns false if modifications are not atomic. This means that modifications
   * to this instance are cheaper but not thread safe.
   */
  public boolean isAtomic();
  /**
   * Returns true if the instance has been {@link #close closed}.
   */
  public boolean isClosed();
  
  ////////////////////////  set() Methods  ///////////////////////

  /**
   * Sets the value of a statistic with the given <code>id</code>
   * whose type is <code>int</code>.
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   *
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public void setInt(int id, int value);

  /**
   * Sets the value of a named statistic of type <code>int</code>
   *
   * @throws IllegalArgumentException
   *         If no statistic exists named <code>name</code> or
   *         if the statistic with name <code>name</code> is not of
   *         type <code>int</code>.
   */
  public void setInt(String name, int value);

  /**
   * Sets the value of a described statistic of type <code>int</code>
   *
   * @throws IllegalArgumentException
   *         If no statistic exists for the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>int</code>.
   */
  public void setInt(StatisticDescriptor descriptor, int value);

  /**
   * Sets the value of a statistic with the given <code>id</code>
   * whose type is <code>long</code>.
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   *
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public void setLong(int id, long value);

  /**
   * Sets the value of a described statistic of type <code>long</code>
   *
   * @throws IllegalArgumentException
   *         If no statistic exists for the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>long</code>.
   */
  public void setLong(StatisticDescriptor descriptor, long value);

  /**
   * Sets the value of a named statistic of type <code>long</code>.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists named <code>name</code> or
   *         if the statistic with name <code>name</code> is not of
   *         type <code>long</code>.
   */
  public void setLong(String name, long value);

  /**
   * Sets the value of a statistic with the given <code>id</code>
   * whose type is <code>double</code>.
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   *
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public void setDouble(int id, double value);

  /**
   * Sets the value of a described statistic of type <code>double</code>
   *
   * @throws IllegalArgumentException
   *         If no statistic exists for the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>double</code>.
   */
  public void setDouble(StatisticDescriptor descriptor, double value);
  /**
   * Sets the value of a named statistic of type <code>double</code>.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists named <code>name</code> or
   *         if the statistic with name <code>name</code> is not of
   *         type <code>double</code>.
   */
  public void setDouble(String name, double value);

  ///////////////////////  get() Methods  ///////////////////////

  /**
   * Returns the value of the identified statistic of type <code>int</code>.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public int getInt(int id);

  /**
   * Returns the value of the described statistic of type <code>int</code>.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the specified <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>int</code>.
   */
  public int getInt(StatisticDescriptor descriptor);
  /**
   * Returns the value of the statistic of type <code>int</code> at
   * the given name.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>int</code>.
   */
  public int getInt(String name);

  /**
   * Returns the value of the identified statistic of type <code>long</code>.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public long getLong(int id);


  /**
   * Returns the value of the described statistic of type <code>long</code>.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the specified <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>long</code>.
   */
  public long getLong(StatisticDescriptor descriptor);
  /**
   * Returns the value of the statistic of type <code>long</code> at
   * the given name.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>long</code>.
   */
  public long getLong(String name);

  /**
   * Returns the value of the identified statistic of type <code>double</code>.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public double getDouble(int id);

  /**
   * Returns the value of the described statistic of type <code>double</code>.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the specified <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>double</code>.
   */
  public double getDouble(StatisticDescriptor descriptor);
  /**
   * Returns the value of the statistic of type <code>double</code> at
   * the given name.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>double</code>.
   */
  public double getDouble(String name);

  /**
   * Returns the value of the identified statistic.
   *
   * @param descriptor a statistic descriptor obtained with {@link #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If the described statistic does not exist
   */
  public Number get(StatisticDescriptor descriptor);

  /**
   * Returns the value of the named statistic.
   *
   * @throws IllegalArgumentException
   *         If the named statistic does not exist
   */
  public Number get(String name);

  /**
   * Returns the bits that represent the raw value of the described statistic.
   *
   * @param descriptor a statistic descriptor obtained with {@link #nameToDescriptor}
   * or {@link StatisticsType#nameToDescriptor}.
   * @throws IllegalArgumentException
   *         If the described statistic does not exist
   */
  public long getRawBits(StatisticDescriptor descriptor);

  /**
   * Returns the bits that represent the raw value of the named statistic.
   *
   * @throws IllegalArgumentException
   *         If the named statistic does not exist
   */
  public long getRawBits(String name);

  ////////////////////////  inc() Methods  ////////////////////////

  /**
   * Increments the value of the identified statistic of type <code>int</code>
   * by the given amount.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   *
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public void incInt(int id, int delta);

  /**
   * Increments the value of the described statistic of type <code>int</code>
   * by the given amount.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>int</code>.
   */
  public void incInt(StatisticDescriptor descriptor, int delta);

  /**
   * Increments the value of the statistic of type <code>int</code> with
   * the given name by a given amount.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>int</code>.
   */
  public void incInt(String name, int delta);

  /**
   * Increments the value of the identified statistic of type <code>long</code>
   * by the given amount.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   *
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public void incLong(int id, long delta);

  /**
   * Increments the value of the described statistic of type <code>long</code>
   * by the given amount.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>long</code>.
   */
  public void incLong(StatisticDescriptor descriptor, long delta);
  /**
   * Increments the value of the statistic of type <code>long</code> with
   * the given name by a given amount.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>long</code>.
   */
  public void incLong(String name, long delta);

  /**
   * Increments the value of the identified statistic of type <code>double</code>
   * by the given amount.
   *
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   *
   * @throws ArrayIndexOutOfBoundsException
   *         If the id is invalid.
   */
  public void incDouble(int id, double delta);

  /**
   * Increments the value of the described statistic of type <code>double</code>
   * by the given amount.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>double</code>.
   */
  public void incDouble(StatisticDescriptor descriptor, double delta);
  /**
   * Increments the value of the statistic of type <code>double</code> with
   * the given name by a given amount.
   *
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>double</code>.
   */
  public void incDouble(String name, double delta);

  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by long running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws ArrayIndexOutOfBoundsException If the id is invalid.
   * @throws IllegalArgumentException if the type of this
   * stat is not int
   */
  public IntSupplier setIntSupplier(int id, IntSupplier supplier);

  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by long running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param name the nane of the statistic to update
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>int</code>.
   * @throws IllegalArgumentException if the type of this
   * stat is not int
   */
  public IntSupplier setIntSupplier(String name, IntSupplier supplier);


  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by long running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param descriptor the descriptor of the statistic to update
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>int</code>.
   * @throws IllegalArgumentException if the type of this
   * stat is not int
   */
  public IntSupplier setIntSupplier(StatisticDescriptor descriptor, IntSupplier supplier);

  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by long running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws ArrayIndexOutOfBoundsException If the id is invalid.
   * @throws IllegalArgumentException if the type of this
   * stat is not long
   */
  public LongSupplier setLongSupplier(int id, LongSupplier supplier);

  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by long running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param name the nane of the statistic to update
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>long</code>.
   * @throws IllegalArgumentException if the type of this
   * stat is not long
   */
  public LongSupplier setLongSupplier(String name, LongSupplier supplier);


  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by long running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param descriptor the descriptor of the statistic to update
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>long</code>.
   * @throws IllegalArgumentException if the type of this
   * stat is not long
   */
  public LongSupplier setLongSupplier(StatisticDescriptor descriptor, LongSupplier supplier);

  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by double running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param id a statistic id obtained with {@link #nameToId}
   * or {@link StatisticsType#nameToId}.
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws ArrayIndexOutOfBoundsException If the id is invalid.
   * @throws IllegalArgumentException if the type of this
   * stat is not double
   */
  public DoubleSupplier setDoubleSupplier(int id, DoubleSupplier supplier);

  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by double running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param name the nane of the statistic to update
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws IllegalArgumentException
   *         If no statistic exists with name <code>name</code> or
   *         if the statistic named <code>name</code> is not of
   *         type <code>double</code>.
   * @throws IllegalArgumentException if the type of this
   * stat is not double
   */
  public DoubleSupplier setDoubleSupplier(String name, DoubleSupplier supplier);


  /**
   * Provide a callback to compute the value of this statistic
   * every sample interval and use that as the value of the stat.
   * <p>
   * The callback should return quickly because it is invoked on a shared thread.
   * It should not do any expensive computations, network calls, or access any resources
   * under locks that may be locked by double running processes.
   * <p>
   * This callback will only be invoked if the distributed system property
   * statistic-sampling-enabled is set to true, and it will be invoked at intervals
   * determined by the statistic-sampling-rate.
   * <p>
   * Get methods are not guaranteed to recompute a new value, they may return
   * the last sampled value
   * @param descriptor the descriptor of the statistic to update
   * @param supplier a callback that will return the value of the stat. This
   * replaces any previously registered supplier. If the passed in suppplier is null, it
   * will remove any existing supplier
   * @return the previously registered supplier, or null if there was no previously registered supplier
   * @throws IllegalArgumentException
   *         If no statistic exists with the given <code>descriptor</code> or
   *         if the described statistic is not of
   *         type <code>double</code>.
   * @throws IllegalArgumentException if the type of this
   * stat is not double
   */
  public DoubleSupplier setDoubleSupplier(StatisticDescriptor descriptor, DoubleSupplier supplier);
}
