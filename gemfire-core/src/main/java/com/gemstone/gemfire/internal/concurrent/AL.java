/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.concurrent;

/**
 * These methods are the same ones on
 * the JDK 5 version java.util.concurrent.atomic.AtomicLong.
 * Note that unlike AtomicLong this interface does not support
 * <code>java.lang.Number</code>.
 * @author darrel
 * @deprecated used AtomicLong instead
 */
public interface AL {
  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public long get();

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(long newValue);

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public long getAndSet(long newValue);

  /**
   * Atomically sets the value to the given updated value
   * if the current value {@code ==} the expected value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. False return indicates that
   * the actual value was not equal to the expected value.
   */
  public boolean compareAndSet(long expect, long update);

  /**
   * Atomically sets the value to the given updated value
   * if the current value {@code ==} the expected value.
   *
   * <p>May <a href="package-summary.html#Spurious">fail spuriously</a>
   * and does not provide ordering guarantees, so is only rarely an
   * appropriate alternative to {@code compareAndSet}.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful.
   */
  public boolean weakCompareAndSet(long expect, long update);

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  public long getAndIncrement();

  /**
   * Atomically decrements by one the current value.
   *
   * @return the previous value
   */
  public long getAndDecrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  public long getAndAdd(long delta);

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  public long incrementAndGet();

  /**
   * Atomically decrements by one the current value.
   *
   * @return the updated value
   */
  public long decrementAndGet();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  public long addAndGet(long delta);
  
  /**
   * Atomically sets the value to the given updated value
   * if the given value {@code >} the current value.
   * This could be sub-optimat when the update being done by multiple thread is not in 
   * in an incremental fashion.
   * @param update
   */
  public boolean setIfGreater(long update);
}
