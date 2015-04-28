/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks the usage of a reference.
 * 
 * @author bakera
 *
 * @param <T> the reference type
 */
public class TrackedReference<T> {
  /** the referent */
  private final T ref;
  
  /** the number of uses */
  private final AtomicInteger uses;

  /**
   * Decrements the use count of each reference.
   * @param refs the references to decrement
   */
  public static <T> void decrementAll(Iterable<TrackedReference<T>> refs) {
    for (TrackedReference<?> tr : refs) {
      tr.decrement();
    }
  }
  
  public TrackedReference(T ref) {
    this.ref = ref;
    uses = new AtomicInteger(0);
  }
  
  /**
   * Returns the referent.
   * @return the referent
   */
  public T get() {
    return ref;
  }
  
  /**
   * Returns the current count.
   * @return the current uses
   */
  public int uses() {
    return uses.get();
  }
  
  /**
   * Returns true if the reference is in use.
   * @return true if used
   */
  public boolean inUse() {
    return uses() > 0;
  }
  
  /**
   * Increments the use count and returns the reference.
   * @return the reference
   */
  public T getAndIncrement() {
    increment();
    return ref;
  }
  
  /**
   * Increments the use counter and returns the current count.
   * @return the current uses
   */
  public int increment() {
    int val = uses.incrementAndGet();
    assert val >= 1;
    
    return val;
  }
  
  /**
   * Decrements the use counter and returns the current count.
   * @return the current uses
   */
  public int decrement() {
    int val = uses.decrementAndGet();
    assert val >= 0;

    return val;
  }
  
  @Override
  public String toString() {
    return uses() + ": " + ref.toString();
  }
}
