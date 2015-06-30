/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks the usage of a reference.
 * 
 * @author bakera
 *
 * @param <T> the reference type
 */
public final class TrackedReference<T> {
  /** the referent */
  private final T ref;
  
  /** the number of uses */
  private final AtomicInteger uses;
  
  /** list of users using this reference. Mainly for debugging */
  final ConcurrentHashMap<String, AtomicInteger> users;

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
    users = new ConcurrentHashMap<String, AtomicInteger>();
  }
  
  /**
   * Returns the referent.
   * @return the referent
   */
  public final T get() {
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
    return increment(null);
  }
  
  /**
   * Increments the use counter and returns the current count.
   * @return the current uses
   */
  public int increment(String user) {
    int val = uses.incrementAndGet();
    if (user != null) {
      AtomicInteger counter = users.get(user);
      if (counter == null) {
        counter = new AtomicInteger();
        users.putIfAbsent(user, counter);
        counter = users.get(user);
      }
      counter.incrementAndGet();
    }
    assert val >= 1;
    
    return val;
  }
  
  /**
   * Decrements the use counter and returns the current count.
   * @return the current uses
   */
  public int decrement() {
    return decrement(null);
  }
  
  /**
   * Decrements the use counter and returns the current count.
   * @return the current uses
   */
  public int decrement(String user) {
    int val = uses.decrementAndGet();
    assert val >= 0;
    if (user != null) {
      AtomicInteger counter = users.get(user);
      if (counter != null) {
        counter.decrementAndGet();
      }
    }
    
    return val;
  }
  
  @Override
  public String toString() {
    if (users != null) {
      StringBuffer sb = new StringBuffer();
      sb.append(ref.toString()).append(": ").append(uses());
      for (Entry<String, AtomicInteger> user : users.entrySet()) {
        sb.append(" ").append(user.getKey()).append(":").append(user.getValue().intValue());
      }
      return sb.toString();
    }
    return uses() + ": " + ref.toString();
  }
}
