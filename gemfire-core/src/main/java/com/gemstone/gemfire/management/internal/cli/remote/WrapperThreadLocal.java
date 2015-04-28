/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.remote;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @param <T>
 *          Type of ThreadLocal variable
 * 
 * @since 7.0
 */
public abstract class WrapperThreadLocal<T> extends ThreadLocal<T> {
  
  public T getAndCreateIfAbsent() {
    if (!isSet()) {
      set(createWrapped());
    }
    return get();
  }
  
  protected abstract T createWrapped();

  public boolean isSet() {
    return get() != null;
  }
}
