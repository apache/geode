/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.concurrent;

/**
 * AL implementation for JDK 5.
 * @author darrel
 */
public final class AtomicLong5
  extends java.util.concurrent.atomic.AtomicLong
  implements AL {
  private static final long serialVersionUID = -1915700199064062938L;
  public AtomicLong5() {
    super();
  }
  public AtomicLong5(long initialValue) {
    super(initialValue);
  }
  /**
   * Use it only when threads are doing incremental updates. If updates are random 
   * then this method may not be optimal.
   */
  public boolean setIfGreater(long update) {
    while (true) {
      long cur = get();

      if (update > cur) {
        if (compareAndSet(cur, update))
          return true;
      } else
        return false;
    }
  }
}
