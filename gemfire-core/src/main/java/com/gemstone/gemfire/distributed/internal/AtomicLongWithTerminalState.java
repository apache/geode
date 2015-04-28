/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An atomic integer with update methods that check to see if the value is equal
 * to a special flag. Care should be taken to ensure that the value can never
 * become the special value accidentally. For example, a long that can never go
 * negative with normal use could have a terminal state of Long.MIN_VALUE
 * 
 * @author dsmith
 * @since 6.0
 */
public class AtomicLongWithTerminalState extends AtomicLong {
  
  private static final long serialVersionUID = -6130409343386576390L;
  
  

  public AtomicLongWithTerminalState() {
    super();
  }



  public AtomicLongWithTerminalState(long initialValue) {
    super(initialValue);
  }

  /**
   * Add and the the given delta to the long, unless the long
   * has been set to the terminal state.
   * @param terminalState
   * @param delta
   * @return the new value of the field, or the terminalState if the field
   * is already set to the terminal state.
   */
  public long compareAddAndGet(long terminalState, long delta) {
    while(true) {
      long current = get();
      if(current == terminalState) {
        return terminalState;
      }
      long newValue = current +delta;
      if (compareAndSet(current, newValue)) {
        return newValue;
      }
    }
  }
}
