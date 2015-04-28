/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class AtomicLongWithTerminalStateJUnitTest extends TestCase {
  
  public void test() {
    AtomicLongWithTerminalState al = new AtomicLongWithTerminalState();
    assertEquals(23, al.compareAddAndGet(-1, 23));
    assertEquals(23, al.getAndSet(-1));
    // test for terminal state
    assertEquals(-1, al.compareAddAndGet(-1, 12));
    // test for normal delta
    assertEquals(11, al.compareAddAndGet(0, 12));
  }
}
