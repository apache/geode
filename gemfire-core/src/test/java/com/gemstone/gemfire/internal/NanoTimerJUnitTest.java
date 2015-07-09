/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for NanoTimer. This is in addition to NanoTimer2JUnitTest which is
 * an older a JUnit test case for NanoTimer.
 *
 * @author Kirk Lund
 * @since 7.0
 * @see NanoTimer2JUnitTest
 */
@Category(UnitTest.class)
public class NanoTimerJUnitTest {

  @Test
  public void testGetTimeIsPositive() {
    long lastTime = 0;
    for (int i = 0; i < 1000; i++) {
      final long time = NanoTimer.getTime();
      assertTrue(time >= 0);
      assertTrue(time >= lastTime);
      lastTime = time;
    }
  }
  
  @Test
  public void testGetTimeIncreases() {
    final long startNanos = NanoTimer.getTime();
    final long startMillis = System.currentTimeMillis();

    waitMillis(10);

    final long endMillis = System.currentTimeMillis();
    final long endNanos = NanoTimer.getTime();
    
    long elapsedMillis = endMillis - startMillis;
    long elapsedNanos = endNanos - startNanos;
    
    assertTrue(elapsedMillis > 10);
    assertTrue(endNanos > NanoTimer.NANOS_PER_MILLISECOND * 10);
    assertTrue(elapsedNanos * NanoTimer.NANOS_PER_MILLISECOND >= elapsedMillis);
  }

  @Test
  public void testInitialTimes() {
    final long nanoTime = NanoTimer.getTime();
    final NanoTimer timer = new NanoTimer();

    assertEquals(timer.getConstructionTime(), timer.getLastResetTime());
    assertTrue(timer.getTimeSinceConstruction() <= timer.getTimeSinceReset());
    assertTrue(timer.getLastResetTime() >= nanoTime);
    assertTrue(timer.getConstructionTime() >= nanoTime);
    assertTrue(NanoTimer.getTime() >= nanoTime);

    final long nanosOne = NanoTimer.getTime();
    
    waitMillis(10);
    
    assertTrue(timer.getTimeSinceConstruction() > NanoTimer.NANOS_PER_MILLISECOND * 10);
    assertTrue(timer.getTimeSinceConstruction() <= NanoTimer.getTime());
    
    final long nanosTwo = NanoTimer.getTime();
    
    assertTrue(timer.getTimeSinceConstruction() >= nanosTwo - nanosOne);
  }
  
  @Test
  public void testReset() {
    final NanoTimer timer = new NanoTimer();
    final long nanosOne = NanoTimer.getTime();
    
    waitMillis(10);

    assertEquals(timer.getConstructionTime(), timer.getLastResetTime());
    assertTrue(timer.getTimeSinceConstruction() <= timer.getTimeSinceReset());
    
    final long nanosTwo = NanoTimer.getTime();
    final long resetOne = timer.reset();
    
    assertTrue(resetOne >= nanosTwo - nanosOne);
    assertFalse(timer.getConstructionTime() == timer.getLastResetTime());
    
    final long nanosThree = NanoTimer.getTime();

    waitMillis(10);
    
    assertTrue(timer.getLastResetTime() >= nanosTwo);
    assertTrue(timer.getTimeSinceReset() < timer.getTimeSinceConstruction());
    assertTrue(timer.getLastResetTime() <= nanosThree);
    assertTrue(timer.getTimeSinceReset() < NanoTimer.getTime());
    assertTrue(timer.getTimeSinceReset() <= NanoTimer.getTime() - timer.getLastResetTime());
        
    final long nanosFour = NanoTimer.getTime();
    final long resetTwo = timer.reset();
    
    assertTrue(resetTwo >= nanosFour - nanosThree);
    
    waitMillis(10);

    assertTrue(timer.getLastResetTime() >= nanosFour);
    assertTrue(timer.getTimeSinceReset() < timer.getTimeSinceConstruction());
    assertTrue(timer.getLastResetTime() <= NanoTimer.getTime());
    assertTrue(timer.getTimeSinceReset() <= NanoTimer.getTime() - timer.getLastResetTime());
  }
  
  /**
   * Waits for the specified milliseconds to pass as measured by
   * {@link java.lang.System#currentTimeMillis()}.
   */
  private void waitMillis(final long millis) {
    long began = System.currentTimeMillis();
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < millis; done = (System.currentTimeMillis() - began > millis)) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("waiting " + millis + " millis", done);
  }
}
