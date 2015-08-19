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

import com.gemstone.gemfire.internal.NanoTimer.TimeService;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for NanoTimer.
 *
 * @author Kirk Lund
 * @since 7.0
 */
@Category(UnitTest.class)
public class NanoTimerJUnitTest {
  
  /**
   * Simple deterministic clock. Any time you want
   * your clock to tick call incTime.
   */
  private class TestTimeService implements TimeService {
    private long now;
    public void incTime() {
      this.now++;
    }
    @Override
    public long getTime() {
      return this.now;
    }
  }

  @Test
  public void testMillisToNanos() {
    assertEquals(0, NanoTimer.millisToNanos(0));
    assertEquals(1000000, NanoTimer.millisToNanos(1));
  }

  @Test
  public void testNanosToMillis() {
    assertEquals(0, NanoTimer.nanosToMillis(1));
    assertEquals(1, NanoTimer.nanosToMillis(1000000));
  }
  
  @Test
  public void testDefaultNanoTimer() {
    // All the other unit test methods of NanoTimer
    // inject TestTimeService into the NanoTimer.
    // This method verifies that the default constructor
    // works.
    final NanoTimer timer = new NanoTimer();
    timer.getConstructionTime();
    timer.getLastResetTime();
    timer.getTimeSinceConstruction();
    timer.getTimeSinceReset();
    timer.reset();
  }

  @Test
  public void testInitialTimes() {
    TestTimeService ts = new TestTimeService();
    final long nanoTime = ts.getTime();
    final NanoTimer timer = new NanoTimer(ts);

    assertEquals(timer.getConstructionTime(), timer.getLastResetTime());
    assertTrue(timer.getTimeSinceConstruction() <= timer.getTimeSinceReset());
    assertTrue(timer.getLastResetTime() >= nanoTime);
    assertTrue(timer.getConstructionTime() >= nanoTime);
    assertTrue(ts.getTime() >= nanoTime);

    final long nanosOne = ts.getTime();
    
    ts.incTime();
    
    assertEquals(1, timer.getTimeSinceConstruction());
  }
  
  @Test
  public void testReset() {
    TestTimeService ts = new TestTimeService();
    final NanoTimer timer = new NanoTimer(ts);
    final long nanosOne = ts.getTime();
    
    ts.incTime();

    assertEquals(timer.getConstructionTime(), timer.getLastResetTime());
    assertTrue(timer.getTimeSinceConstruction() <= timer.getTimeSinceReset());
    
    final long nanosTwo = ts.getTime();
    final long resetOne = timer.reset();
    
    assertTrue(resetOne >= nanosTwo - nanosOne);
    assertFalse(timer.getConstructionTime() == timer.getLastResetTime());
    
    final long nanosThree = ts.getTime();

    ts.incTime();
    
    assertTrue(timer.getLastResetTime() >= nanosTwo);
    assertTrue(timer.getTimeSinceReset() < timer.getTimeSinceConstruction());
    assertTrue(timer.getLastResetTime() <= nanosThree);
    assertTrue(timer.getTimeSinceReset() <= ts.getTime() - timer.getLastResetTime());
        
    final long nanosFour = ts.getTime();
    final long resetTwo = timer.reset();
    
    assertTrue(resetTwo >= nanosFour - nanosThree);
    
    ts.incTime();

    assertTrue(timer.getLastResetTime() >= nanosFour);
    assertTrue(timer.getTimeSinceReset() < timer.getTimeSinceConstruction());
    assertTrue(timer.getTimeSinceReset() <= ts.getTime() - timer.getLastResetTime());
  }
}
