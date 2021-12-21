/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.internal.NanoTimer.TimeService;

/**
 * Unit tests for NanoTimer.
 *
 * @since GemFire 7.0
 */
public class NanoTimerJUnitTest {

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

  /**
   * Simple deterministic clock. Any time you want your clock to tick call incTime.
   */
  private class TestTimeService implements TimeService {
    private long now;

    public void incTime() {
      now++;
    }

    @Override
    public long getTime() {
      return now;
    }
  }
}
