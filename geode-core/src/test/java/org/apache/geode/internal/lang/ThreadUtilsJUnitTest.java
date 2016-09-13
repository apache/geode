/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.lang;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.lang.Thread.State;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The ThreadUtilsJUnitTest class is a test suite of test cases for testing the contract and functionality of the ThreadUtils
 * class.
 * <p/>
 * @see org.apache.geode.internal.lang.ThreadUtils
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class ThreadUtilsJUnitTest {

  protected Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
      setThreadingPolicy(new Synchroniser());
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
  }

  @Test
  public void testGetThreadNameWithNull() {
    assertNull(ThreadUtils.getThreadName(null));
  }

  @Test
  public void testGetThreadNameWithThread() {
    assertNotNull(ThreadUtils.getThreadName(Thread.currentThread()));
  }

  @Test
  public void testInterruptWithNullThread() {
    ThreadUtils.interrupt(null);
  }

  @Test
  public void testInterruptWithNonNullThread() {
    final Thread mockThread = mockContext.mock(Thread.class, "Interrupted Thread");

    mockContext.checking(new Expectations() {{
      oneOf(mockThread).interrupt();
    }});

    ThreadUtils.interrupt(mockThread);
  }

  @Test
  public void testIsAlive() {
    assertTrue(ThreadUtils.isAlive(Thread.currentThread()));
  }

  @Test
  public void testIsAliveWithNullThread() {
    assertFalse(ThreadUtils.isAlive(null));
  }

  @Test
  public void testIsAliveWithUnstartedThread() {
    final Thread thread = new Thread(new Runnable() {
      public void run() {
      }
    });
    assertFalse(ThreadUtils.isAlive(thread));
  }

  @Test
  public void testIsAliveWithStoppedThread() throws InterruptedException {
    final AtomicBoolean ran = new AtomicBoolean(false);

    final Thread thread = new Thread(new Runnable() {
      public void run() {
        ran.set(true);
      }
    });

    thread.start();
    thread.join(50);

    assertFalse(ThreadUtils.isAlive(thread));
    assertTrue(ran.get());
  }

  @Test
  public void testIsWaitingWithNullThread() {
    assertFalse(ThreadUtils.isWaiting(null));
  }

  @Test
  public void testIsWaitingWithRunningThread() {
    final Thread runningThread = mockContext.mock(Thread.class, "Running Thread");

    mockContext.checking(new Expectations() {{
      oneOf(runningThread).getState();
      will(returnValue(State.RUNNABLE));
    }});

    assertFalse(ThreadUtils.isWaiting(runningThread));
  }

  @Test
  public void testIsWaitingWithWaitingThread() {
    final Thread waitingThread = mockContext.mock(Thread.class, "Waiting Thread");

    mockContext.checking(new Expectations() {{
      one(waitingThread).getState();
      will(returnValue(State.WAITING));
    }});

    assertTrue(ThreadUtils.isWaiting(waitingThread));
  }

  @Test
  public void testSleep() {
    final long t0 = System.currentTimeMillis();
    final long sleepDuration = ThreadUtils.sleep(500);
    final long t1 = System.currentTimeMillis();

    assertTrue(t1 > t0);
    assertTrue(sleepDuration > 0);
  }

  @Ignore("This is really just testing Thread.sleep(long)")
  @Test
  public void testSleepWithInterrupt() throws Throwable {
    TestFramework.runOnce(new SleepInterruptedMultithreadedTestCase(10 * 1000));
  }

  protected static final class SleepInterruptedMultithreadedTestCase extends MultithreadedTestCase {

    private final long sleepDuration;

    private volatile Thread sleeperThread;
    private volatile boolean sleeperWasInterrupted;
    private volatile long actualSleepDuration;
    
    public SleepInterruptedMultithreadedTestCase(final long sleepDuration) {
      assert sleepDuration > 0 : "The duration of sleep must be greater than equal to 0!";
      this.sleepDuration = sleepDuration;
    }

    public void thread1() {
      assertTick(0);
      sleeperThread = Thread.currentThread();
      sleeperThread.setName("Sleeper Thread");
      waitForTick(1);
      actualSleepDuration = ThreadUtils.sleep(sleepDuration);
      sleeperWasInterrupted = sleeperThread.isInterrupted();
      assertTick(2);
    }

    public void thread2() throws Exception {
      assertTick(0);
      Thread.currentThread().setName("Interrupting Thread");
      waitForTick(1);
      waitForTick(2);
      sleeperThread.interrupt();
      assertTick(2);
    }

    @Override
    public void finish() {
      assertThat(sleeperWasInterrupted).isTrue();
      assertThat(actualSleepDuration).isGreaterThan(0);
    }
  }

}
