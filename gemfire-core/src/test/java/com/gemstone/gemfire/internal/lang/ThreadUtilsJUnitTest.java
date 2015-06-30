/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

import static org.junit.Assert.*;

import java.lang.Thread.State;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The ThreadUtilsJUnitTest class is a test suite of test cases for testing the contract and functionality of the ThreadUtils
 * class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.lang.ThreadUtils
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@Category(UnitTest.class)
public class ThreadUtilsJUnitTest {

  protected Mockery mockContext;

  @Before
  public void setup() {
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

    assertTrue((t1 - t0) >= 500);
    assertTrue(sleepDuration >= 500);
  }

  @Test
  public void testSleepWithInterrupt() throws Throwable {
    TestFramework.runOnce(new SleepInterruptedMultithreadedTestCase(10 * 1000));
  }

  protected static final class SleepInterruptedMultithreadedTestCase extends MultithreadedTestCase {

    private final long expectedSleepDuration;
    private volatile long actualSleepDuration;

    private final CountDownLatch latch;

    private volatile Thread sleeperThread;

    public SleepInterruptedMultithreadedTestCase(final long expectedSleepDuration) {
      assert expectedSleepDuration > 0 : "The duration of sleep must be greater than equal to 0!";
      this.expectedSleepDuration = expectedSleepDuration;
      this.latch = new CountDownLatch(1);
    }

    public void thread1() {
      assertTick(0);

      sleeperThread = Thread.currentThread();
      sleeperThread.setName("Sleeper Thread");
      this.latch.countDown();
      actualSleepDuration = ThreadUtils.sleep(expectedSleepDuration);
    }

    public void thread2() throws Exception {
      assertTick(0);

      Thread.currentThread().setName("Interrupting Thread");
      this.latch.await();
      sleeperThread.interrupt();
    }

    @Override
    public void finish() {
      assertTrue(actualSleepDuration + " should be <= " + expectedSleepDuration, actualSleepDuration <= expectedSleepDuration);
    }
  }

}
