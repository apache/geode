/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.sequencelog.SequenceLoggerImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class Bug49856JUnitTest {

  public Bug49856JUnitTest() {
  }

  @Test
  public void testNoGFThreadsRunningPostCacheClose() throws Exception {

    ClientCacheFactory ccf = new ClientCacheFactory();
    GemFireCacheImpl cache = (GemFireCacheImpl)ccf.create();

    SystemFailure.getFailure();
    SequenceLoggerImpl.getInstance();

    Thread.sleep(5000);
    // Assert the threads have been started.
    checkThreads(true);

    cache.close();

    Thread.sleep(5000);
    // Assert the threads have been terminated.
    checkThreads(false);
  }

  private void checkThreads(boolean expectThreads) {
    boolean proctorRunning = false;
    boolean watchDogRunning = false;
    boolean loggerConsumerRunning = false;

    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains("SystemFailure WatchDog")) {
        watchDogRunning = true;
      } else if (t.getName().contains("SystemFailure Proctor")) {
        proctorRunning = true;
      } else if (t.getName().contains("State Logger Consumer Thread")) {
        loggerConsumerRunning = true;
      }
    }
    StringBuilder sb = new StringBuilder(new Date(System.currentTimeMillis()).toString());
    sb.append((watchDogRunning ^ expectThreads) ? " SystemFailure WatchDog, " : " ");
    sb.append((proctorRunning ^ expectThreads) ? "SystemFailure Proctor, " : "");
    sb.append((loggerConsumerRunning ^ expectThreads) ? "State Logger Consumer Thread " : "");
    sb.append(expectThreads ? "not started." : "still running.");

    if (expectThreads) {
      assertTrue(sb.toString(), proctorRunning && watchDogRunning && loggerConsumerRunning);
    } else {
      assertTrue(sb.toString(), !proctorRunning && !watchDogRunning && !loggerConsumerRunning);
    }
  }

}
