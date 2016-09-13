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
package org.apache.geode.internal;

import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.sequencelog.SequenceLoggerImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class Bug49856JUnitTest {

  public Bug49856JUnitTest() {
  }

  @Test
  public void testNoGFThreadsRunningPostCacheClose() throws Exception {

    ClientCacheFactory ccf = new ClientCacheFactory();
    GemFireCacheImpl cache = (GemFireCacheImpl)ccf.create();

    SystemFailure.getFailure();

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
      }
    }
    StringBuilder sb = new StringBuilder(new Date(System.currentTimeMillis()).toString());
    sb.append((watchDogRunning ^ expectThreads) ? " SystemFailure WatchDog, " : " ");
    sb.append((proctorRunning ^ expectThreads) ? "SystemFailure Proctor, " : "");
    sb.append(expectThreads ? "not started." : "still running.");

    if (expectThreads) {
      assertTrue(sb.toString(), proctorRunning && watchDogRunning);
    } else {
      assertTrue(sb.toString(), !proctorRunning && !watchDogRunning);
    }
  }

}
