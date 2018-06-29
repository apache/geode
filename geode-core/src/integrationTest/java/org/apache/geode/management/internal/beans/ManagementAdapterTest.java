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
package org.apache.geode.management.internal.beans;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class ManagementAdapterTest {

  private InternalCache cache = null;
  private DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
  private volatile boolean race = false;

  @Rule
  public ServerStarterRule serverRule =
      new ServerStarterRule().withWorkingDir().withLogFile().withAutoStart();

  @Before
  public void before() {
    cache = serverRule.getCache();
    doReturn(new DiskStoreStats(cache.getInternalDistributedSystem(), "disk-stats")).when(diskStore)
        .getStats();
    doReturn(new File[] {}).when(diskStore).getDiskDirs();
  }

  @Test
  public void testHandlingNotificationsConcurrently() throws InterruptedException {
    /*
     * Tests to see if there are any concurrency issues handling resource lifecycle events.
     *
     * There are three runnables with specific tasks as below:
     * r1 - continuously send cache creation/removal notifications, thread modifying the state
     * r2 - continuously send disk creation/removal, thread relying on state
     * r3 - monitors log to see if there is a null pointer due race'
     *
     * Test runs at most 2 seconds or until a race.
     */

    Runnable r1 = () -> {
      while (!race) {
        try {
          cache.getInternalDistributedSystem().handleResourceEvent(ResourceEvent.CACHE_REMOVE,
              cache);
          Thread.sleep(10);
          cache.getInternalDistributedSystem().handleResourceEvent(ResourceEvent.CACHE_CREATE,
              cache);
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };

    Runnable r2 = () -> {
      while (!race) {
        try {
          cache.getInternalDistributedSystem().handleResourceEvent(ResourceEvent.DISKSTORE_CREATE,
              diskStore);
          Thread.sleep(5);
          cache.getInternalDistributedSystem().handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE,
              diskStore);
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };

    // r3 scans server log to see if there is null pointer due to caused by cache removal.
    Runnable r3 = () -> {
      while (!race) {
        try {
          File logFile = new File(serverRule.getWorkingDir() + "/server.log");
          Scanner scanner = new Scanner(logFile);
          while (scanner.hasNextLine()) {
            final String lineFromFile = scanner.nextLine();
            if (lineFromFile.contains("java.lang.NullPointerException")) {
              race = true;
              break;
            }
          }
        } catch (FileNotFoundException e) {
          // ignore this exception as the temp file might have been deleted after timeout
        }
      }
    };

    List<Runnable> runnables = Arrays.asList(r1, r2, r3);

    final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    try {
      final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
      final CountDownLatch afterInitBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      for (final Runnable submittedTestRunnable : runnables) {
        threadPool.submit(() -> {
          allExecutorThreadsReady.countDown();
          try {
            afterInitBlocker.await();
            submittedTestRunnable.run();
          } catch (final Throwable e) {
            exceptions.add(e);
          } finally {
            allDone.countDown();
          }
        });
      }
      // wait until all threads are ready
      allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS);
      // start all test runners
      afterInitBlocker.countDown();
      // wait until all done or timeout
      allDone.await(2, TimeUnit.SECONDS);
    } finally {
      threadPool.shutdownNow();
    }
    assertThat(exceptions).as("failed with exception(s)" + exceptions).isEmpty();
    assertThat(race).as("is service to be null due to race").isEqualTo(false);
  }
}
