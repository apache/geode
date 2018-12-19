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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.rules.ConcurrencyRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class ManagementAdapterTest {

  private InternalCache cache = null;
  private DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
  private AtomicBoolean raceConditionFound = new AtomicBoolean(false);

  @Rule
  public ServerStarterRule serverRule =
      new ServerStarterRule().withLogFile().withAutoStart();

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Before
  public void before() {
    cache = serverRule.getCache();
    doReturn(new DiskStoreStats(cache.getInternalDistributedSystem(), "disk-stats"))
        .when(diskStore).getStats();
    doReturn(new File[] {}).when(diskStore).getDiskDirs();
  }

  @Test
  public void testHandlingNotificationsConcurrently() {
    // continuously send cache creation/removal notifications, thread modifying the state
    Callable<Void> cacheNotifications = () -> {
      if (raceConditionFound.get() == Boolean.FALSE) {
        try {
          InternalDistributedSystem ids = cache.getInternalDistributedSystem();
          ids.handleResourceEvent(ResourceEvent.CACHE_REMOVE, cache);
          Thread.sleep(10);
          ids.handleResourceEvent(ResourceEvent.CACHE_CREATE, cache);
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return null;
    };

    // continuously send disk creation/removal, thread relying on state
    Callable<Void> diskNotifications = () -> {
      if (raceConditionFound.get() == Boolean.FALSE) {
        try {
          InternalDistributedSystem ids = cache.getInternalDistributedSystem();
          ids.handleResourceEvent(ResourceEvent.DISKSTORE_CREATE, diskStore);
          Thread.sleep(5);
          ids.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, diskStore);
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return null;
    };

    // scans server log to see if there is null pointer due to caused by cache removal.
    Callable<Boolean> scanLogs = () -> {
      if (raceConditionFound.get() == Boolean.FALSE) {
        try {
          File logFile = new File("server.log");
          Scanner scanner = new Scanner(logFile);
          while (scanner.hasNextLine()) {
            final String lineFromFile = scanner.nextLine();
            if (lineFromFile.contains("java.lang.NullPointerException")) {
              raceConditionFound.set(Boolean.TRUE);
              break;
            }
          }
        } catch (FileNotFoundException e) {
          // ignore this exception as the temp file might have been deleted after timeout
        }
      }
      return raceConditionFound.get();
    };

    Duration notificationRunDuration = Duration.ofSeconds(3);
    Duration scannerRunDuration = Duration.ofSeconds(4);
    Duration timeout = Duration.ofSeconds(10);

    concurrencyRule.setTimeout(timeout);
    concurrencyRule.add(cacheNotifications).repeatForDuration(notificationRunDuration);
    concurrencyRule.add(diskNotifications).repeatForDuration(notificationRunDuration);
    concurrencyRule.add(scanLogs).repeatForDuration(scannerRunDuration).expectValue(Boolean.FALSE);
    concurrencyRule.executeInParallel();
  }
}
