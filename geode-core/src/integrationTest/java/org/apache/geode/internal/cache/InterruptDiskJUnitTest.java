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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Test of interrupting threads doing disk writes to see the effect.
 */
public class InterruptDiskJUnitTest {

  private final AtomicInteger nextValue = new AtomicInteger();
  private final AtomicReference<Thread> puttingThread = new AtomicReference<>();

  private Cache cache;
  private Region<Object, Object> region;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    String diskStoreName = getClass().getSimpleName() + "_diskStore";
    String regionName = getClass().getSimpleName() + "_region";

    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    File diskDir = temporaryFolder.getRoot();

    cache = new CacheFactory(config).create();

    cache.createDiskStoreFactory().setMaxOplogSize(1).setDiskDirs(new File[] {diskDir})
        .create(diskStoreName);

    region = cache.createRegionFactory(REPLICATE_PERSISTENT).setDiskStoreName(diskStoreName)
        .create(regionName);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testDRPutWithInterrupt() throws Exception {
    Future<Void> doPutWhileNotInterrupted = executorServiceRule.runAsync(() -> {
      puttingThread.set(Thread.currentThread());
      while (!Thread.currentThread().isInterrupted()) {
        region.put(0, nextValue.incrementAndGet());
      }
    });

    await().untilAsserted(() -> assertThat(puttingThread).isNotNull());
    Thread.sleep(Duration.ofSeconds(1).toMillis());
    puttingThread.get().interrupt();

    doPutWhileNotInterrupted.get(2, MINUTES);
    assertThat(region.get(0)).isEqualTo(nextValue.get());
  }
}
