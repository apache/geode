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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Regression test that confirms bug GEODE-3407.
 *
 * <p>
 * GEODE-3407: JMX and membership may deadlock on CacheFactory.getAnyInstance
 */
@Category(UnitTest.class)
public class CacheServerBridgeClientMembershipRegressionTest {

  private final AtomicBoolean after = new AtomicBoolean();
  private final AtomicBoolean before = new AtomicBoolean();

  private CacheServerBridge cacheServerBridge;

  private ExecutorService synchronizing;
  private ExecutorService blocking;
  private CountDownLatch latch;

  private InternalCache cache;
  private CacheServerImpl cacheServer;
  private AcceptorImpl acceptor;
  private MBeanStatsMonitor monitor;

  @Before
  public void setUp() throws Exception {
    this.synchronizing = Executors.newSingleThreadExecutor();
    this.blocking = Executors.newSingleThreadExecutor();
    this.latch = new CountDownLatch(1);

    this.cache = mock(InternalCache.class);
    this.cacheServer = mock(CacheServerImpl.class);
    this.acceptor = mock(AcceptorImpl.class);
    this.monitor = mock(MBeanStatsMonitor.class);

    when(cache.getQueryService()).thenReturn(mock(InternalQueryService.class));
    when(acceptor.getStats()).thenReturn(mock(CacheServerStats.class));
  }

  @After
  public void tearDown() throws Exception {
    if (latch.getCount() > 0) {
      latch.countDown();
    }
  }

  @Test
  public void getNumSubscriptionsDeadlocksOnCacheFactory() throws Exception {
    givenCacheFactoryIsSynchronized();
    givenCacheServerBridge();

    blocking.execute(() -> {
      try {
        before.set(true);

        // getNumSubscriptions -> getClientQueueSizes -> synchronizes on CacheFactory
        cacheServerBridge.getNumSubscriptions();

      } catch (CacheClosedException ignored) {
      } finally {
        after.set(true);
      }
    });

    await().atMost(10, SECONDS).until(() -> before.get());

    // if deadlocked, then this line will throw ConditionTimeoutException
    await().atMost(10, SECONDS).until(() -> assertThat(after.get()).isTrue());
  }

  private void givenCacheFactoryIsSynchronized() {
    synchronizing.execute(() -> {
      synchronized (CacheFactory.class) {
        try {
          latch.await(2, MINUTES);
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
      }
    });
  }

  private void givenCacheServerBridge() {
    cacheServerBridge = new CacheServerBridge(cache, cacheServer, acceptor, monitor);
  }

}
