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

package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.command.AcceptorImplObserver;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import static org.junit.Assert.*;

/**
 * Tests for AcceptorImpl.
 */
@Category(DistributedTest.class)
public class AcceptorImplDUnitTest extends JUnit4DistributedTestCase {
  private static Cache cache;

  public AcceptorImplDUnitTest() {
    super();
  }

  @Override
  public void postTearDown() throws Exception {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    super.postTearDown();
  }

  public static class SleepyCacheWriter<K, V> extends CacheWriterAdapter<K, V> {
    @Override
    public void beforeCreate(EntryEvent<K, V> event) {
      while (true) {
        System.out.println("Sleeping a long time.");
        try {
          Thread.sleep(100000000);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }

  /**
   * Dump threads to standard out. For debugging.
   */
  private void dumpThreads() {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos = bean.dumpAllThreads(true, true);
    System.out.println("infos = " + Arrays.toString(infos));
  }

  /**
   * GEODE-2324. There was a bug where, due to an uncaught exception, `AcceptorImpl.close()` was
   * short-circuiting and failing to clean up properly.
   *
   * What this test does is start a Cache and hook the Acceptor to interrupt the thread before the
   * place where an InterruptedException could be thrown. It interrupts the thread, and checks that
   * the thread has terminated normally without short-circuiting. It doesn't check that every part
   * of the AcceptorImpl has shut down properly -- that seems both difficult to check (especially
   * since the fields are private) and implementation-dependent.
   */
  @Test
  public void testShutdownCatchesException() throws Exception {
    final String hostname = Host.getHost(0).getHostName();
    final VM clientVM = Host.getHost(0).getVM(0);

    // AtomicBooleans can be set from wherever they are, including an anonymous class or other
    // thread.
    AtomicBoolean terminatedNormally = new AtomicBoolean(false);
    AtomicBoolean passedPostConditions = new AtomicBoolean(false);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");

    AcceptorImpl.setObserver_TESTONLY(new AcceptorImplObserver() {
      @Override
      public void beforeClose(AcceptorImpl acceptorImpl) {
        Thread.currentThread().interrupt();
      }

      @Override
      public void normalCloseTermination(AcceptorImpl acceptorImpl) {
        terminatedNormally.set(true);
      }

      @Override
      public void afterClose(AcceptorImpl acceptorImpl) {
        passedPostConditions.set(!acceptorImpl.isRunning());
      }
    });

    try (InternalCache cache = (InternalCache) new CacheFactory(props).create()) {
      RegionFactory<Object, Object> regionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);

      regionFactory.setCacheWriter(new SleepyCacheWriter<>());

      final CacheServer server = cache.addCacheServer();
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.setPort(port);
      server.start();

      regionFactory.create("region1");

      clientVM.invokeAsync(() -> {
        ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
        clientCacheFactory.addPoolServer(hostname, port);
        ClientCache clientCache = clientCacheFactory.create();
        Region<Object, Object> clientRegion1 =
            clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region1");
        clientRegion1.put("foo", "bar");
      });

      cache.close();

      dumpThreads();
      assertTrue(terminatedNormally.get());
      assertTrue(passedPostConditions.get());

      // cleanup.
      AcceptorImpl.setObserver_TESTONLY(new AcceptorImplObserver() {
        @Override
        public void beforeClose(AcceptorImpl acceptorImpl) {}

        @Override
        public void normalCloseTermination(AcceptorImpl acceptorImpl) {}

        @Override
        public void afterClose(AcceptorImpl acceptorImpl) {}
      });
    }
  }
}
