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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests for AcceptorImpl.
 */
@Category(DistributedTest.class)
public class AcceptorImplDUnitTest extends JUnit4DistributedTestCase {

  public AcceptorImplDUnitTest() {
    super();
  }

  // SleepyCacheWriter will block indefinitely.
  // Anyone who has a handle on the SleepyCacheWriter can interrupt it by calling wakeUp.
  class SleepyCacheWriter<K, V> extends CacheWriterAdapter<K, V> {
    private boolean setOnStart;
    private boolean setOnInterrupt;
    private boolean stopWaiting;
    // locks the above three booleans.
    private final Object lock = new Object();

    public void notifyStart() {
      synchronized (lock) {
        setOnStart = true;
      }
    }

    public boolean isStarted() {
      synchronized (lock) {
        return setOnStart;
      }
    }

    public void notifyInterrupt() {
      synchronized (lock) {
        setOnInterrupt = true;
      }
    }

    public boolean isInterrupted() {
      synchronized (lock) {
        return setOnInterrupt;
      }
    }

    public void stopWaiting() {
      synchronized (lock) {
        this.stopWaiting = true;
        lock.notify();
      }
    }

    public boolean isReadyToQuit() {
      synchronized (lock) {
        return stopWaiting;
      }
    }

    SleepyCacheWriter() {}

    @Override
    public void beforeCreate(EntryEvent<K, V> event) {
      System.out.println("Sleeping a long time.");
      notifyStart();
      while (!isReadyToQuit()) {
        try {
          synchronized (lock) {
            lock.wait();
          }
        } catch (InterruptedException ex) {
          notifyInterrupt();
        }
      }
      if (isInterrupted()) {
        Thread.currentThread().interrupt();
      }
    }
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
  public void testAcceptorImplCloseCleansUpWithHangingConnection() throws Exception {
    final String hostname = Host.getHost(0).getHostName();
    final VM clientVM = Host.getHost(0).getVM(0);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");

    try (InternalCache cache = (InternalCache) new CacheFactory(props).create()) {
      RegionFactory<Object, Object> regionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);

      SleepyCacheWriter<Object, Object> sleepyCacheWriter = new SleepyCacheWriter<>();
      regionFactory.setCacheWriter(sleepyCacheWriter);

      final CacheServer server = cache.addCacheServer();
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.setPort(port);
      server.start();

      regionFactory.create("region1");

      assertTrue(cache.isServer());
      assertFalse(cache.isClosed());

      Awaitility.await("Acceptor is up and running").atMost(10, SECONDS)
          .until(() -> getAcceptorImplFromCache(cache) != null);
      AcceptorImpl acceptorImpl = getAcceptorImplFromCache(cache);


      clientVM.invokeAsync(() -> {
        // System.setProperty("gemfire.PoolImpl.TRY_SERVERS_ONCE", "true");
        ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
        clientCacheFactory.addPoolServer(hostname, port);
        clientCacheFactory.setPoolReadTimeout(5000);
        clientCacheFactory.setPoolRetryAttempts(1);
        clientCacheFactory.setPoolMaxConnections(1);
        clientCacheFactory.setPoolFreeConnectionTimeout(1000);
        ClientCache clientCache = clientCacheFactory.create();
        Region<Object, Object> clientRegion1 =
            clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region1");
        clientRegion1.put("foo", "bar");
      });

      Awaitility.await("Cache writer starts").atMost(10, SECONDS)
          .until(sleepyCacheWriter::isStarted);

      cache.close();

      Awaitility.await("Cache writer interrupted").atMost(10, SECONDS)
          .until(sleepyCacheWriter::isInterrupted);

      sleepyCacheWriter.stopWaiting();

      Awaitility.await("Acceptor shuts down properly").atMost(10, SECONDS)
          .until(() -> acceptorImpl.isShutdownProperly());

      ThreadUtils.dumpMyThreads(); // for debugging.

      regionFactory.setCacheWriter(null);
    }
  }


  @Test
  public void testAcceptorImplCloseCleansUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");

    try (InternalCache cache = (InternalCache) new CacheFactory(props).create()) {
      RegionFactory<Object, Object> regionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);

      final CacheServer server = cache.addCacheServer();
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      server.setPort(port);
      server.start();

      regionFactory.create("region1");

      assertTrue(cache.isServer());
      assertFalse(cache.isClosed());
      Awaitility.await("Acceptor is up and running").atMost(10, SECONDS)
          .until(() -> getAcceptorImplFromCache(cache) != null);

      AcceptorImpl acceptorImpl = getAcceptorImplFromCache(cache);

      cache.close();
      Awaitility.await("Acceptor shuts down properly").atMost(10, SECONDS)
          .until(acceptorImpl::isShutdownProperly);

      assertTrue(cache.isClosed());
      assertFalse(acceptorImpl.isRunning());
    }
  }

  /**
   *
   * @return the cache's Acceptor, if there is exactly one CacheServer. Otherwise null.
   */
  public AcceptorImpl getAcceptorImplFromCache(GemFireCache cache) {
    GemFireCacheImpl gemFireCache = (GemFireCacheImpl) cache;
    List<CacheServer> cacheServers = gemFireCache.getCacheServers();
    if (cacheServers.size() != 1) {
      return null;
    }

    CacheServerImpl cacheServerImpl = (CacheServerImpl) cacheServers.get(0);
    return cacheServerImpl.getAcceptor();
  }
}
