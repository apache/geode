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
package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class ConcurrentStartIntegrationTest {

  private Cache cache;
  private int numServers = 10;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY,
        getClass().getSimpleName() + ".properties");
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

  @Test
  public void testCachelessStart() throws InterruptedException {
    runNServers(numServers);
    getCache().close();
  }

  @Test
  public void testCachefulStart() throws InterruptedException {
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();

    runNServers(numServers);
  }

  private void runNServers(int n) throws InterruptedException {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(numServers);
    final Thread[] threads = new Thread[n];
    for (int i = 0; i < n; i++) {
      final int j = i;
      Runnable r = () -> {
        GeodeRedisServer s = new GeodeRedisServer(ports[j]);
        s.start();
        s.shutdown();
      };

      Thread t = new Thread(r);
      t.setDaemon(true);
      t.start();
      threads[i] = t;
    }
    for (Thread t : threads)
      t.join();
    cache = getCache();
    assertFalse(cache.isClosed());
  }

  @SuppressWarnings("deprecation")
  private GemFireCacheImpl getCache() {
    return GemFireCacheImpl.getInstance();
  }
}
