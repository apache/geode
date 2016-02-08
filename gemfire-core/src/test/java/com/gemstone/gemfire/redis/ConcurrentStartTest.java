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
package com.gemstone.gemfire.redis;

import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ConcurrentStartTest {

  int numServers = 10;
  @Test
  public void testCachelessStart() throws InterruptedException {
    runNServers(numServers);
    GemFireCacheImpl.getInstance().close();
  }
  @Test
  public void testCachefulStart() throws InterruptedException {
    CacheFactory cf = new CacheFactory();
    cf.set("mcast-port", "0");
    cf.set("locators", "");
    Cache c = cf.create();
    runNServers(numServers);
    c.close();
  }
  
  private void runNServers(int n) throws InterruptedException {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(numServers);
    final Thread[] threads = new Thread[n];
    for (int i = 0; i < n; i++) {
      final int j = i;
      Runnable r = new Runnable() {

        @Override
        public void run() {
          GemFireRedisServer s = new GemFireRedisServer(ports[j]);
          s.start();
          s.shutdown();
        }
      };
      
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.start();
      threads[i] = t;
    }
    for (Thread t : threads)
      t.join();
    Cache c = GemFireCacheImpl.getInstance();
    assertFalse(c.isClosed());
  }
}
