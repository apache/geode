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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Integration tests extracted from LocatorLoadSnapshotJUnitTest
 */
@Category({MembershipTest.class})
public class LocatorLoadSnapshotIntegrationTest {

  /**
   * A basic test of concurrent functionality. Starts a number of threads making requests and
   * expects the load to be balanced between three servers.
   *
   */
  @Test
  public void testConcurrentBalancing() throws InterruptedException {
    int NUM_THREADS = 50;
    final int NUM_REQUESTS = 10000;
    int ALLOWED_THRESHOLD = 50; // We should never be off by more than
    // the number of concurrent threads.
    final int LOAD_POLL_INTERVAL = 30000;

    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    int initialLoad1 = (int) (Math.random() * (NUM_REQUESTS / 2));
    int initialLoad2 = (int) (Math.random() * (NUM_REQUESTS / 2));
    int initialLoad3 = (int) (Math.random() * (NUM_REQUESTS / 2));

    sn.addServer(l1, uniqueId1, new String[0], new ServerLoad(initialLoad1, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[0], new ServerLoad(initialLoad2, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    sn.addServer(l3, uniqueId3, new String[0], new ServerLoad(initialLoad3, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    final Map loadCounts = new HashMap();
    loadCounts.put(l1, new AtomicInteger(initialLoad1));
    loadCounts.put(l2, new AtomicInteger(initialLoad2));
    loadCounts.put(l3, new AtomicInteger(initialLoad3));

    Thread[] threads = new Thread[NUM_THREADS];
    // final Object lock = new Object();
    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new Thread("Thread-" + i) {
        @Override
        public void run() {
          for (int ii = 0; ii < NUM_REQUESTS; ii++) {
            ServerLocation location;
            // synchronized(lock) {
            location = sn.getServerForConnection(null, Collections.EMPTY_SET);
            // }
            AtomicInteger count = (AtomicInteger) loadCounts.get(location);
            count.incrementAndGet();
          }
        }
      };
    }

    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i].start();
    }

    for (int i = 0; i < NUM_THREADS; i++) {
      Thread t = threads[i];
      long ms = 30 * 1000;
      t.join(30 * 1000);
      if (t.isAlive()) {
        for (int j = 0; j < NUM_THREADS; j++) {
          threads[j].interrupt();
        }
        fail("Thread did not terminate after " + ms + " ms: " + t);
      }
    }

    double expectedPerServer =
        (initialLoad1 + initialLoad2 + initialLoad3 + NUM_REQUESTS * NUM_THREADS)
            / (double) loadCounts.size();
    // for(Iterator itr = loadCounts.entrySet().iterator(); itr.hasNext(); ) {
    // Map.Entry entry = (Entry) itr.next();
    // ServerLocation location = (ServerLocation) entry.getKey();
    // AI count= (AI) entry.getValue();
    // }

    for (final Object o : loadCounts.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      ServerLocation location = (ServerLocation) entry.getKey();
      AtomicInteger count = (AtomicInteger) entry.getValue();
      int difference = (int) Math.abs(count.get() - expectedPerServer);
      assertTrue("Count " + count + " for server " + location + " is not within "
          + ALLOWED_THRESHOLD + " of " + expectedPerServer, difference < ALLOWED_THRESHOLD);
    }
  }

}
