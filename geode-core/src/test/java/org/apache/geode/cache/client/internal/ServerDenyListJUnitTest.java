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
package org.apache.geode.cache.client.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.internal.ServerDenyList.DenyListListenerAdapter;
import org.apache.geode.cache.client.internal.ServerDenyList.FailureTracker;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ServerDenyListJUnitTest {

  private ScheduledExecutorService background;
  private ServerDenyList denyList;

  @Before
  public void setUp() throws Exception {
    background = Executors.newSingleThreadScheduledExecutor();
    denyList = new ServerDenyList(100);
    denyList.start(background);
  }

  @After
  public void tearDown() {
    assertTrue(background.shutdownNow().isEmpty());
  }

  @Test
  public void testDenyListing() throws Exception {
    ServerLocation location1 = new ServerLocation("localhost", 1);
    FailureTracker tracker1 = denyList.getFailureTracker(location1);
    tracker1.addFailure();
    tracker1.addFailure();
    assertEquals(Collections.EMPTY_SET, denyList.getBadServers());
    tracker1.addFailure();
    assertEquals(Collections.singleton(location1), denyList.getBadServers());

    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 10000; done =
        (denyList.getBadServers().size() == 0)) {
      Thread.sleep(200);
    }
    assertTrue("denyList still has bad servers", done);

    assertEquals(Collections.EMPTY_SET, denyList.getBadServers());
  }

  @Test
  public void testListener() throws Exception {
    final AtomicInteger adds = new AtomicInteger();
    final AtomicInteger removes = new AtomicInteger();
    denyList.addListener(new DenyListListenerAdapter() {

      @Override
      public void serverAdded(ServerLocation location) {
        adds.incrementAndGet();
      }

      @Override
      public void serverRemoved(ServerLocation location) {
        removes.incrementAndGet();
      }
    });

    ServerLocation location1 = new ServerLocation("localhost", 1);
    FailureTracker tracker1 = denyList.getFailureTracker(location1);
    tracker1.addFailure();
    tracker1.addFailure();

    assertEquals(0, adds.get());
    assertEquals(0, removes.get());
    tracker1.addFailure();
    assertEquals(1, adds.get());
    assertEquals(0, removes.get());

    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 10000; done =
        (removes.get() != 0)) {
      Thread.sleep(200);
    }
    assertTrue("removes still empty", done);

    assertEquals(1, adds.get());
    assertEquals(1, removes.get());
  }
}
