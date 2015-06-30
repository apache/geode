/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.BlackListListenerAdapter;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.FailureTracker;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class ServerBlackListJUnitTest {
  
  private ScheduledExecutorService background;
  protected ServerBlackList blackList;

  @Before
  public void setUp()  throws Exception {
    background = Executors.newSingleThreadScheduledExecutor();
    blackList = new ServerBlackList(100);
    blackList.start(background);
  }
  
  @After
  public void tearDown() {
    assertTrue(background.shutdownNow().isEmpty());
  }
  
  @Test
  public void testBlackListing()  throws Exception {
    ServerLocation location1 = new ServerLocation("localhost", 1);
    FailureTracker tracker1 = blackList.getFailureTracker(location1);
    tracker1.addFailure();
    tracker1.addFailure();
    assertEquals(Collections.EMPTY_SET,  blackList.getBadServers());
    tracker1.addFailure();
    assertEquals(Collections.singleton(location1),  blackList.getBadServers());
    
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 10000; done = (blackList.getBadServers().size() == 0)) {
        Thread.sleep(200);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("blackList still has bad servers", done);
    
    assertEquals(Collections.EMPTY_SET,  blackList.getBadServers());
  }

  @Test
  public void testListener()  throws Exception {
    
    final AtomicInteger adds = new AtomicInteger();
    final AtomicInteger removes = new AtomicInteger();
    blackList.addListener(new BlackListListenerAdapter() {

      public void serverAdded(ServerLocation location) {
        adds.incrementAndGet();
      }

      public void serverRemoved(ServerLocation location) {
        removes.incrementAndGet();
      }
    });
    
    ServerLocation location1 = new ServerLocation("localhost", 1);
    FailureTracker tracker1 = blackList.getFailureTracker(location1);
    tracker1.addFailure();
    tracker1.addFailure();
    
    assertEquals(0, adds.get());
    assertEquals(0, removes.get());
    tracker1.addFailure();
    assertEquals(1, adds.get());
    assertEquals(0, removes.get());
    
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 10000; done = (removes.get() != 0)) {
        Thread.sleep(200);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("removes still empty", done);
    
    assertEquals(1, adds.get());
    assertEquals(1, removes.get());
  }
}
