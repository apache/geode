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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.BlackListListenerAdapter;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList.FailureTracker;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.junit.UnitTest;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class ServerBlackListJUnitTest {
  
  private ExecutorService futures;
  private ScheduledExecutorService background;
  protected ServerBlackList blackList;

  @Before
  public void setUp()  throws Exception {
    futures = Executors.newSingleThreadExecutor();
    background = Executors.newSingleThreadScheduledExecutor();
    blackList = new ServerBlackList(100);
    blackList.start(background);
  }
  
  @After
  public void tearDown() {
    try {
      assertTrue(futures.shutdownNow().isEmpty());
    } finally {
      assertTrue(background.shutdownNow().isEmpty());
    }
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
    
    Future<Boolean> waitForBlackListToEmpty = this.futures.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        boolean done = false;
        try {
          for (; !done; done = (blackList.getBadServers().size() == 0)) {
            Thread.sleep(200);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return done;
      }
    });
    assertTrue("blackList still has bad servers", waitForBlackListToEmpty.get(10, TimeUnit.SECONDS));
    
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
    
    Future<Boolean> waitForRemoves = this.futures.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        boolean done = false;
        try {
          for (; !done; done = (removes.get() != 0)) {
            Thread.sleep(200);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return done;
      }
    });
    assertTrue("removes still empty", waitForRemoves.get(10, TimeUnit.SECONDS));

    assertEquals(1, adds.get());
    assertEquals(1, removes.get());
  }
}
