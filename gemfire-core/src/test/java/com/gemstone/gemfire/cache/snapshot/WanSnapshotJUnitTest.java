/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.examples.snapshot.MyObject;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class WanSnapshotJUnitTest extends SnapshotTestCase {
  private Region<Integer, MyObject> region;
  private WanListener wan;
  private static final long MAX_WAIT = 5 * 60 * 1000; //6 minutes
  
  @Test
  public void testWanCallback() throws Exception {
    int count = 1000;
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "clienttest " + i));
    }

    File snapshot = new File("wan.snapshot");
    region.getSnapshotService().save(snapshot, SnapshotFormat.GEMFIRE);
    region.clear();
    
    long start = System.currentTimeMillis();
    // wait for the events to drain out
    while (!wan.ticker.compareAndSet(count, 0)) {
      Thread.sleep(100);
      if(System.currentTimeMillis() - start > MAX_WAIT) {
        fail("Event did not drain in 5 minutes");
      }
    }

    region.getSnapshotService().load(snapshot, SnapshotFormat.GEMFIRE);

    // delay, just in case we get any events
    Thread.sleep(1000);
    
    assertEquals("WAN callback detected during import", 0, wan.ticker.get());
    assertEquals(count, region.size());
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    
    wan = new WanListener();
    cache.createAsyncEventQueueFactory().setBatchSize(1).create("wanqueue", wan);
    region = cache.<Integer, MyObject>createRegionFactory(RegionShortcut.REPLICATE)
        .addAsyncEventQueueId("wanqueue").create("test");
  }
  
  private class WanListener implements AsyncEventListener {
    private final AtomicInteger ticker = new AtomicInteger(0);
    
    @Override
    public void close() {
    }

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      ticker.incrementAndGet();
      return true;
    }
  }

}

