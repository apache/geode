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
package org.apache.geode.cache.snapshot;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.examples.snapshot.MyObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.test.junit.categories.SnapshotTest;

@Category({SnapshotTest.class})
public class WanSnapshotJUnitTest extends SnapshotTestCase {
  private Region<Integer, MyObject> region;
  private WanListener wan;

  @Test
  public void testWanCallback() throws Exception {
    int count = 1000;
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "clienttest " + i));
    }

    File snapshot = new File(getSnapshotDirectory(), "wan.snapshot.gfd");
    region.getSnapshotService().save(snapshot, SnapshotFormat.GEMFIRE);
    region.clear();

    await()
        .until(() -> wan.ticker.compareAndSet(count, 0));

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
    public void close() {}

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      ticker.incrementAndGet();
      return true;
    }
  }

}
