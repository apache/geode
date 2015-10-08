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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.internal.HoplogListenerForRegion;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;


@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSRegionDirectorJUnitTest extends BaseHoplogTestCase {
  public void testDirector() throws Exception {
    int bucketId = 0;

    HdfsRegionManager mgr = regionManager;
    
    // no buckets have been created so far.
    assertEquals(0, director.getBucketCount("/" + getName()));

    // one bucket created
    mgr.create(bucketId);
    assertEquals(1, director.getBucketCount("/" + getName()));

    // close bucket test
    mgr.close(bucketId);
    
    // all buckets have been closed.
    assertEquals(0, director.getBucketCount("/" + getName()));

    mgr.create(bucketId);
    assertEquals(1, director.getBucketCount("/" + getName()));
    director.clear("/" + getName());
    try {
      assertEquals(0, director.getBucketCount("/" + getName()));
      fail("The region is no longer managed, hence an exception is expected");
    } catch (IllegalStateException e) {
      // exception expected as the region is no longer managed
    }
  }
  
  public void testCompactionEvents() throws Exception {
    final AtomicInteger counter = new AtomicInteger(0);
    HoplogListener myListener = new HoplogListener() {
      public void hoplogDeleted(String regionFolder, int bucketId, Hoplog... oplogs)
          throws IOException {
      }
      public void hoplogCreated(String regionFolder, int bucketId, Hoplog... oplogs)
          throws IOException {
      }
      public void compactionCompleted(String region, int bucket, boolean isMajor) {
        counter.incrementAndGet();
      }
    };

    HoplogListenerForRegion listenerManager = ((LocalRegion)region).getHoplogListener();
    listenerManager.addListener(myListener);
    
    HoplogOrganizer bucket = regionManager.create(0);
    // #1
    ArrayList<PersistedEventImpl> items = new ArrayList<PersistedEventImpl>();
    items.add(new TestEvent("1", "1"));
    bucket.flush(items.iterator(), items.size());

    // #2
    items.clear();
    items.add(new TestEvent("2", "1"));
    bucket.flush(items.iterator(), items.size());

    // #3
    items.clear();
    items.add(new TestEvent("3", "1"));
    bucket.flush(items.iterator(), items.size());
    
    // #4
    items.clear();
    items.add(new TestEvent("4", "1"));
    bucket.flush(items.iterator(), items.size());
    
    bucket.getCompactor().compact(false, false);
    assertEquals(1, counter.get());
  }
}
