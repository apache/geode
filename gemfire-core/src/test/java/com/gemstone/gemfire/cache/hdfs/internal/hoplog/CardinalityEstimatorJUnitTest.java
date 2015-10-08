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

import java.util.ArrayList;
import java.util.List;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;


@Category({IntegrationTest.class, HoplogTest.class})
public class CardinalityEstimatorJUnitTest extends BaseHoplogTestCase {

  public void testSingleHoplogCardinality() throws Exception {
    int count = 10;
    int bucketId = (int) System.nanoTime();
    HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    // assert that size is 0 before flush begins
    assertEquals(0, organizer.sizeEstimate());
    organizer.flush(items.iterator(), count);

    assertEquals(count, organizer.sizeEstimate());
    assertEquals(0, stats.getActiveReaderCount());
    
    organizer.close();
    organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);
    assertEquals(count, organizer.sizeEstimate());
    assertEquals(1, stats.getActiveReaderCount());
  }

  public void testSingleHoplogCardinalityWithDuplicates() throws Exception {
    int bucketId = (int) System.nanoTime();
    HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    List<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent("key-0", "value-0"));
    items.add(new TestEvent("key-0", "value-0"));
    items.add(new TestEvent("key-1", "value-1"));
    items.add(new TestEvent("key-2", "value-2"));
    items.add(new TestEvent("key-3", "value-3"));
    items.add(new TestEvent("key-3", "value-3"));
    items.add(new TestEvent("key-4", "value-4"));

    organizer.flush(items.iterator(), 7);
    assertEquals(5, organizer.sizeEstimate());
  }

  public void testMultipleHoplogCardinality() throws Exception {
    int bucketId = (int) System.nanoTime();
    HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    List<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent("key-0", "value-0"));
    items.add(new TestEvent("key-1", "value-1"));
    items.add(new TestEvent("key-2", "value-2"));
    items.add(new TestEvent("key-3", "value-3"));
    items.add(new TestEvent("key-4", "value-4"));

    organizer.flush(items.iterator(), 5);
    assertEquals(5, organizer.sizeEstimate());

    items.clear();
    items.add(new TestEvent("key-1", "value-0"));
    items.add(new TestEvent("key-5", "value-5"));
    items.add(new TestEvent("key-6", "value-6"));
    items.add(new TestEvent("key-7", "value-7"));
    items.add(new TestEvent("key-8", "value-8"));
    items.add(new TestEvent("key-9", "value-9"));

    organizer.flush(items.iterator(), 6);
    assertEquals(10, organizer.sizeEstimate());
  }

  public void testCardinalityAfterRestart() throws Exception {
    int bucketId = (int) System.nanoTime();
    HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    List<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent("key-0", "value-0"));
    items.add(new TestEvent("key-1", "value-1"));
    items.add(new TestEvent("key-2", "value-2"));
    items.add(new TestEvent("key-3", "value-3"));
    items.add(new TestEvent("key-4", "value-4"));

    assertEquals(0, organizer.sizeEstimate());
    organizer.flush(items.iterator(), 5);
    assertEquals(5, organizer.sizeEstimate());

    // restart
    organizer.close();
    organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);
    assertEquals(5, organizer.sizeEstimate());
    
    items.clear();
    items.add(new TestEvent("key-1", "value-0"));
    items.add(new TestEvent("key-5", "value-5"));
    items.add(new TestEvent("key-6", "value-6"));
    items.add(new TestEvent("key-7", "value-7"));
    items.add(new TestEvent("key-8", "value-8"));
    items.add(new TestEvent("key-9", "value-9"));

    organizer.flush(items.iterator(), 6);
    assertEquals(10, organizer.sizeEstimate());

    // restart - make sure that HLL from the youngest file is read
    organizer.close();
    organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);
    assertEquals(10, organizer.sizeEstimate());
    
    items.clear();
    items.add(new TestEvent("key-1", "value-1"));
    items.add(new TestEvent("key-5", "value-5"));
    items.add(new TestEvent("key-10", "value-10"));
    items.add(new TestEvent("key-11", "value-11"));
    items.add(new TestEvent("key-12", "value-12"));
    items.add(new TestEvent("key-13", "value-13"));
    items.add(new TestEvent("key-14", "value-14"));

    organizer.flush(items.iterator(), 7);
    assertEquals(15, organizer.sizeEstimate());
  }

  public void testCardinalityAfterMajorCompaction() throws Exception {
    doCardinalityAfterCompactionWork(true);
  }

  public void testCardinalityAfterMinorCompaction() throws Exception {
    doCardinalityAfterCompactionWork(false);
  }

  private void doCardinalityAfterCompactionWork(boolean isMajor) throws Exception {
    int bucketId = (int) System.nanoTime();
    HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    List<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent("key-0", "value-0"));
    items.add(new TestEvent("key-1", "value-1"));
    items.add(new TestEvent("key-2", "value-2"));
    items.add(new TestEvent("key-3", "value-3"));
    items.add(new TestEvent("key-4", "value-4"));

    organizer.flush(items.iterator(), 5);
    assertEquals(5, organizer.sizeEstimate());

    items.clear();
    items.add(new TestEvent("key-0", "value-0"));
    items.add(new TestEvent("key-1", "value-5", Operation.DESTROY));
    items.add(new TestEvent("key-2", "value-6", Operation.INVALIDATE));
    items.add(new TestEvent("key-5", "value-5"));

    organizer.flush(items.iterator(), 4);
    assertEquals(6, organizer.sizeEstimate());

    items.clear();
    items.add(new TestEvent("key-3", "value-5", Operation.DESTROY));
    items.add(new TestEvent("key-4", "value-6", Operation.INVALIDATE));
    items.add(new TestEvent("key-5", "value-0"));
    items.add(new TestEvent("key-6", "value-5"));

    organizer.flush(items.iterator(), 4);
    
    items.add(new TestEvent("key-5", "value-0"));
    items.add(new TestEvent("key-6", "value-5"));
    
    items.clear();
    organizer.flush(items.iterator(), items.size());
    assertEquals(7, organizer.sizeEstimate());

    organizer.getCompactor().compact(isMajor, false);
    assertEquals(3, organizer.sizeEstimate());
  }
}
