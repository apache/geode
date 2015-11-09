/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollectorManager.ListScanner;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TopEntriesCollectorJUnitTest {
  EntryScore r1_1 = new EntryScore("1-1", .9f);
  EntryScore r1_2 = new EntryScore("1-2", .7f);
  EntryScore r1_3 = new EntryScore("1-3", .5f);

  EntryScore r2_1 = new EntryScore("2-1", .85f);
  EntryScore r2_2 = new EntryScore("2-2", .65f);

  EntryScore r3_1 = new EntryScore("3-1", .8f);
  EntryScore r3_2 = new EntryScore("3-2", .6f);
  EntryScore r3_3 = new EntryScore("3-3", .4f);

  TopEntriesCollectorManager manager;

  @Before
  public void setup() {
    manager = new TopEntriesCollectorManager();
  }

  @Test
  public void testReduce() throws Exception {
    TopEntriesCollector c1 = manager.newCollector("c1");
    c1.collect(r1_1.getKey(), r1_1.getScore());
    c1.collect(r1_2.getKey(), r1_2.getScore());
    c1.collect(r1_3.getKey(), r1_3.getScore());

    TopEntriesCollector c2 = manager.newCollector("c2");
    c2.collect(r2_1.getKey(), r2_1.getScore());
    c2.collect(r2_2.getKey(), r2_2.getScore());

    TopEntriesCollector c3 = manager.newCollector("c3");
    c3.collect(r3_1.getKey(), r3_1.getScore());
    c3.collect(r3_2.getKey(), r3_2.getScore());
    c3.collect(r3_3.getKey(), r3_3.getScore());

    List<TopEntriesCollector> collectors = new ArrayList<>();
    collectors.add(c1);
    collectors.add(c2);
    collectors.add(c3);

    TopEntriesCollector hits = manager.reduce(collectors);
    assertEquals(8, hits.getEntries().getHits().size());
    TopEntriesJUnitTest.verifyResultOrder(hits.getEntries().getHits(), r1_1, r2_1, r3_1, r1_2, r2_2, r3_2, r1_3, r3_3);

    // input collector should not change
    assertEquals(3, c1.getEntries().getHits().size());
    assertEquals(2, c2.getEntries().getHits().size());
    assertEquals(3, c3.getEntries().getHits().size());
    // TopEntriesJUnitTest.verifyResultOrder(c1.getEntries().getHits(), r1_1, r2_1, r3_1, r1_2, r2_2, r3_2, r1_3, r3_3);
  }

  @Test
  public void testInitialization() {
    TopEntriesCollector collector = new TopEntriesCollector("name");
    assertEquals("name", collector.getName());
    assertEquals(0, collector.size());
  }

  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    TopEntriesCollectorManager manager = new TopEntriesCollectorManager("id", 213);
    TopEntriesCollectorManager copy = CopyHelper.deepCopy(manager);
    assertEquals("id", copy.getId());
    assertEquals(213, copy.getLimit());
  }

  @Test
  public void testCollectorSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    TopEntriesCollector collector = new TopEntriesCollector("collector", 345);
    TopEntriesCollector copy = CopyHelper.deepCopy(collector);
    assertEquals("collector", copy.getName());
    assertEquals(345, copy.getEntries().getLimit());
  }

  @Test
  public void testScannerDoesNotMutateHits() {
    TopEntriesCollector c1 = manager.newCollector("c1");
    c1.collect(r1_1.getKey(), r1_1.getScore());
    c1.collect(r1_2.getKey(), r1_2.getScore());
    c1.collect(r1_3.getKey(), r1_3.getScore());

    assertEquals(3, c1.getEntries().getHits().size());
    TopEntriesJUnitTest.verifyResultOrder(c1.getEntries().getHits(), r1_1, r1_2, r1_3);

    ListScanner scanner = new ListScanner(c1.getEntries().getHits());
    Assert.assertTrue(scanner.hasNext());
    assertEquals(r1_1.getKey(), scanner.peek().getKey());
    assertEquals(r1_1.getKey(), scanner.next().getKey());
    Assert.assertTrue(scanner.hasNext());
    assertEquals(r1_2.getKey(), scanner.peek().getKey());
    assertEquals(r1_2.getKey(), scanner.next().getKey());
    Assert.assertTrue(scanner.hasNext());
    assertEquals(r1_3.getKey(), scanner.peek().getKey());
    assertEquals(r1_3.getKey(), scanner.next().getKey());
    Assert.assertFalse(scanner.hasNext());

    assertEquals(3, c1.getEntries().getHits().size());
    TopEntriesJUnitTest.verifyResultOrder(c1.getEntries().getHits(), r1_1, r1_2, r1_3);
  }
}
