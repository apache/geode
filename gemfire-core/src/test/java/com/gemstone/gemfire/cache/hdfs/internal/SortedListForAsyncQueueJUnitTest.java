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
package com.gemstone.gemfire.cache.hdfs.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.internal.ParallelAsyncEventQueueImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue.KeyToSeqNumObject;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue.MultiRegionSortedQueue;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue.SortedEventQueue;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

import junit.framework.TestCase;

/**
 * A test class for testing whether the functionalities of sorted Aysync Queue.
 * 
 * @author Hemant Bhanawat
 */
@Category({IntegrationTest.class})
public class SortedListForAsyncQueueJUnitTest extends TestCase {
  
  public SortedListForAsyncQueueJUnitTest() {
    super();
  }

  private GemFireCacheImpl c;

  @Override
  public void setUp() {
    
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");
 // make it a loner
    this.c = createCache();
    AbstractHoplogOrganizer.JUNIT_TEST_RUN = true;
  }

  protected GemFireCacheImpl createCache() {
    return (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").set("log-level", "warning")
        .create();
  }

  @Override
  public void tearDown() {
    this.c.close();
  }
  
  public void testHopQueueWithOneBucket() throws Exception {
    this.c.close();
    this.c = createCache();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(1);
    
    RegionFactory rf1 = this.c.createRegionFactory(RegionShortcut.PARTITION);
    PartitionedRegion r1 = (PartitionedRegion) rf1.setPartitionAttributes(paf.create()).create("r1");
    r1.put("K9", "x1");
    r1.put("K8", "x2");
    // hack to get the queue. 
    HDFSParallelGatewaySenderQueue hopqueue = getHDFSQueue(r1, this.c);
    HDFSBucketRegionQueue hdfsBQ = (HDFSBucketRegionQueue)((PartitionedRegion)hopqueue.getRegion()).getDataStore().getLocalBucketById(0);
    
    EntryEventImpl ev1 = EntryEventImpl.create((LocalRegion)r1, Operation.CREATE,
        (Object)"K1", (Object)"V1", null,
        false, (DistributedMember)c.getMyId());
    // put some keys with multiple updates.
    hopqueue.put(getNewEvent("K2", "V2", r1, 0, 2) );
    hopqueue.put(getNewEvent("K3", "V3a", r1, 0, 8) );
    hopqueue.put(getNewEvent("K3", "V3", r1, 0, 7) );
    hopqueue.put(getNewEvent("K1", "V1", r1, 0, 3) );
    hopqueue.put(getNewEvent("K2", "V2a", r1, 0, 6) );
    hopqueue.put(getNewEvent("K3", "V3b", r1, 0, 9) );
    
    assertTrue(" skip list size should be  6 ", getSortedEventQueue(hdfsBQ).currentSkipList.size() == 6);
    
    
    // peek a key. it should be the lowesy
    Object[] l = hopqueue.peek(1, 0).toArray();
    
    assertTrue("First key should be K1 but is " + ((HDFSGatewayEventImpl)l[0]).getKey(), ((HDFSGatewayEventImpl)l[0]).getKey().equals("K1"));
    assertTrue(" Peeked skip list size should be  0 ", getSortedEventQueue(hdfsBQ).getPeeked().size() == 6);
    assertTrue(" skip list size should be  6 ", getSortedEventQueue(hdfsBQ).currentSkipList.size() == 0);
    
    // try to fetch the key. it would be in peeked skip list but still available
    Object o = hopqueue.get(r1, CacheServerHelper.serialize("K1"), 0);
    assertTrue("First key should be K1", ((HDFSGatewayEventImpl)o).getKey().equals("K1"));
    
    assertTrue(" skip lists size should be  6"  , ( getSortedEventQueue(hdfsBQ).getPeeked().size() + getSortedEventQueue(hdfsBQ).currentSkipList.size() ) == 6);
    
    o = hopqueue.get(r1, CacheServerHelper.serialize("K2"), 0);
    Object v = ((HDFSGatewayEventImpl)o).getDeserializedValue();
    assertTrue(" key should K2 with value V2a but the value was " + v , ((String)v).equals("V2a"));
    
    o = hopqueue.get(r1, CacheServerHelper.serialize("K3"), 0);
    v = ((HDFSGatewayEventImpl)o).getDeserializedValue();
    assertTrue(" key should K3 with value V3b but the value was " + v , ((String)v).equals("V3b"));
  }

  protected SortedEventQueue getSortedEventQueue(HDFSBucketRegionQueue hdfsBQ) {
    MultiRegionSortedQueue multiQueue = (MultiRegionSortedQueue)(hdfsBQ.hdfsEventQueue);
    return multiQueue.regionToEventQueue.values().iterator().next();
  }
  
  public void testPeekABatch() throws Exception {
    this.c.close();
    this.c = createCache();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(1);
    
    RegionFactory rf1 = this.c.createRegionFactory(RegionShortcut.PARTITION);
    PartitionedRegion r1 = (PartitionedRegion) rf1.setPartitionAttributes(paf.create()).create("r1");
    r1.put("K9", "x1");
    r1.put("K8", "x2");
    // hack to get the queue. 
    HDFSParallelGatewaySenderQueue hopqueue = getHDFSQueue(r1, this.c);
    HDFSBucketRegionQueue hdfsBQ = (HDFSBucketRegionQueue)((PartitionedRegion)hopqueue.getRegion()).getDataStore().getLocalBucketById(0);
    
    
    // put some keys with multiple updates.
    hopqueue.put(getNewEvent("K2", "V2", r1, 0, 2) );
    hopqueue.put(getNewEvent("K3", "V3a", r1, 0, 8) );
    hopqueue.put(getNewEvent("K3", "V3", r1, 0, 7) );
    hopqueue.put(getNewEvent("K1", "V1", r1, 0, 3) );
    hopqueue.put(getNewEvent("K2", "V2a", r1, 0, 6) );
    hopqueue.put(getNewEvent("K3", "V3b", r1, 0, 9) );
    
    getSortedEventQueue(hdfsBQ).rollover(true);
    
    hopqueue.put(getNewEvent("K1", "V12", r1, 0, 11) );
    hopqueue.put(getNewEvent("K5", "V3a", r1, 0, 12) );
    hopqueue.put(getNewEvent("K5", "V3b", r1, 0, 13) );
    
    assertTrue(" skip list size should be  3 but is " + getSortedEventQueue(hdfsBQ).currentSkipList.size(), getSortedEventQueue(hdfsBQ).currentSkipList.size() == 3);
    assertTrue(" skip list size should be  6 but is " + getSortedEventQueue(hdfsBQ).queueOfLists.peek().size(), getSortedEventQueue(hdfsBQ).queueOfLists.peek().size() == 6);
    
    Object o1 = hopqueue.get(r1, CacheServerHelper.serialize("K3"), 0);;
    Object o2 = hopqueue.get(r1, CacheServerHelper.serialize("K1"), 0);;
    Object v1 = ((HDFSGatewayEventImpl)o1).getDeserializedValue();
    Object v2 = ((HDFSGatewayEventImpl)o2).getDeserializedValue();
    assertTrue(" key should K3 with value V3b but the value was " + v1 , ((String)v1).equals("V3b"));
    assertTrue(" key should K1 with value V12 but the value was " + v2 , ((String)v2).equals("V12"));
    
    
    ArrayList a = hdfsBQ.peekABatch();
    assertTrue("First key should be K1 but is " + ((HDFSGatewayEventImpl)a.get(0)).getKey(), ((HDFSGatewayEventImpl)a.get(0)).getKey().equals("K1"));
    assertTrue("Second key should be K2 but is " + ((HDFSGatewayEventImpl)a.get(1)).getKey(), ((HDFSGatewayEventImpl)a.get(1)).getKey().equals("K2"));
    assertTrue("Third key should be K2 but is " + ((HDFSGatewayEventImpl)a.get(2)).getKey(), ((HDFSGatewayEventImpl)a.get(2)).getKey().equals("K2"));
    
    
    assertTrue(" Peeked skip list size should be 6 ", getSortedEventQueue(hdfsBQ).getPeeked().size() == 6);
    assertTrue(" queueOfLists size should be  2 ", getSortedEventQueue(hdfsBQ).queueOfLists.size() == 2);
    
    assertTrue(" skip list size should be  3 ", getSortedEventQueue(hdfsBQ).currentSkipList.size() == 3);
    
    o1 = hopqueue.get(r1, CacheServerHelper.serialize("K3"), 0);;
    o2 = hopqueue.get(r1, CacheServerHelper.serialize("K1"), 0);;
    v1 = ((HDFSGatewayEventImpl)o1).getDeserializedValue();
    v2 = ((HDFSGatewayEventImpl)o2).getDeserializedValue();
    assertTrue(" key should K3 with value V3b but the value was " + v1 , ((String)v1).equals("V3b"));
    assertTrue(" key should K1 with value V12 but the value was " + v2 , ((String)v2).equals("V12"));
    
    
    java.util.Iterator<KeyToSeqNumObject> iter1 = getSortedEventQueue(hdfsBQ).getPeeked().iterator();
    assertTrue("key in peeked list should be 3 ", iter1.next().getSeqNum() == 3);
    assertTrue("key in peeked list should be 6 ", iter1.next().getSeqNum() == 6);
    assertTrue("key in peeked list should be 2 ", iter1.next().getSeqNum() == 2);
    assertTrue("key in peeked list should be 9 ", iter1.next().getSeqNum() == 9);
    assertTrue("key in peeked list should be 8 ", iter1.next().getSeqNum() == 8);
    assertTrue("key in peeked list should be 7 ", iter1.next().getSeqNum() == 7);
    assertTrue(" Peeked list should not have any more elements. ", iter1.hasNext() == false);
    
    
    java.util.Iterator<KeyToSeqNumObject> iter2 = getSortedEventQueue(hdfsBQ).currentSkipList.iterator();
    assertTrue("key in peeked list should be 11", iter2.next().getSeqNum() == 11);
    assertTrue("key in peeked list should be 13", iter2.next().getSeqNum() == 13);
    assertTrue("key in peeked list should be 12 ", iter2.next().getSeqNum() == 12);
    
    iter2 = getSortedEventQueue(hdfsBQ).currentSkipList.iterator();
    HashSet<Long> hs = new HashSet<Long>();
    hs.add((long) 11);
    hs.add((long) 13);
    hs.add((long) 12);
    hs.add((long) 3);
    hs.add((long) 6);
    hs.add((long) 2);
    hs.add((long) 9);
    hs.add((long) 8);
    hs.add((long) 7);
    
    hdfsBQ.hdfsEventQueue.handleRemainingElements(hs);
    
    ArrayList a1 = hdfsBQ.peekABatch();
    o1 = hopqueue.get(r1, CacheServerHelper.serialize("K3"), 0);;
    o2 = hopqueue.get(r1, CacheServerHelper.serialize("K1"), 0);;
    v2 = ((HDFSGatewayEventImpl)o2).getDeserializedValue();
    assertTrue(" key should K3 should not have been found ",  o1 ==null);
    assertTrue(" key should K1 with value V12 but the value was " + v2 , ((String)v2).equals("V12"));
    
    assertTrue("First key should be K1 but is " + ((HDFSGatewayEventImpl)a1.get(0)).getKey(), ((HDFSGatewayEventImpl)a1.get(0)).getKey().equals("K1"));
    assertTrue("Second key should be K5 but is " + ((HDFSGatewayEventImpl)a1.get(1)).getKey(), ((HDFSGatewayEventImpl)a1.get(1)).getKey().equals("K5"));
    assertTrue("Third key should be K5 but is " + ((HDFSGatewayEventImpl)a1.get(2)).getKey(), ((HDFSGatewayEventImpl)a1.get(2)).getKey().equals("K5"));
    
    assertTrue(" Peeked skip list size should be  3 ", getSortedEventQueue(hdfsBQ).getPeeked().size() == 3);
    assertTrue(" skip list size should be  0 but is " + getSortedEventQueue(hdfsBQ).currentSkipList.size(), getSortedEventQueue(hdfsBQ).currentSkipList.size() == 0);
    assertTrue(" skip list size should be  3 but is " + getSortedEventQueue(hdfsBQ).queueOfLists.peek().size(), getSortedEventQueue(hdfsBQ).queueOfLists.peek().size() == 3);
    assertTrue(" skip list size should be  2 but is " + getSortedEventQueue(hdfsBQ).queueOfLists.size(), getSortedEventQueue(hdfsBQ).queueOfLists.size() == 2);
    
  }
  
  private HDFSGatewayEventImpl getNewEvent(Object key, Object value, Region r1, int bid, int tailKey) throws Exception {
    EntryEventImpl ev1 = EntryEventImpl.create((LocalRegion)r1, Operation.CREATE,
        key, value, null,
        false, (DistributedMember)c.getMyId());
    ev1.setEventId(new EventID(this.c.getDistributedSystem()));
    HDFSGatewayEventImpl event = null;
    event = new HDFSGatewayEventImpl(EnumListenerEvent.AFTER_CREATE, ev1, null , true, bid);
    event.setShadowKey((long)tailKey);
    return event;
  }
  
  /**
   * Creates the HDFS Queue instance for a region (this skips the creation of 
   * event processor)
   */
  private HDFSParallelGatewaySenderQueue getHDFSQueue(Region region, Cache c) {
    GatewaySenderAttributes gattrs = new GatewaySenderAttributes();
    gattrs.isHDFSQueue = true;
    gattrs.id = "SortedListForAsyncQueueJUnitTest_test";
    ParallelAsyncEventQueueImpl gatewaySender = new ParallelAsyncEventQueueImpl(c, gattrs);
    HashSet<Region> set = new HashSet<Region>();
    set.add(region);
    return new HDFSParallelGatewaySenderQueue(gatewaySender, set, 0, 1);
  }
  
 // A test for testing whether the KeyToSeqNumObject compare function is in order.
  public void testIfTheKeyToSeqNumIsKeptSortedWithoutConflation() throws Exception {
    byte[] k1 = new byte[] { 1};
    byte[] k2 = new byte[] { 2};
    byte[] k3 = new byte[] { 3};
    byte[] k4 = new byte[] { 4};
    
    KeyToSeqNumObject keyToSeq1 = new KeyToSeqNumObject(k1, new Long(2));
    KeyToSeqNumObject keyToSeq2 = new KeyToSeqNumObject(k1, new Long(5));
    KeyToSeqNumObject keyToSeq3 = new KeyToSeqNumObject(k1, new Long(8));
    KeyToSeqNumObject keyToSeq4 = new KeyToSeqNumObject(k2, new Long(3));
    KeyToSeqNumObject keyToSeq5 = new KeyToSeqNumObject(k2, new Long(7));
    
    ConcurrentSkipListSet<KeyToSeqNumObject> list = new ConcurrentSkipListSet<HDFSBucketRegionQueue.KeyToSeqNumObject>();
    list.add(keyToSeq4);
    list.add(keyToSeq3);
    list.add(keyToSeq5);
    list.add(keyToSeq1);
    list.add(keyToSeq2);
    list.add(keyToSeq5);
    KeyToSeqNumObject k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq3));
    list.remove(k);
    
    k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq2));
    list.remove(k);
    
    k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq1));
    list.remove(k);
    
    list.add(keyToSeq4);
    list.add(keyToSeq3);
    list.add(keyToSeq5);
    list.add(keyToSeq1);
    k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq3));
    list.remove(k);
    
    k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq1));
    list.remove(k);
    
    k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq5));
    list.remove(k);
    
    k = list.pollFirst();
    this.c.getLoggerI18n().fine(" KeyToSeqNumObject  byte: " + k.getRegionkey()[0] + " seq num: " + k.getSeqNum());
    assertTrue ("Order of elements in Concurrent list is not correct ", k.equals(keyToSeq4));
    
    list.remove(k);
  }
  
  public void testSingleGet() throws Exception {
    checkQueueGet("K1", new KeyValue("K1", "V1"), "K1-V1");
  }
  
  public void testMissingGet() throws Exception {
    checkQueueGet("K1", null, 
        "K0-V0",
        "K2-V2");
  }

  public void testMultipleGet() throws Exception {
    checkQueueGet("K1", new KeyValue("K1", "V1"), 
        "K0-V0",
        "K1-V1",
        "K2-V2");
  }

  public void testDuplicateGet() throws Exception {
    checkQueueGet("K1", new KeyValue("K1", "V1.4"), 
        "K0-V0",
        "K1-V1.0",
        "K1-V1.1",
        "K1-V1.2",
        "K1-V1.3",
        "K1-V1.4",
        "K2-V2");
  }

  public void testEmptyIterator() throws Exception {
    checkQueueIteration(Collections.<KeyValue>emptyList());
  }
  
  public void testSingleIterator() throws Exception {
    checkQueueIteration(getExpected(), 
        "K0-V0",
        "K1-V1",
        "K2-V2",
        "K3-V3",
        "K4-V4",
        "K5-V5",
        "K6-V6",
        "K7-V7",
        "K8-V8",
        "K9-V9"
        );
  }

  public void testMultipleIterator() throws Exception {
    checkQueueIteration(getExpected(), 
        "K0-V0",
        "K1-V1",
        "K2-V2",
        "roll",
        "K3-V3",
        "K4-V4",
        "K5-V5",
        "K6-V6",
        "roll",
        "K7-V7",
        "K8-V8",
        "K9-V9"
        );
  }

  public void testMixedUpIterator() throws Exception {
    checkQueueIteration(getExpected(), 
        "K0-V0",
        "K5-V5",
        "K9-V9",
        "roll",
        "K3-V3",
        "K2-V2",
        "K6-V6",
        "roll",
        "K4-V4",
        "K7-V7",
        "K8-V8",
        "K1-V1"
        );
  }

  public void testMixedUpIterator2() throws Exception {
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(new KeyValue("K0", "V0"));
    expected.add(new KeyValue("K1", "V1.2"));
    expected.add(new KeyValue("K2", "V2.1"));
    expected.add(new KeyValue("K3", "V3.1"));
    expected.add(new KeyValue("K4", "V4.2"));
    expected.add(new KeyValue("K5", "V5.2"));
    expected.add(new KeyValue("K6", "V6"));
    expected.add(new KeyValue("K7", "V7"));
    expected.add(new KeyValue("K8", "V8"));
    expected.add(new KeyValue("K9", "V9"));
    
    checkQueueIteration(expected, 
        "K1-V1.0",
        "K2-V2.0",
        "K3-V3.0",
        "K4-V4.0",
        "roll",
        "K2-V2.1",
        "K4-V4.1",
        "K6-V6",
        "K8-V8",
        "roll",
        "K1-V1.1",
        "K3-V3.1",
        "K5-V5.0",
        "K7-V7",
        "K9-V9",
        "roll",
        "K0-V0",
        "K1-V1.2",
        "K4-V4.2",
        "K5-V5.1",
        "K5-V5.2"
        );
  }

  private List<KeyValue> getExpected() {
    List<KeyValue> expected = new ArrayList<KeyValue>();
    expected.add(new KeyValue("K0", "V0"));
    expected.add(new KeyValue("K1", "V1"));
    expected.add(new KeyValue("K2", "V2"));
    expected.add(new KeyValue("K3", "V3"));
    expected.add(new KeyValue("K4", "V4"));
    expected.add(new KeyValue("K5", "V5"));
    expected.add(new KeyValue("K6", "V6"));
    expected.add(new KeyValue("K7", "V7"));
    expected.add(new KeyValue("K8", "V8"));
    expected.add(new KeyValue("K9", "V9"));
    
    return expected;
  }
  
  private void checkQueueGet(String key, KeyValue expected, String... entries) throws Exception {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(1);
    
    RegionFactory rf1 = this.c.createRegionFactory(RegionShortcut.PARTITION);
    PartitionedRegion r1 = (PartitionedRegion) rf1.setPartitionAttributes(paf.create()).create("r1");

    // create the buckets
    r1.put("blah", "blah");

    // hack to get the queue. 
    HDFSParallelGatewaySenderQueue hopqueue = getHDFSQueue(r1, this.c);
    HDFSBucketRegionQueue brq = (HDFSBucketRegionQueue)((PartitionedRegion)hopqueue.getRegion()).getDataStore().getLocalBucketById(0);

    
    int seq = 0;
    for (String s : entries) {
      if (s.equals("roll")) {
        brq.rolloverSkipList();
      } else {
        String[] kv = s.split("-");
        hopqueue.put(getNewEvent(kv[0], kv[1], r1, 0, seq++));
      }
    }

    byte[] bkey = EntryEventImpl.serialize(key);
    HDFSGatewayEventImpl evt = hopqueue.get(r1, bkey, 0);
    if (expected == null) {
      assertNull(evt);
      
    } else {
      assertEquals(expected.key, evt.getKey());
      assertEquals(expected.value, evt.getDeserializedValue());
    }
  }
  
  private void checkQueueIteration(List<KeyValue> expected, String... entries) throws Exception {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(1);
    
    RegionFactory rf1 = this.c.createRegionFactory(RegionShortcut.PARTITION);
    Region r1 = rf1.setPartitionAttributes(paf.create()).create("r1");

    // create the buckets
    r1.put("blah", "blah");

    HDFSParallelGatewaySenderQueue hopqueue = getHDFSQueue(r1, this.c);
    HDFSBucketRegionQueue brq = (HDFSBucketRegionQueue)((PartitionedRegion)hopqueue.getRegion()).getDataStore().getLocalBucketById(0);
    
    int seq = 0;
    for (String s : entries) {
      if (s.equals("roll")) {
        brq.rolloverSkipList();
      } else {
        String[] kv = s.split("-");
        hopqueue.put(getNewEvent(kv[0], kv[1], r1, 0, seq++));
        getSortedEventQueue(brq).rollover(true);
      }
    }
    
    Iterator<HDFSGatewayEventImpl> iter = brq.iterator(r1);
    List<KeyValue> actual = new ArrayList<KeyValue>();
    while (iter.hasNext()) {
      HDFSGatewayEventImpl evt = iter.next();
      actual.add(new KeyValue((String) evt.getKey(), (String) evt.getDeserializedValue()));
    }
    
    assertEquals(expected, actual);
  }
  
  public static class KeyValue {
    public final String key;
    public final String value;
    
    public KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o == null)
        return false;

      KeyValue obj = (KeyValue) o;
      return key.equals(obj.key) && value.equals(obj.value);
    }
    
    @Override
    public String toString() {
      return key + "=" + value;
    }
  }
}
