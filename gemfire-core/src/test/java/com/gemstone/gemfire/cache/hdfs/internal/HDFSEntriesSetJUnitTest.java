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
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.internal.ParallelAsyncEventQueueImpl;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.SortedListForAsyncQueueJUnitTest.KeyValue;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion.IteratorType;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@SuppressWarnings("rawtypes")
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSEntriesSetJUnitTest extends TestCase {
  private GemFireCacheImpl cache;
  private HDFSStoreImpl store;
  private PartitionedRegion region;
  private BucketRegion bucket;
  private HDFSParallelGatewaySenderQueue queue;
  
  private HDFSBucketRegionQueue brq;
  private HoplogOrganizer hdfs;
  
  public void setUp() throws Exception {
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");
    cache = (GemFireCacheImpl) new CacheFactory()
        .set("mcast-port", "0")
        .set("log-level", "info")
        .create();
    
    HDFSStoreFactory hsf = this.cache.createHDFSStoreFactory();
    hsf.setHomeDir("hoplogs");
    store = (HDFSStoreImpl) hsf.create("test");

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(1);
    
    RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION_HDFS);
    region = (PartitionedRegion) rf.setHDFSStoreName("test").setPartitionAttributes(paf.create()).create("test");
    
    // prime the region so buckets get created
    region.put("test", "test");
    GatewaySenderAttributes g = new GatewaySenderAttributes();
    g.isHDFSQueue = true;
    g.id = "HDFSEntriesSetJUnitTest_Queue";
    ParallelAsyncEventQueueImpl gatewaySender = new ParallelAsyncEventQueueImpl(cache, g);
    Set<Region> set = new HashSet<Region>();
    set.add(region);
    
    queue = new HDFSParallelGatewaySenderQueue(gatewaySender, set, 0, 1);
    brq = (HDFSBucketRegionQueue)((PartitionedRegion) queue.getRegion()).getDataStore().getLocalBucketById(0);
    bucket = region.getDataStore().getLocalBucketById(0);
        
    HdfsRegionManager mgr = HDFSRegionDirector.getInstance().manageRegion(region, "test", null);
    hdfs =  mgr.<SortedHoplogPersistedEvent>create(0);
    AbstractHoplogOrganizer.JUNIT_TEST_RUN = true;
  }
  
  public void tearDown() throws Exception {
    store.getFileSystem().delete(new Path("hoplogs"), true);
    hdfs.close();
    
    cache.close();
  }
  
  public void testEmptyIterator() throws Exception {
    checkIteration(Collections.<String>emptyList(), new KeyValue[] { }, new KeyValue[] { });
  }
  
  public void testQueueOnlyIterator() throws Exception {
    KeyValue[] qvals = new KeyValue[] {
      new KeyValue("K0", "0"),
      new KeyValue("K1", "1"),
      new KeyValue("K2", "2"),
      new KeyValue("K3", "3"),
      new KeyValue("K4", "4")
    };
    checkIteration(getExpected(), qvals, new KeyValue[] { });
  }
  
  public void testHdfsOnlyIterator() throws Exception {
    KeyValue[] hvals = new KeyValue[] {
      new KeyValue("K0", "0"),
      new KeyValue("K1", "1"),
      new KeyValue("K2", "2"),
      new KeyValue("K3", "3"),
      new KeyValue("K4", "4")
    };
    checkIteration(getExpected(), new KeyValue[] { }, hvals);
  }
  
  public void testUnevenIterator() throws Exception {
    KeyValue[] qvals = new KeyValue[] {
        new KeyValue("K0", "0"),
        new KeyValue("K2", "2"),
      };

    KeyValue[] hvals = new KeyValue[] {
      new KeyValue("K1", "1"),
      new KeyValue("K3", "3"),
      new KeyValue("K4", "4")
    };
    
    checkIteration(getExpected(), qvals, hvals);
  }

  public void testEitherOrIterator() throws Exception {
    KeyValue[] qvals = new KeyValue[] {
        new KeyValue("K0", "0"),
        new KeyValue("K2", "2"),
        new KeyValue("K4", "4")
      };

    KeyValue[] hvals = new KeyValue[] {
      new KeyValue("K1", "1"),
      new KeyValue("K3", "3")
    };
    
    checkIteration(getExpected(), qvals, hvals);
  }

  public void testDuplicateIterator() throws Exception {
    KeyValue[] qvals = new KeyValue[] {
        new KeyValue("K0", "0"),
        new KeyValue("K1", "1"),
        new KeyValue("K2", "2"),
        new KeyValue("K3", "3"),
        new KeyValue("K4", "4"),
        new KeyValue("K4", "4")
      };

    KeyValue[] hvals = new KeyValue[] {
        new KeyValue("K0", "0"),
        new KeyValue("K1", "1"),
        new KeyValue("K2", "2"),
        new KeyValue("K3", "3"),
        new KeyValue("K4", "4"),
        new KeyValue("K4", "4")
    };
    
    checkIteration(getExpected(), qvals, hvals);
  }

  private List<String> getExpected() {
    List<String> expected = new ArrayList<String>();
    expected.add("0");
    expected.add("1");
    expected.add("2");
    expected.add("3");
    expected.add("4");
    return expected;
  }
  
  private void checkIteration(List<String> expected, KeyValue[] qvals, KeyValue[] hvals) 
  throws Exception {
    int seq = 0;
    List<PersistedEventImpl> evts = new ArrayList<PersistedEventImpl>();
    for (KeyValue kv : hvals) {
      evts.add(new SortedHDFSQueuePersistedEvent(getNewEvent(kv.key, kv.value, seq++)));
    }
    hdfs.flush(evts.iterator(), evts.size());

    for (KeyValue kv : qvals) {
      queue.put(getNewEvent(kv.key, kv.value, seq++));
    }

    List<String> actual = new ArrayList<String>();
    Iterator vals = new HDFSEntriesSet(bucket, brq, hdfs, IteratorType.VALUES, null).iterator();
    while (vals.hasNext()) {
      Object val = vals.next();
      if(val instanceof CachedDeserializable) {
        val = ((CachedDeserializable) val).getDeserializedForReading();
      }
      actual.add((String) val);
    }
    
    assertEquals(expected, actual);
  }
  
  private HDFSGatewayEventImpl getNewEvent(Object key, Object value, long seq) throws Exception {
    EntryEventImpl evt = EntryEventImpl.create(region, Operation.CREATE,
        key, value, null, false, (DistributedMember) cache.getMyId());
    
    evt.setEventId(new EventID(cache.getDistributedSystem()));
    HDFSGatewayEventImpl event = new HDFSGatewayEventImpl(EnumListenerEvent.AFTER_CREATE, evt, null, true, 0);
    event.setShadowKey(seq);
    
    return event;
  }
}
