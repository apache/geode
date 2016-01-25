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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;


/**
 * @author bruce
 *
 */
public class ElidedPutAllDUnitTest extends CacheTestCase {
  
  private static final long serialVersionUID = -184003583877999750L;

  public ElidedPutAllDUnitTest(String name) {
    super(name);
  }
  
  /**
   * bug #47425 - elided putAll event causes PutAllPartialResultException
   */
  public void testElidedPutAllOnPR() throws Exception {
    final String regionName = getUniqueName() + "Region";
    final String key = "key-1";
    
    Cache cache = getCache();
    PartitionedRegion region = (PartitionedRegion)cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    region.put(key, "value-1");
    region.put(key, "value-2");
    Entry<?,?> entry = region.getEntry(key);
    assertTrue("expected entry to be in this vm", entry != null);
    
    VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(new SerializableRunnable("perform conflicting update") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion)cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
        try {
          Entry<?,?> entry = region.getEntry(key);
          assertTrue(entry instanceof EntrySnapshot);
          RegionEntry regionEntry = ((EntrySnapshot)entry).getRegionEntry();

          final VersionTag<?> tag = regionEntry.getVersionStamp().asVersionTag();

          tag.setEntryVersion(tag.getEntryVersion()-1);
          tag.setRegionVersion(1);

          Map<String, String> map = new HashMap<String, String>();
          map.put(key, "value-3");
          DistributedPutAllOperation dpao = region.newPutAllOperation(map, null);
          EntryEventImpl event = EntryEventImpl.create(region,
              Operation.PUTALL_CREATE, null, null, null, true, (DistributedMember)tag.getMemberID());
          event.setOldValue("value-1");
          event.setVersionTag(tag);
          event.setEventId(new EventID(cache.getDistributedSystem()));
          event.setKeyInfo(((PartitionedRegion)region).getKeyInfo(key));
          dpao.addEntry(event, event.getKeyInfo().getBucketId());
          //            getLogWriter().info("dpao data = " + dpao.getPutAllEntryData()[0]);
          VersionedObjectList successfulPuts = new VersionedObjectList(1, true, true);
          successfulPuts.addKeyAndVersion(key, tag);
          try {
            region.postPutAllSend(dpao, successfulPuts);
          } catch (ConcurrentCacheModificationException e) {
            fail("Should not have received an exception for an elided operation", e);
          } finally {
            event.release();
            dpao.getBaseEvent().release();
            dpao.freeOffHeapResources();
          }
        } catch (Exception e) {
          fail("caught unexpected exception", e);
        }
      }
    });

    entry = region.getEntry(key);
    assertTrue("expected value-2: " + entry.getValue(), entry.getValue().equals("value-2"));

    RegionEntry regionEntry = ((EntrySnapshot)entry).getRegionEntry();
    final VersionTag<?> tag = regionEntry.getVersionStamp().asVersionTag();
    assertTrue(tag.getEntryVersion() == 2);
  }


}
