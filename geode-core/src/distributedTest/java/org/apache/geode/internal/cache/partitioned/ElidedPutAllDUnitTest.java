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
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.RegionsTest;


@Category({RegionsTest.class})
public class ElidedPutAllDUnitTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = -184003583877999750L;

  public ElidedPutAllDUnitTest() {
    super();
  }

  /**
   * bug #47425 - elided putAll event causes PutAllPartialResultException
   */
  @Test
  public void testElidedPutAllOnPR() throws Exception {
    final String regionName = getUniqueName() + "Region";
    final String key = "key-1";

    Cache cache = getCache();
    PartitionedRegion region =
        (PartitionedRegion) cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    region.put(key, "value-1");
    region.put(key, "value-2");
    Entry<?, ?> entry = region.getEntry(key);
    assertTrue("expected entry to be in this vm", entry != null);

    VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(new SerializableRunnable("perform conflicting update") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache
            .createRegionFactory(RegionShortcut.PARTITION).create(regionName);
        try {
          Entry<?, ?> entry = region.getEntry(key);
          assertTrue(entry instanceof EntrySnapshot);
          RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

          final VersionTag<?> tag = regionEntry.getVersionStamp().asVersionTag();

          tag.setEntryVersion(tag.getEntryVersion() - 1);
          tag.setRegionVersion(1);

          Map<String, String> map = new HashMap<String, String>();
          map.put(key, "value-3");
          DistributedPutAllOperation dpao = region.newPutAllOperation(map, null);
          EntryEventImpl event = EntryEventImpl.create(region, Operation.PUTALL_CREATE, null, null,
              null, true, (DistributedMember) tag.getMemberID());
          event.setOldValue("value-1");
          event.setVersionTag(tag);
          event.setEventId(new EventID(cache.getDistributedSystem()));
          event.setKeyInfo(region.getKeyInfo(key));
          dpao.addEntry(event, event.getKeyInfo().getBucketId());
          // getLogWriter().info("dpao data = " + dpao.getPutAllEntryData()[0]);
          VersionedObjectList successfulPuts = new VersionedObjectList(1, true, true);
          successfulPuts.addKeyAndVersion(key, tag);
          try {
            region.postPutAllSend(dpao, successfulPuts);
          } catch (ConcurrentCacheModificationException e) {
            Assert.fail("Should not have received an exception for an elided operation", e);
          } finally {
            event.release();
            dpao.getBaseEvent().release();
            dpao.freeOffHeapResources();
          }
        } catch (Exception e) {
          Assert.fail("caught unexpected exception", e);
        }
      }
    });

    entry = region.getEntry(key);
    assertTrue("expected value-2: " + entry.getValue(), entry.getValue().equals("value-2"));

    RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();
    final VersionTag<?> tag = regionEntry.getVersionStamp().asVersionTag();
    assertTrue(tag.getEntryVersion() == 2);
  }


}
