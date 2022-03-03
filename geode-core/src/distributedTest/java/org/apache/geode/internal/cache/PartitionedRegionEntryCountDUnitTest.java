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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * This class tests total entry count of partitioned regions.
 *
 *
 */

public class PartitionedRegionEntryCountDUnitTest extends JUnit4CacheTestCase {
  private static final long serialVersionUID = 19808034671087558L;

  @Test
  public void testTotalEntryCountAfterLocalDestroyEviction() {
    final Host host = Host.getHost(0);
    final VM vm1 = host.getVM(0);
    final VM vm2 = host.getVM(1);
    final VM vm3 = host.getVM(2);
    final int redundantCopies = 1;
    final int maxEntriesForVm1 = 100;
    final int maxEntriesForOtherVm = 2000;
    final String name = "PR_TEMP";

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region having max entries "
            + maxEntriesForVm1) {
      @Override
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntriesForVm1,
            EvictionAction.LOCAL_DESTROY));
        final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm1.invoke(create);

    final SerializableRunnable create2 = new SerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region having max entries "
            + maxEntriesForOtherVm) {
      @Override
      public void run() {
        try {
          final AttributesFactory factory = new AttributesFactory();
          factory.setPartitionAttributes(
              new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
          factory.setEvictionAttributes(EvictionAttributes
              .createLRUEntryAttributes(maxEntriesForOtherVm, EvictionAction.LOCAL_DESTROY));

          final PartitionedRegion pr = (PartitionedRegion) createRootRegion(name, factory.create());
          assertNotNull(pr);
        } catch (final CacheException ex) {
          Assert.fail("While creating Partitioned region", ex);
        }
      }
    };
    vm2.invoke(create2);
    vm3.invoke(create2);

    final SerializableRunnable putData = new SerializableRunnable("Puts Data") {
      @Override
      public void run() {
        final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (int counter = 1; counter <= 6 * maxEntriesForVm1; counter++) {
          pr.put(counter, new byte[1]);
        }
      }
    };
    vm1.invoke(putData);

    final SerializableCallable getTotalEntryCount =
        new SerializableCallable("Get total entry count") {
          @Override
          public Object call() throws Exception {
            try {
              final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);

              assertNotNull(pr);
              return pr.entryCount(false);
            } finally {
            }
          }
        };

    Integer v1T = (Integer) vm1.invoke(getTotalEntryCount);
    Integer v2T = (Integer) vm2.invoke(getTotalEntryCount);
    Integer v3T = (Integer) vm3.invoke(getTotalEntryCount);
    assertEquals(v1T, v2T);
    assertEquals(v1T, v3T);
    assertEquals(v2T, v3T);

    final SerializableCallable getLocalEntryCount =
        new SerializableCallable("Get local entry count") {
          @Override
          public Object call() throws Exception {
            try {
              final PartitionedRegion pr = (PartitionedRegion) getRootRegion(name);

              assertNotNull(pr);
              return pr.entryCount(pr.getDataStore().getAllLocalPrimaryBucketIds());
            } finally {
            }
          }
        };

    Integer v1L = (Integer) vm1.invoke(getLocalEntryCount);
    Integer v2L = (Integer) vm2.invoke(getLocalEntryCount);
    Integer v3L = (Integer) vm3.invoke(getLocalEntryCount);
    Integer total = v1L + v2L + v3L;

    assertEquals(v1T, total);
  }
}
