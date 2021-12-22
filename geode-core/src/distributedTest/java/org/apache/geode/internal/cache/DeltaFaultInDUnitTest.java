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

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class DeltaFaultInDUnitTest extends JUnit4CacheTestCase {


  public DeltaFaultInDUnitTest() {
    super();
  }

  @Test
  public void bucketSizeShould_notGoNegative_onFaultInDeltaObject() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final boolean copyOnRead = false;
    final boolean clone = true;

    SerializableCallable createDataRegion = new SerializableCallable("createDataRegion") {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        cache.createDiskStoreFactory().create("DeltaFaultInDUnitTestData");
        AttributesFactory attr = new AttributesFactory();
        attr.setDiskStoreName("DeltaFaultInDUnitTestData");
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setCloningEnabled(clone);
        attr.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        Region region = cache.createRegion("region1", attr.create());

        return null;
      }
    };

    vm0.invoke(createDataRegion);

    SerializableRunnable createEmptyRegion = new SerializableRunnable("createEmptyRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<>();
        attr.setCloningEnabled(clone);
        PartitionAttributesFactory<Integer, TestDelta> paf =
            new PartitionAttributesFactory<>();
        paf.setRedundantCopies(1);
        paf.setLocalMaxMemory(0);
        PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PARTITION);
        attr.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        Region<Integer, TestDelta> region = cache.createRegion("region1", attr.create());


        // Put an entry
        region.put(new Integer(0), new TestDelta(false, "initial"));

        // Put a delta object that is larger
        region.put(new Integer(0), new TestDelta(true, "initial_plus_some_more_data"));
      }
    };

    vm1.invoke(createEmptyRegion);

    vm0.invoke(new SerializableRunnable("doPut") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region<Integer, TestDelta> region = cache.getRegion("region1");


        // Evict the other object
        region.put(new Integer(113), new TestDelta(false, "bogus"));

        // Something was going weird with the LRU list. It was evicting this object.
        // I want to make sure the other object is the one evicted.
        region.get(new Integer(113));

        long entriesEvicted = ((InternalRegion) region).getTotalEvictions();
        // assertIndexDetailsEquals(1, entriesEvicted);

        TestDelta result = region.get(new Integer(0));
        assertEquals("initial_plus_some_more_data", result.info);
      }
    });
  }

  private long checkObjects(VM vm, final int serializations, final int deserializations,
      final int deltas, final int clones) {
    SerializableCallable getSize = new SerializableCallable("check objects") {
      @Override
      public Object call() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        long size = region.getDataStore().getBucketSize(0);
        TestDelta value = (TestDelta) region.get(Integer.valueOf(0));
        value.checkFields(serializations, deserializations, deltas, clones);
        return Long.valueOf(size);
      }
    };
    Object size = vm.invoke(getSize);
    return (Long) size;
  }
}
