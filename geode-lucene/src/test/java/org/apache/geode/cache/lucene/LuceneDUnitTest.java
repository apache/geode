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
package org.apache.geode.cache.lucene;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.*;


public abstract class LuceneDUnitTest extends JUnit4CacheTestCase {
  protected VM dataStore1;
  protected VM dataStore2;

  protected static int NUM_BUCKETS = 10;
  protected static int MAX_ENTRIES_FOR_EVICTION = 20;

  @Override
  public void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
  }

  protected void initDataStore(SerializableRunnableIF createIndex,
      RegionTestableType regionTestType) throws Exception {
    createIndex.run();
    regionTestType.createDataStore(getCache(), REGION_NAME);
  }

  protected void initAccessor(SerializableRunnableIF createIndex, RegionTestableType regionTestType)
      throws Exception {
    createIndex.run();
    regionTestType.createAccessor(getCache(), REGION_NAME);
  }

  protected RegionTestableType[] getListOfRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION,
        RegionTestableType.PARTITION_REDUNDANT, RegionTestableType.PARTITION_OVERFLOW_TO_DISK,
        RegionTestableType.PARTITION_PERSISTENT, RegionTestableType.FIXED_PARTITION};
  }

  protected final Object[] parameterCombiner(Object[] aValues, Object[] bValues) {
    Object[] parameters = new Object[aValues.length * bValues.length];
    for (int i = 0; i < aValues.length; i++) {
      for (int j = 0; j < bValues.length; j++) {
        parameters[i * bValues.length + j] = new Object[] {aValues[i], bValues[j]};
      }
    }
    return parameters;
  }

  public enum RegionTestableType {
    PARTITION(RegionShortcut.PARTITION_PROXY, RegionShortcut.PARTITION),
    PARTITION_REDUNDANT_PERSISTENT(RegionShortcut.PARTITION_PROXY_REDUNDANT,
        RegionShortcut.PARTITION_REDUNDANT_PERSISTENT),
    PARTITION_PERSISTENT(RegionShortcut.PARTITION_PROXY, RegionShortcut.PARTITION_PERSISTENT),
    PARTITION_REDUNDANT(RegionShortcut.PARTITION_PROXY_REDUNDANT,
        RegionShortcut.PARTITION_REDUNDANT),
    PARTITION_OVERFLOW_TO_DISK(RegionShortcut.PARTITION_PROXY, RegionShortcut.PARTITION_OVERFLOW,
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK)),
    FIXED_PARTITION(RegionShortcut.PARTITION, RegionShortcut.PARTITION),
    PARTITION_WITH_CLIENT(RegionShortcut.PARTITION_PROXY, RegionShortcut.PARTITION),
    PARTITION_PERSISTENT_REDUNDANT_EVICTION_OVERFLOW(RegionShortcut.PARTITION_PROXY_REDUNDANT,
        RegionShortcut.PARTITION_REDUNDANT_PERSISTENT,
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK)),
    PARTITION_REDUNDANT_EVICTION_LOCAL_DESTROY(RegionShortcut.PARTITION_PROXY_REDUNDANT,
        RegionShortcut.PARTITION_REDUNDANT, EvictionAttributes
            .createLRUEntryAttributes(MAX_ENTRIES_FOR_EVICTION, EvictionAction.LOCAL_DESTROY)),
    PARTITION_REDUNDANT_PERSISTENT_EVICTION_LOCAL_DESTROY(RegionShortcut.PARTITION_PROXY_REDUNDANT,
        RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, EvictionAttributes
            .createLRUEntryAttributes(MAX_ENTRIES_FOR_EVICTION, EvictionAction.LOCAL_DESTROY)),
    PARTITION_EVICTION_LOCAL_DESTROY(RegionShortcut.PARTITION_PROXY, RegionShortcut.PARTITION,
        EvictionAttributes.createLRUEntryAttributes(MAX_ENTRIES_FOR_EVICTION,
            EvictionAction.LOCAL_DESTROY)),
    PARTITION_PERSISTENT_EVICTION_LOCAL_DESTROY(RegionShortcut.PARTITION_PROXY,
        RegionShortcut.PARTITION_PERSISTENT, EvictionAttributes
            .createLRUEntryAttributes(MAX_ENTRIES_FOR_EVICTION, EvictionAction.LOCAL_DESTROY))


    ;


    EvictionAttributes evictionAttributes = null;
    private RegionShortcut serverRegionShortcut;
    private RegionShortcut clientRegionShortcut;

    RegionTestableType(RegionShortcut clientRegionShortcut, RegionShortcut serverRegionShortcut) {
      this(clientRegionShortcut, serverRegionShortcut, null);
    }

    RegionTestableType(RegionShortcut clientRegionShortcut, RegionShortcut serverRegionShortcut,
        EvictionAttributes evictionAttributes) {
      this.clientRegionShortcut = clientRegionShortcut;
      this.serverRegionShortcut = serverRegionShortcut;
      this.evictionAttributes = evictionAttributes;
    }

    public Region createDataStore(Cache cache, String regionName) {
      if (this.equals(FIXED_PARTITION)) {
        try {
          return LuceneTestUtilities.initDataStoreForFixedPR(cache);
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
      }
      if (evictionAttributes == null) {
        return cache.createRegionFactory(serverRegionShortcut)
            .setPartitionAttributes(getPartitionAttributes(false)).create(regionName);
      } else {
        return cache.createRegionFactory(serverRegionShortcut)
            .setPartitionAttributes(getPartitionAttributes(false))
            .setEvictionAttributes(evictionAttributes).create(regionName);
      }
    }

    public Region createAccessor(Cache cache, String regionName) {
      if (this.equals(PARTITION_WITH_CLIENT)) {
        return null;
      }
      if (this.equals(FIXED_PARTITION)) {
        return LuceneTestUtilities.createFixedPartitionedRegion(cache, regionName, null, 0);
      }
      if (evictionAttributes == null) {
        return cache.createRegionFactory(clientRegionShortcut)
            .setPartitionAttributes(getPartitionAttributes(true)).create(regionName);
      } else {
        return cache.createRegionFactory(clientRegionShortcut)
            .setPartitionAttributes(getPartitionAttributes(true))
            .setEvictionAttributes(evictionAttributes).create(regionName);
      }
    }
  }

  protected static PartitionAttributes getPartitionAttributes(final boolean isAccessor) {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    } else {
      factory.setLocalMaxMemory(100);
    }
    factory.setTotalNumBuckets(NUM_BUCKETS);
    return factory.create();
  }

}
