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


  @Override
  public void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
  }

  protected void initDataStore(SerializableRunnableIF createIndex,
      RegionTestableType regionTestType) throws Exception {
    createIndex.run();
    regionTestType.createRegion(getCache(), REGION_NAME);
  }

  // Needed to seperate due to client/server tests...
  protected void initAccessor(SerializableRunnableIF createIndex, RegionTestableType regionTestType)
      throws Exception {
    initDataStore(createIndex, regionTestType);
  }

  protected RegionTestableType[] getListOfServerRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION,
        RegionTestableType.PARTITION_PERSISTENT, RegionTestableType.PARTITION_REDUNDANT,
        RegionTestableType.PARTITION_OVERFLOW_TO_DISK, RegionTestableType.PARTITION_REDUNDANT};
  }


  protected Object[] getListOfClientServerTypes() {
    return new Object[] {
        new RegionTestableType[] {RegionTestableType.PARTITION_PROXY, RegionTestableType.PARTITION},
        new RegionTestableType[] {RegionTestableType.PARTITION, RegionTestableType.PARTITION},
        new RegionTestableType[] {RegionTestableType.PARTITION_PROXY_REDUNDANT,
            RegionTestableType.PARTITION_REDUNDANT},
        new RegionTestableType[] {RegionTestableType.PARTITION_PROXY_WITH_OVERFLOW,
            RegionTestableType.PARTITION_OVERFLOW_TO_DISK},
        new RegionTestableType[] {RegionTestableType.PARTITION_REDUNDANT,
            RegionTestableType.PARTITION_REDUNDANT},
        new RegionTestableType[] {RegionTestableType.PARTITION_OVERFLOW_TO_DISK,
            RegionTestableType.PARTITION_OVERFLOW_TO_DISK},
        new RegionTestableType[] {RegionTestableType.FIXED_PARTITION_ACCESSOR,
            RegionTestableType.FIXED_PARTITION}};
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
    PARTITION(RegionShortcut.PARTITION, false),
    PARTITION_PROXY(RegionShortcut.PARTITION_PROXY, true),
    PARTITION_PERSISTENT(RegionShortcut.PARTITION_PERSISTENT, false),
    PARTITION_REDUNDANT(RegionShortcut.PARTITION_REDUNDANT, false),
    PARTITION_OVERFLOW_TO_DISK(RegionShortcut.PARTITION_OVERFLOW, false,
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK)),
    PARTITION_PROXY_WITH_OVERFLOW(RegionShortcut.PARTITION_PROXY, true,
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK)),
    FIXED_PARTITION(RegionShortcut.PARTITION, false),
    FIXED_PARTITION_ACCESSOR(RegionShortcut.PARTITION, true),

    PARTITION_PROXY_REDUNDANT(RegionShortcut.PARTITION_PROXY_REDUNDANT, true),
    CLIENT_PARTITION(RegionShortcut.PARTITION, false),
    CLIENT_PARTITION_PERSISTENT(RegionShortcut.PARTITION_PERSISTENT, false),
    ACCESSOR_PARTITION_PERSISTENT(RegionShortcut.PARTITION_PERSISTENT, true);


    EvictionAttributes evictionAttributes = null;
    private RegionShortcut shortcut;
    private boolean isAccessor;

    RegionTestableType(RegionShortcut shortcut, boolean isAccessor) {
      this(shortcut, isAccessor, null);
    }

    RegionTestableType(RegionShortcut shortcut, boolean isAccessor,
        EvictionAttributes evictionAttributes) {
      this.shortcut = shortcut;
      this.isAccessor = isAccessor;
      this.evictionAttributes = evictionAttributes;
    }

    public Region createRegion(Cache cache, String regionName) {
      if (this.equals(CLIENT_PARTITION)) {
        return null;
      } else if (this.equals(FIXED_PARTITION_ACCESSOR)) {
        return LuceneTestUtilities.createFixedPartitionedRegion(cache, regionName, null, 0);
      } else if (this.equals(FIXED_PARTITION)) {
        try {
          return LuceneTestUtilities.initDataStoreForFixedPR(cache);
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
      }
      if (evictionAttributes == null) {
        return cache.createRegionFactory(shortcut)
            .setPartitionAttributes(getPartitionAttributes(isAccessor)).create(regionName);
      } else {
        return cache.createRegionFactory(shortcut)
            .setPartitionAttributes(getPartitionAttributes(isAccessor))
            .setEvictionAttributes(evictionAttributes).create(regionName);
      }
    }
  }

  protected static PartitionAttributes getPartitionAttributes(final boolean isAccessor) {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    if (!isAccessor) {
      factory.setLocalMaxMemory(100);
    }
    factory.setTotalNumBuckets(NUM_BUCKETS);
    return factory.create();
  }

}
