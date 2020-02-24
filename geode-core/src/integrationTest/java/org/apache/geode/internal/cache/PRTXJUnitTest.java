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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.TXJUnitTest;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.internal.cache.partitioned.colocation.ColocationLoggerFactory;

public class PRTXJUnitTest extends TXJUnitTest {

  @Override
  protected void createRegion() throws Exception {
    InternalRegionFactory regionFactory = cache.createInternalRegionFactory();
    // test validation expects this behavior
    regionFactory.setConcurrencyChecksEnabled(false);
    regionFactory
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(3).create());
    regionFactory.setDestroyLockFlag(true).setRecreateFlag(false)
        .setSnapshotInputStream(null).setImageTarget(null);

    this.region =
        new PRWithLocalOps(getClass().getSimpleName(), regionFactory.getCreateAttributes(), null,
            this.cache, regionFactory.getInternalRegionArguments());

    ((PartitionedRegion) this.region).initialize(null, null, null);
    ((PartitionedRegion) this.region).postCreateRegion();
    this.cache.setRegionByPath(this.region.getFullPath(), (LocalRegion) this.region);
  }

  @Override
  protected void checkUserAttributeConflict(CacheTransactionManager txMgrImpl) {}

  @Override
  protected void checkSubRegionCollection(Region reg1) {}

  @Override
  @Ignore
  @Test
  public void testTXAndQueries() throws CacheException, QueryException {}

  @Override
  @Ignore
  @Test
  public void testCollections() throws CacheException {}

  @Override
  @Ignore
  @Test
  public void testTxAlgebra() throws CacheException {}

  @Test
  public void testTxId() {
    AttributesFactory<String, Integer> attributesFactory = new AttributesFactory<>();
    attributesFactory.setPartitionAttributes(
        new PartitionAttributesFactory<String, Integer>().setTotalNumBuckets(2).create());

    Region<String, Integer> region =
        this.cache.createRegion("testTxId", attributesFactory.create());
    region.put("one", 1);

    CacheTransactionManager txManager = this.cache.getTxManager();
    txManager.begin();
    region.put("two", 2);
    txManager.getTransactionId();
    txManager.rollback();
  }

  private static class PRWithLocalOps extends PartitionedRegion {

    PRWithLocalOps(String regionName, RegionAttributes ra, LocalRegion parentRegion,
        GemFireCacheImpl cache, InternalRegionArguments internalRegionArgs) {
      super(regionName, ra, parentRegion, cache, internalRegionArgs, disabledClock(),
          ColocationLoggerFactory.create());
    }

    @Override
    public void localDestroy(Object key, Object callbackArgument) throws EntryNotFoundException {
      super.destroy(key, callbackArgument);
    }

    @Override
    public void localInvalidate(Object key, Object callbackArgument) throws EntryNotFoundException {
      super.invalidate(key, callbackArgument);
    }

    @Override
    public void localInvalidateRegion(Object callbackArgument) {
      super.invalidateRegion(callbackArgument);
    }
  }
}
