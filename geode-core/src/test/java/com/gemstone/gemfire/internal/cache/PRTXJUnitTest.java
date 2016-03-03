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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.TXJUnitTest;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class PRTXJUnitTest extends TXJUnitTest {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.TXTest#createRegion()
   */
  @Override
  protected void createRegion() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setConcurrencyChecksEnabled(false);  // test validation expects this behavior
    af.setPartitionAttributes(new PartitionAttributesFactory()
      .setTotalNumBuckets(3).create());
    //this.region = this.cache.createRegion("PRTXJUnitTest", af.create());
    this.region = new PRWithLocalOps("PRTXJUnitTest", af.create(),
        null, this.cache, new InternalRegionArguments()
        .setDestroyLockFlag(true).setRecreateFlag(false)
        .setSnapshotInputStream(null).setImageTarget(null));
    ((PartitionedRegion)this.region).initialize(null, null, null);
    ((PartitionedRegion)this.region).postCreateRegion();
    this.cache.setRegionByPath(this.region.getFullPath(), (LocalRegion)this.region);
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.TXTest#checkUserAttributeConflict(com.gemstone.gemfire.internal.cache.TXManagerImpl)
   */
  @Override
  protected void checkUserAttributeConflict(CacheTransactionManager txMgrImpl) {
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.TXTest#checkSubRegionCollecection(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  @Override
  protected void checkSubRegionCollecection(Region reg1) {
  }
  @Override
  @Ignore
  @Test
  public void testTXAndQueries() throws CacheException, QueryException {
    // TODO fix this?
  }
  @Override
  @Ignore
  @Test
  public void testCollections() throws CacheException {
    // TODO make PR iterators tx aware
  }
  @Override
  @Ignore
  @Test
  public void testTxAlgebra() throws CacheException {
    // TODO Auto-generated method stub
  }
  @Test
  public void testTxId() {
    AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
    af.setPartitionAttributes(new PartitionAttributesFactory<String, Integer>()
        .setTotalNumBuckets(2).create());
    Region<String, Integer> r = this.cache.createRegion("testTxId", af.create());
    r.put("one", 1);
    CacheTransactionManager mgr = this.cache.getTxManager();
    mgr.begin();
    r.put("two", 2);
    mgr.getTransactionId();
    mgr.rollback();
  }

  private static class PRWithLocalOps extends PartitionedRegion {

    /**
     * @param regionname
     * @param ra
     * @param parentRegion
     * @param cache
     * @param internalRegionArgs
     */
    public PRWithLocalOps(String regionname, RegionAttributes ra,
        LocalRegion parentRegion, GemFireCacheImpl cache,
        InternalRegionArguments internalRegionArgs) {
      super(regionname, ra, parentRegion, cache, internalRegionArgs);
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#localDestroy(java.lang.Object, java.lang.Object)
     */
    @Override
    public void localDestroy(Object key, Object callbackArgument)
        throws EntryNotFoundException {
      super.destroy(key, callbackArgument);
    }
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#localInvalidate(java.lang.Object, java.lang.Object)
     */
    @Override
    public void localInvalidate(Object key, Object callbackArgument)
        throws EntryNotFoundException {
      super.invalidate(key, callbackArgument);
    }
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#localInvalidateRegion(java.lang.Object)
     */
    @Override
    public void localInvalidateRegion(Object callbackArgument) {
      super.invalidateRegion(callbackArgument);
    }
  }
}
