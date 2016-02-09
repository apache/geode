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
package com.gemstone.gemfire.disttx;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;

public class DistTXPersistentDebugDUnitTest extends DistTXDebugDUnitTest {

  public DistTXPersistentDebugDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "true");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
        return null;
      }
    }); 
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //System.setProperty("gemfire.ALLOW_PERSISTENT_TRANSACTIONS", "false");
        TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
        return null;
      }
    }); 
  }
  
  protected void createPesistentPR(Object[] attributes) {
    dataStore1.invoke(DistTXPersistentDebugDUnitTest.class, "createPersistentPR", attributes);
    dataStore2.invoke(DistTXPersistentDebugDUnitTest.class, "createPersistentPR", attributes);
//    dataStore3.invoke(TxPersistentDebugDUnit.class, "createPR", attributes);
//    // make Local max memory = o for accessor
//    attributes[2] = new Integer(0);
//    accessor.invoke(TxPersistentDebugDUnit.class, "createPR", attributes);
  }
  
  public static void createPersistentPR(String regionName) {
    assertNotNull(cache);
    cache.createRegion(regionName, getPersistentPRAttributes(1, -1, cache, 113, true));
  }
  
  protected static RegionAttributes getPersistentPRAttributes(final int redundancy, final int recoveryDelay,
      Cache cache, int numBuckets, boolean synchronous) {
        DiskStore ds = cache.findDiskStore("disk");
        if(ds == null) {
          ds = cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs()).create("disk");
        }
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy);
        paf.setRecoveryDelay(recoveryDelay);
        paf.setTotalNumBuckets(numBuckets);
        paf.setLocalMaxMemory(500);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        af.setDiskStoreName("disk");
        af.setDiskSynchronous(synchronous);
        RegionAttributes attr = af.create();
        return attr;
      }
  
  public void testBasicDistributedTX() throws Exception {
    createCacheInAllVms();
    final String regionName = "persistentCustomerPRRegion";
    Object[] attrs = new Object[] { regionName };
    createPesistentPR(attrs);
    SerializableCallable TxOps = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = cache.getCacheTransactionManager();
        mgr.setDistributed(true);
        LogWriterUtils.getLogWriter().fine("SJ:TX BEGIN");
        mgr.begin();
        Region<CustId, Customer> prRegion = cache.getRegion(regionName);

        CustId custIdOne = new CustId(1);
        Customer customerOne = new Customer("name1", "addr1");
        LogWriterUtils.getLogWriter().fine("SJ:TX PUT 1");
        prRegion.put(custIdOne, customerOne);

        CustId custIdTwo = new CustId(2);
        Customer customerTwo = new Customer("name2", "addr2");
        LogWriterUtils.getLogWriter().fine("SJ:TX PUT 2");
        prRegion.put(custIdTwo, customerTwo);

        LogWriterUtils.getLogWriter().fine("SJ:TX COMMIT");
        mgr.commit();
        return null;
      }
    };

    dataStore2.invoke(TxOps);
  }
}
