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
package org.apache.geode.disttx;

import static org.apache.geode.test.dunit.Assert.*;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class DistTXPersistentDebugDUnitTest extends DistTXDebugDUnitTest {

  @Override
  public final void postSetUpDistTXDebugDUnitTest() throws Exception {
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
  public final void preTearDownCacheTestCase() throws Exception {
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
    assertNotNull(basicGetCache());
    basicGetCache().createRegion(regionName, getPersistentPRAttributes(1, -1, basicGetCache(), 113, true));
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

  @Test
  public void testBasicDistributedTX() throws Exception {
    createCacheInAllVms();
    final String regionName = "persistentCustomerPRRegion";
    Object[] attrs = new Object[] { regionName };
    createPesistentPR(attrs);
    SerializableCallable TxOps = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheTransactionManager mgr = basicGetCache().getCacheTransactionManager();
        mgr.setDistributed(true);
        LogWriterUtils.getLogWriter().fine("SJ:TX BEGIN");
        mgr.begin();
        Region<CustId, Customer> prRegion = basicGetCache().getRegion(regionName);

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
