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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.execute.CustomerIDPartitionResolver;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class DistTXDebugDUnitTest extends CacheTestCase {
  VM accessor = null;
  VM dataStore1 = null;
  VM dataStore2 = null;
  VM dataStore3 = null;

  public DistTXDebugDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    accessor = host.getVM(3);
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        InternalResourceManager.setResourceObserver(null);
      }
    });
    InternalResourceManager.setResourceObserver(null);
  }

  public static void createCacheInVm() {
    new DistTXDebugDUnitTest("temp").getCache();
  }

  protected void createCacheInAllVms() {
    dataStore1.invoke(DistTXDebugDUnitTest.class, "createCacheInVm");
    dataStore2.invoke(DistTXDebugDUnitTest.class, "createCacheInVm");
    dataStore3.invoke(DistTXDebugDUnitTest.class, "createCacheInVm");
    accessor.invoke(DistTXDebugDUnitTest.class, "createCacheInVm");
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith,
      Boolean isPartitionResolver) {
    createPR(partitionedRegionName, redundancy, localMaxMemory,
        totalNumBuckets, colocatedWith, isPartitionResolver, 
        Boolean.TRUE/*Concurrency checks; By default is false*/);
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith,
      Boolean isPartitionResolver, Boolean concurrencyChecks) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    paf.setRedundantCopies(redundancy.intValue());
    if (localMaxMemory != null) {
      paf.setLocalMaxMemory(localMaxMemory.intValue());
    }
    if (totalNumBuckets != null) {
      paf.setTotalNumBuckets(totalNumBuckets.intValue());
    }
    if (colocatedWith != null) {
      paf.setColocatedWith((String) colocatedWith);
    }
    if (isPartitionResolver.booleanValue()) {
      paf.setPartitionResolver(new CustomerIDPartitionResolver(
          "CustomerIDPartitionResolver"));
    }
    PartitionAttributes prAttr = paf.create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    attr.setConcurrencyChecksEnabled(concurrencyChecks);
    // assertNotNull(basicGetCache());
    // Region pr = basicGetCache().createRegion(partitionedRegionName,
    // attr.create());
    assertNotNull(cache);
    Region pr = cache.createRegion(partitionedRegionName, attr.create());
    assertNotNull(pr);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + partitionedRegionName
            + " created Successfully :" + pr.toString());
  }

  protected void createPartitionedRegion(Object[] attributes) {
    dataStore1.invoke(DistTXDebugDUnitTest.class, "createPR", attributes);
    dataStore2.invoke(DistTXDebugDUnitTest.class, "createPR", attributes);
    dataStore3.invoke(DistTXDebugDUnitTest.class, "createPR", attributes);
    // make Local max memory = o for accessor
    attributes[2] = new Integer(0);
    accessor.invoke(DistTXDebugDUnitTest.class, "createPR", attributes);
  }

  public static void destroyPR(String partitionedRegionName) {
    // assertNotNull(basicGetCache());
    // Region pr = basicGetCache().getRegion(partitionedRegionName);

    assertNotNull(cache);
    Region pr = cache.getRegion(partitionedRegionName);
    assertNotNull(pr);
    LogWriterUtils.getLogWriter().info(
        "Destroying Partitioned Region " + partitionedRegionName);
    pr.destroyRegion();
  }

  public static void createRR(String replicatedRegionName, boolean empty) {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_ACK);
    if (empty) {
      af.setDataPolicy(DataPolicy.EMPTY);
    } else {
      af.setDataPolicy(DataPolicy.REPLICATE);
    }
    // Region rr = basicGetCache().createRegion(replicatedRegionName,
    // af.create());
    Region rr = cache.createRegion(replicatedRegionName, af.create());
    assertNotNull(rr);
    LogWriterUtils.getLogWriter().info(
        "Replicated Region " + replicatedRegionName + " created Successfully :"
            + rr.toString());
  }

  protected void createReplicatedRegion(Object[] attributes) {
    dataStore1.invoke(DistTXDebugDUnitTest.class, "createRR", attributes);
    dataStore2.invoke(DistTXDebugDUnitTest.class, "createRR", attributes);
    dataStore3.invoke(DistTXDebugDUnitTest.class, "createRR", attributes);
    // DataPolicy.EMPTY for accessor
    attributes[1] = Boolean.TRUE;
    accessor.invoke(DistTXDebugDUnitTest.class, "createRR", attributes);
  }

  public void testTXPR() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        // PartitionedRegion pr1 = (PartitionedRegion)
        // basicGetCache().getRegion(
        // "pregion1");
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        // put some data (non tx ops)
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.put");
          pr1.put(dummy, "1_entry__" + i);
        }

        // put in tx and commit
        // CacheTransactionManager ctx = basicGetCache()
        // .getCacheTransactionManager();
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.put in tx 1");
          pr1.put(dummy, "2_entry__" + i);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get");
          assertEquals("2_entry__" + i, pr1.get(dummy));
        }

        // put data in tx and rollback
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.put in tx 2");
          pr1.put(dummy, "3_entry__" + i);
        }
        ctx.rollback();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get");
          assertEquals("2_entry__" + i, pr1.get(dummy));
        }

        // destroy data in tx and commit
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.destroy in tx 3");
          pr1.destroy(dummy);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get");
          assertEquals(null, pr1.get(dummy));
        }

        // verify data size on all replicas
        SerializableCallable verifySize = new SerializableCallable("getOps") {
          @Override
          public Object call() throws CacheException {
            PartitionedRegion pr1 = (PartitionedRegion) cache
                .getRegion("pregion1");
            LogWriterUtils.getLogWriter().info(
                " calling pr.getLocalSize " + pr1.getLocalSize());
            assertEquals(0, pr1.getLocalSize());
            return null;
          }
        };
        dataStore1.invoke(verifySize);
        dataStore2.invoke(verifySize);
        dataStore3.invoke(verifySize);

        return null;
      }
    };

    accessor.invoke(TxOps);

    accessor.invoke(DistTXDebugDUnitTest.class, "destroyPR",
        new Object[] { "pregion1" });
  }

  public void testTXDestroy_invalidate() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    Object[] rrAttrs = new Object[] { "rregion1", Boolean.FALSE };
    createReplicatedRegion(rrAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        Region rr1 = cache.getRegion("rregion1");

        // put some data (non tx ops)
        for (int i = 1; i <= 6; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling non-tx put");
          pr1.put(dummy, "1_entry__" + i);
          rr1.put(dummy, "1_entry__" + i);
        }

        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        // destroy data in tx and commit
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(
              " calling pr1.destroy in tx key=" + dummy);
          pr1.destroy(dummy);
          LogWriterUtils.getLogWriter().info(" calling rr1.destroy in tx key=" + i);
          rr1.destroy(dummy);
        }
        for (int i = 4; i <= 6; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(
              " calling pr1.invalidate in tx key=" + dummy);
          pr1.invalidate(dummy);
          LogWriterUtils.getLogWriter().info(" calling rr1.invalidate in tx key=" + i);
          rr1.invalidate(dummy);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 6; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr1.get");
          assertEquals(null, pr1.get(dummy));
          LogWriterUtils.getLogWriter().info(" calling rr1.get");
          assertEquals(null, rr1.get(i));
        }
        return null;
      }
    };

    accessor.invoke(TxOps);

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        Region rr1 = cache.getRegion("rregion1");
        LogWriterUtils.getLogWriter().info(
            " calling pr1.getLocalSize " + pr1.getLocalSize());
        assertEquals(2, pr1.getLocalSize());
        LogWriterUtils.getLogWriter().info(" calling rr1.size " + rr1.size());
        assertEquals(3, rr1.size());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);

    accessor.invoke(DistTXDebugDUnitTest.class, "destroyPR",
        new Object[] { "pregion1" });
  }

  public void testTXPR_RR() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    Object[] rrAttrs = new Object[] { "rregion1", Boolean.FALSE };
    createReplicatedRegion(rrAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        // PartitionedRegion pr1 = (PartitionedRegion)
        // basicGetCache().getRegion(
        // "pregion1");
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        // Region rr1 = basicGetCache().getRegion("rregion1");
        Region rr1 = cache.getRegion("rregion1");
        // put some data (non tx ops)
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.put non-tx PR1_entry__" + i);
          pr1.put(dummy, "PR1_entry__" + i);
          LogWriterUtils.getLogWriter().info(" calling rr.put non-tx RR1_entry__" + i);
          rr1.put(new Integer(i), "RR1_entry__" + i);
        }

        // put in tx and commit
        // CacheTransactionManager ctx = basicGetCache()
        // .getCacheTransactionManager();
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.put in tx PR2_entry__" + i);
          pr1.put(dummy, "PR2_entry__" + i);
          LogWriterUtils.getLogWriter().info(" calling rr.put in tx RR2_entry__" + i);
          rr1.put(new Integer(i), "RR2_entry__" + i);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get PR2_entry__" + i);
          assertEquals("PR2_entry__" + i, pr1.get(dummy));
          LogWriterUtils.getLogWriter().info(" calling rr.get RR2_entry__" + i);
          assertEquals("RR2_entry__" + i, rr1.get(new Integer(i)));
        }
        return null;
      }
    };

    accessor.invoke(TxOps);

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        LogWriterUtils.getLogWriter().info(
            " calling pr.getLocalSize " + pr1.getLocalSize());
        assertEquals(2, pr1.getLocalSize());

        Region rr1 = cache.getRegion("rregion1");
        LogWriterUtils.getLogWriter()
            .info(" calling rr.getLocalSize " + rr1.size());
        assertEquals(3, rr1.size());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);

    accessor.invoke(DistTXDebugDUnitTest.class, "destroyPR",
        new Object[] { "pregion1" });
  }

  public void testTXPR2() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        // PartitionedRegion pr1 = (PartitionedRegion)
        // basicGetCache().getRegion(
        // "pregion1");
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");

        // put in tx and commit
        // CacheTransactionManager ctx = basicGetCache()
        // .getCacheTransactionManager();
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.put in tx 1");
          pr1.put(dummy, "2_entry__" + i);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get " + pr1.get(dummy));
          assertEquals("2_entry__" + i, pr1.get(dummy));
        }
        return null;
      }
    };

    accessor.invoke(TxOps);

    SerializableCallable TxGetOps = new SerializableCallable("TxGetOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        LogWriterUtils.getLogWriter().info(
            " calling pr.getLocalSize " + pr1.getLocalSize());
        assertEquals(2, pr1.getLocalSize());
        return null;
      }
    };

    dataStore1.invoke(TxGetOps);
    dataStore2.invoke(TxGetOps);
    dataStore3.invoke(TxGetOps);

    SerializableCallable TxRollbackOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        // PartitionedRegion pr1 = (PartitionedRegion)
        // basicGetCache().getRegion(
        // "pregion1");
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");

        // put in tx and commit
        // CacheTransactionManager ctx = basicGetCache()
        // .getCacheTransactionManager();
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(
              " calling pr.put in tx for rollback no_entry__" + i);
          pr1.put(dummy, "no_entry__" + i);
        }
        ctx.rollback();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(
              " calling pr.get after rollback " + pr1.get(dummy));
          assertEquals("2_entry__" + i, pr1.get(dummy));
        }
        return null;
      }
    };

    accessor.invoke(TxRollbackOps);

    accessor.invoke(DistTXDebugDUnitTest.class, "destroyPR",
        new Object[] { "pregion1" });
  }
  
  public void testTXPRRR2_create() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    Object[] rrAttrs = new Object[] { "rregion1", Boolean.FALSE };
    createReplicatedRegion(rrAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        Region rr1 = cache.getRegion("rregion1");
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.create in tx 1");
          pr1.create(dummy, "2_entry__" + i);
          
          LogWriterUtils.getLogWriter().info(" calling rr.create " + "2_entry__" + i);
          rr1.create(new Integer(i), "2_entry__" + i);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get " + pr1.get(dummy));
          assertEquals("2_entry__" + i, pr1.get(dummy));
          
          LogWriterUtils.getLogWriter().info(
              " calling rr.get " + rr1.get(new Integer(i)));
          assertEquals("2_entry__" + i, rr1.get(new Integer(i)));
        }
        return null;
      }
    };

    accessor.invoke(TxOps);

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");
        LogWriterUtils.getLogWriter()
            .info(" calling rr.getLocalSize " + rr1.size());
        assertEquals(3, rr1.size());
        
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        LogWriterUtils.getLogWriter().info(
            " calling pr.getLocalSize " + pr1.getLocalSize());
        assertEquals(2, pr1.getLocalSize());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);
  }
  
  public void testTXPRRR2_putall() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    Object[] rrAttrs = new Object[] { "rregion1", Boolean.FALSE };
    createReplicatedRegion(rrAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        Region rr1 = cache.getRegion("rregion1");     
        
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        HashMap<DummyKeyBasedRoutingResolver, String> phm = new HashMap<DummyKeyBasedRoutingResolver, String>();
        HashMap<Integer, String> rhm = new HashMap<Integer, String>();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          phm.put(dummy, "2_entry__" + i);
          rhm.put(i, "2_entry__" + i);
        }
        pr1.putAll(phm);
        rr1.putAll(rhm);
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get " + pr1.get(dummy));
          assertEquals("2_entry__" + i, pr1.get(dummy));
          
          LogWriterUtils.getLogWriter().info(
              " calling rr.get " + rr1.get(new Integer(i)));
          assertEquals("2_entry__" + i, rr1.get(new Integer(i)));
        }
        return null;
      }
    };

    accessor.invoke(TxOps);

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");
        LogWriterUtils.getLogWriter()
            .info(" calling rr.getLocalSize " + rr1.size());
        assertEquals(3, rr1.size());
        
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        LogWriterUtils.getLogWriter().info(
            " calling pr.getLocalSize " + pr1.getLocalSize());
        assertEquals(2, pr1.getLocalSize());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);
    
//    accessor.invoke(TxOps);
  }
  
  public void testTXPR_putall() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        HashMap<DummyKeyBasedRoutingResolver, String> phm = new HashMap<DummyKeyBasedRoutingResolver, String>();
        HashMap<Integer, String> rhm = new HashMap<Integer, String>();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          phm.put(dummy, "2_entry__" + i);
        }
        pr1.putAll(phm);
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get " + pr1.get(dummy));
          assertEquals("2_entry__" + i, pr1.get(dummy));
          
        }
        return null;
      }
    };

//    dataStore1.invoke(TxOps);
    accessor.invoke(TxOps);

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        LogWriterUtils.getLogWriter().info(
            " calling pr.getLocalSize " + pr1.getLocalSize());
        assertEquals(2, pr1.getLocalSize());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);
    
//    accessor.invoke(TxOps);
  }

  
  public void testTXRR_removeAll() throws Exception {
    performRR_removeAllTest(false);
  }
  
  public void testTXRR_removeAll_dataNodeAsCoordinator() throws Exception {
    performRR_removeAllTest(true);
  }

  /**
   * @param dataNodeAsCoordinator TODO
   * 
   */
  private void performRR_removeAllTest(boolean dataNodeAsCoordinator) {
    createCacheInAllVms();
    Object[] rrAttrs = new Object[] { "rregion1", Boolean.FALSE };
    createReplicatedRegion(rrAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");     
        //put some data
        HashMap<Integer, String> rhm = new HashMap<Integer, String>();
        for (int i = 1; i <= 3; i++) {
          rhm.put(i, "2_entry__" + i);
        }
        rr1.putAll(rhm);
        
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        rr1.removeAll(rhm.keySet());

        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          LogWriterUtils.getLogWriter().info(
              " calling rr.get " + rr1.get(new Integer(i)));
          assertEquals(null, rr1.get(new Integer(i)));
        }
        return null;
      }
    };
    
    if (dataNodeAsCoordinator) {
      dataStore1.invoke(TxOps);
    } else {
      accessor.invoke(TxOps);
    }

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");
        LogWriterUtils.getLogWriter()
            .info(" calling rr.getLocalSize " + rr1.size());
        assertEquals(0, rr1.size());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);
    
//    accessor.invoke(TxOps);
  }
  
  public void testTXPR_removeAll() throws Exception {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        HashMap<DummyKeyBasedRoutingResolver, String> phm = new HashMap<DummyKeyBasedRoutingResolver, String>();
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          phm.put(dummy, "2_entry__" + i);
        }
        pr1.putAll(phm);
        
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        pr1.removeAll(phm.keySet());
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(
              i);
          LogWriterUtils.getLogWriter().info(" calling pr.get " + pr1.get(dummy));
          assertEquals(null, pr1.get(dummy));
        }
        return null;
      }
    };

    accessor.invoke(TxOps);

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        PartitionedRegion pr1 = (PartitionedRegion) cache.getRegion("pregion1");
        LogWriterUtils.getLogWriter().info(
            " calling pr.getLocalSize " + pr1.getLocalSize());
        assertEquals(0, pr1.getLocalSize());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);
    
//    accessor.invoke(TxOps);
  }

  
  public void performTXRRtestOps(boolean makeDatNodeAsCoordinator) {
    createCacheInAllVms();
    Object[] prAttrs = new Object[] { "pregion1", 1, null, 3, null,
        Boolean.FALSE, Boolean.FALSE };
    createPartitionedRegion(prAttrs);

    Object[] rrAttrs = new Object[] { "rregion1", Boolean.FALSE };
    createReplicatedRegion(rrAttrs);

    SerializableCallable TxOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          LogWriterUtils.getLogWriter().info(" calling rr.put " + "2_entry__" + i);
          rr1.put(new Integer(i), "2_entry__" + i);
        }
        ctx.commit();

        // verify the data
        for (int i = 1; i <= 3; i++) {
          LogWriterUtils.getLogWriter().info(
              " calling rr.get " + rr1.get(new Integer(i)));
          assertEquals("2_entry__" + i, rr1.get(new Integer(i)));
        }
        return null;
      }
    };
    
    if (makeDatNodeAsCoordinator) {
      dataStore1.invoke(TxOps);
    } else {
      accessor.invoke(TxOps);  
    }

    // verify data size on all replicas
    SerializableCallable verifySize = new SerializableCallable("getOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");
        LogWriterUtils.getLogWriter()
            .info(" calling rr.getLocalSize " + rr1.size());
        assertEquals(3, rr1.size());
        return null;
      }
    };
    dataStore1.invoke(verifySize);
    dataStore2.invoke(verifySize);
    dataStore3.invoke(verifySize);

    SerializableCallable TxRollbackOps = new SerializableCallable("TxOps") {
      @Override
      public Object call() throws CacheException {
        Region rr1 = cache.getRegion("rregion1");
        CacheTransactionManager ctx = cache.getCacheTransactionManager();
        ctx.setDistributed(true);
        ctx.begin();
        for (int i = 1; i <= 3; i++) {
          LogWriterUtils.getLogWriter().info(
              " calling rr.put for rollback no_entry__" + i);
          rr1.put(new Integer(i), "no_entry__" + i);
        }
        ctx.rollback();
        ;

        // verify the data
        for (int i = 1; i <= 3; i++) {
          LogWriterUtils.getLogWriter().info(
              " calling rr.get after rollback "
                  + rr1.get(new Integer(i)));
          assertEquals("2_entry__" + i, rr1.get(new Integer(i)));
        }
        return null;
      }
    };

    if (makeDatNodeAsCoordinator) {
      dataStore1.invoke(TxRollbackOps);
    } else {
      accessor.invoke(TxRollbackOps);  
    }
  }
    

  public void testTXRR2() throws Exception {
    performTXRRtestOps(false); // actual test
  }

  public void testTXRR2_dataNodeAsCoordinator() throws Exception {
    performTXRRtestOps(true);
  }
}

class DummyKeyBasedRoutingResolver implements PartitionResolver,
    DataSerializable {
  Integer dummyID;

  public DummyKeyBasedRoutingResolver() {
  }

  public DummyKeyBasedRoutingResolver(int id) {
    this.dummyID = new Integer(id);
  }

  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    return (Serializable) opDetails.getKey();
  }

  public void close() {
    // TODO Auto-generated method stub
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.dummyID = DataSerializer.readInteger(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.dummyID, out);
  }

  @Override
  public int hashCode() {
    int i = this.dummyID.intValue();
    return i;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof DummyKeyBasedRoutingResolver))
      return false;

    DummyKeyBasedRoutingResolver otherDummyID = (DummyKeyBasedRoutingResolver) o;
    return (otherDummyID.dummyID.equals(dummyID));

  }
}
