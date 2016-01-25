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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBucketMessage;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;


/**
 * @author dsmith
 *
 */
public class Bug39356DUnitTest extends CacheTestCase {
  protected static final String REGION_NAME = "myregion";
  
  public Bug39356DUnitTest(String name) {
    super(name);
  }
  
  /**
   * This tests the case where the VM forcing other
   * VMs to create a bucket crashes while creating the bucket.
   */
  public void testCrashWhileCreatingABucket() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
    SerializableRunnable createParReg = new SerializableRunnable("Create parReg") {
      public void run() {
        DistributionMessageObserver.setInstance(new MyRegionObserver(vm0));
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory pf = new PartitionAttributesFactory();
        pf.setRedundantCopies(1);
        pf.setRecoveryDelay(0);
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setPartitionAttributes(pf.create());
        cache.createRegion(REGION_NAME, af.create());
      }
    };
    vm1.invoke(createParReg);
    vm2.invoke(createParReg);
    
    SerializableRunnable createParRegAccessor = new SerializableRunnable("Create parReg") {
      public void run() {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory pf = new PartitionAttributesFactory();
        pf.setRedundantCopies(1);
        pf.setLocalMaxMemory(0);
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setPartitionAttributes(pf.create());
        Region r = cache.createRegion(REGION_NAME, af.create());
        
        //trigger the creation of a bucket, which should trigger the destruction of this VM.
        try {
          r.put("ping", "pong");
          fail("Should have gotten a CancelException");
        } 
        catch (CancelException e) {
          //this is ok, we expect our observer to close this cache.
        }
      }
    };
  
    vm0.invoke(createParRegAccessor);
    
    SerializableRunnable verifyBuckets = new SerializableRunnable("Verify buckets") {

      public void run() {
        LogWriter log = getLogWriter();
        Cache cache = getCache();
        PartitionedRegion r = (PartitionedRegion) cache.getRegion(REGION_NAME);
        for(int i = 0; i < r.getAttributes().getPartitionAttributes().getTotalNumBuckets(); i++) {
          List owners = null;
          while(owners == null) {
            try {
              owners = r.getBucketOwnersForValidation(i);
            } catch (ForceReattemptException e) {
              log.info(Bug39356DUnitTest.class + " verify buckets Caught a ForceReattemptException");
              pause(1000);
            }
          }
          if(owners.isEmpty()) {
            log.info("skipping bucket " + i + " because it has no data");
            continue;
          }
          Assert.assertEquals("Expecting bucket " +  i + " to have two copies", 2, owners.size());
          log.info("bucket " + i + " had two copies");
          }
      }
    };
    vm1.invoke(verifyBuckets);
    vm2.invoke(verifyBuckets);
  }
  
  protected final class MyRegionObserver extends DistributionMessageObserver implements Serializable {
    private final VM vm0;

    /**
     * @param vm0
     */
    MyRegionObserver(VM vm0) {
      this.vm0 = vm0;
    }

    
    public void afterProcessMessage(DistributionManager dm,
        DistributionMessage message) {
    }


    public void beforeProcessMessage(DistributionManager dm,
        DistributionMessage message) {
      if(message instanceof ManageBucketMessage) {
        vm0.invoke(new SerializableRunnable("Disconnect VM 0") {
          public void run() {
            disconnectFromDS();
            try {
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        });
      }
    }

  }
  
  /**
   * A test to make sure that we cannot move a bucket to a member which already
   * hosts the bucket, thereby reducing our redundancy.
   */
  public void testMoveBucketToHostThatHasTheBucketAlready() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    SerializableRunnable createPrRegion = new SerializableRunnable("createRegion") {
      public void run()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setRecoveryDelay(-1);
        paf.setStartupRecoveryDelay(-1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    
  //Create a bucket
    vm0.invoke(new SerializableRunnable("createSomeBuckets") {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        region.put(Integer.valueOf(0), "A");
      }
    });
    
    final InternalDistributedMember vm1MemberId = (InternalDistributedMember) vm1.invoke(new SerializableCallable() {

      public Object call() throws Exception {
        return InternalDistributedSystem.getAnyInstance().getDistributedMember();
      }
    });
    
    
    //Move the bucket
    vm0.invoke(new SerializableRunnable("moveBucket") {
      
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        Set<InternalDistributedMember> owners = region.getRegionAdvisor().getBucketOwners(0);
        assertEquals(2, owners.size());
        PartitionedRegionDataStore ds = region.getDataStore();
        assertTrue(ds.isManagingBucket(0));
        //try to move the bucket from the other member to this one. This should
        //fail because we already have the bucket 
        assertFalse(ds.moveBucket(0, vm1MemberId, true));
        assertEquals(owners, region.getRegionAdvisor().getBucketOwners(0));
      }
    });
  }
}
