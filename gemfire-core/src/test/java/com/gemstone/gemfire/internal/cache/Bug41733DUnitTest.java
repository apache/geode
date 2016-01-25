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
package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBucketMessage.ManageBucketReplyMessage;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 * Test to make sure that we can handle 
 * a crash of the member directing bucket creation.
 */
public class Bug41733DUnitTest extends CacheTestCase {
  
  
  public Bug41733DUnitTest(String name) {
    super(name);
  }
  
  

  @Override
  public void tearDown2() throws Exception {
    disconnectAllFromDS();
    super.tearDown2();
  }



  /** 
   * Test the we can handle a member departing after creating
   * a bucket on the remote node but before we choose a primary
   */
  public void testCrashAfterBucketCreation() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    vm0.invoke(new SerializableRunnable("Install observer") {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof ManageBucketReplyMessage) {
              DistributedTestCase.disconnectFromDS();
            }
          }
        });
        
      }
    });
    createPR(vm0, 0);
    
    //Create a couple of buckets in VM0. This will make sure
    //the next bucket we create will be created in VM 1.
    putData(vm0, 0, 2, "a");
    
    createPR(vm1, 0);
    
    //Trigger a bucket creation in VM1, which should cause vm0 to close it's cache.
    try {
      putData(vm0, 3, 4, "a");
      fail("should have received a cache closed exception");
    } catch(RMIException e) {
      if(!(e.getCause() instanceof DistributedSystemDisconnectedException)) {
        throw e;
      }
    }
    
    assertEquals(Collections.singleton(3), getBucketList(vm1));
    
    //This shouldn't hang, because the bucket creation should finish,.
    putData(vm1, 3, 4, "a");
  }
  
  /** 
   * Test the we can handle a member departing while we are 
   *  in the process of creating the bucket on the remote node.
   */
  public void testCrashDuringBucketCreation() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    vm1.invoke(new SerializableRunnable("Install observer") {
      
      public void run() {
        
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
          
          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof ManageBucketMessage) {
              vm0.invoke(DistributedTestCase.class, "disconnectFromDS");
            }
          }
        });
        
      }
    });
    createPR(vm0, 0);
    
    //Create a couple of buckets in VM0. This will make sure
    //the next bucket we create will be created in VM 1.
    putData(vm0, 0, 2, "a");
    
    createPR(vm1, 0);
    
    //Trigger a bucket creation in VM1, which should cause vm0 to close it's cache.
    try {
      putData(vm0, 3, 4, "a");
      fail("should have received a cache closed exception");
    } catch(RMIException e) {
      if(!(e.getCause() instanceof DistributedSystemDisconnectedException)) {
        throw e;
      }
    }
    
    assertEquals(Collections.singleton(3), getBucketList(vm1));
    
    //This shouldn't hang, because the bucket creation should finish,.
    putData(vm1, 3, 4, "a");
  }

  /**
   * @param vm0
   * @param i
   */
  private void createPR(VM vm0, final int redundancy) {
    vm0.invoke(new SerializableRunnable("Create PR") {

      public void run() {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy);
        af.setPartitionAttributes(paf.create());
        af.setDataPolicy(DataPolicy.PARTITION);
        cache.createRegion("region", af.create());
      }
      
    });
  }
  
  protected void putData(VM vm, final int startKey, final int endKey,
      final String value) {
    SerializableRunnable createData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        
        for(int i =startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }
  
  protected Set<Integer> getBucketList(VM vm0) {
    return getBucketList(vm0, "region");
  }

  protected Set<Integer> getBucketList(VM vm0, final String regionName) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        return new TreeSet<Integer>(region.getDataStore().getAllLocalBucketIds());
      }
    };
    
    return (Set<Integer>) vm0.invoke(getBuckets);
  }

}
