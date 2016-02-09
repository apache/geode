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

import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * @author dsmith
 *
 */
public class PartitionedRegionMetaDataCleanupDUnitTest extends CacheTestCase {

  public PartitionedRegionMetaDataCleanupDUnitTest(String name) {
    super(name);
  }
  
  public void testCleanupOnCloseCache() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    createPR(vm0, "region1", 5);
    createPR(vm1, "region2", 10);
    //This should fail
    IgnoredException ex = IgnoredException.addIgnoredException( "IllegalStateException", vm1);
    try {
      createPR(vm1, "region1", 10);
      fail("Should have received an exception");
    } catch(RMIException e) {
      //ok
    } finally {
      ex.remove();
    }
    closeCache(vm0);
    waitForCreate(vm0, "region1", 15);
  }
  
  public void testCleanupOnCloseRegion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    createPR(vm0, "region1", 5);
    createPR(vm1, "region2", 10);
    //This should fail
    IgnoredException ex = IgnoredException.addIgnoredException( "IllegalStateException", vm1);
    try {
      createPR(vm1, "region1", 10);
      fail("Should have received an exception");
    } catch(RMIException e) {
      //ok
    } finally {
      ex.remove();
    }
    closePr(vm0, "region1");
    waitForCreate(vm0, "region1", 15);
  }
  
  public void testCrash() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    createPR(vm0, "region1", 5);
    createPR(vm1, "region2", 10);
    //This should fail
    IgnoredException ex = IgnoredException.addIgnoredException("IllegalStateException", vm1);
    try {
      createPR(vm1, "region1", 10);
      fail("Should have received an exception");
    } catch(RMIException e) {
      //ok
    } finally {
      ex.remove();
    }
    
    ex = IgnoredException.addIgnoredException("DistributedSystemDisconnectedException", vm0);
    try {
      fakeCrash(vm0);
    } finally {
      ex.remove();
    }
    
    waitForCreate(vm0, "region1", 15);
  }
  
  private void closeCache(VM vm0) {
    vm0.invoke(new SerializableRunnable()  {
      public void run() {
        closeCache();

      }
    });
  }
  
  private void fakeCrash(VM vm0) {
    vm0.invoke(new SerializableRunnable()  {
      public void run() {
        InternalDistributedSystem ds = (InternalDistributedSystem) getCache().getDistributedSystem();
        //Shutdown without closing the cache.
        ds.getDistributionManager().close();
        
        //now cleanup the cache and ds.
        disconnectFromDS();

      }
    });
  }
  
  private void closePr(VM vm0, final String regionName) {
    vm0.invoke(new SerializableRunnable()  {
      public void run() {
        getCache().getRegion(regionName).close();
      }
    });
  }

  private void createPR(VM vm0, final String regionName, final int expirationTime) {
    vm0.invoke(new SerializableRunnable()  {
      public void run() {
        getCache().createRegionFactory(RegionShortcut.PARTITION)
//          .setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(evictionEntries, EvictionAction.LOCAL_DESTROY))
          .setEntryTimeToLive(new ExpirationAttributes(expirationTime))
          .create(regionName);
      }
    });
  }

  /**
   * Try to create the region with the given attributes.
   * This will try 20 times to create the region until
   * the region can be successfully created without an illegal state exception.
   * 
   * This is a workaround for bug 47125, because the metadata cleanup happens
   * asynchronously.
   */
  private void waitForCreate(VM vm0, final String regionName, final int expirationTime) {
    vm0.invoke(new SerializableRunnable()  {
      public void run() {
        RegionFactory<Object, Object> rf = getCache().createRegionFactory(RegionShortcut.PARTITION)
//          .setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(evictionEntries, EvictionAction.LOCAL_DESTROY))
          .setEntryTimeToLive(new ExpirationAttributes(expirationTime));

        //We may log an exception if the create fails. Ignore thse.
          IgnoredException ex = IgnoredException.addIgnoredException("IllegalStateException");
          try {
            int i= 0;
            //Loop until a successful create
            while(true) {
              try {
                i++;
                rf.create(regionName);
                //if the create was succesfull, we're done
                return;
              } catch(IllegalStateException expected) {
                //give up if we can't create the region in 20 tries
                if(i == 20) {
                  Assert.fail("Metadata was never cleaned up in 20 tries", expected);
                }
                
                //wait a bit before the next attempt.
                Wait.pause(500);
              }
            }
          } finally {
            ex.remove();
          }
      }
    });
  }
}
