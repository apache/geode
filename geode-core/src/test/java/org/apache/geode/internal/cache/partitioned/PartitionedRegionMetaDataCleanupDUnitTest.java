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
package org.apache.geode.internal.cache.partitioned;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;

/**
 *
 */
@Category(DistributedTest.class)
public class PartitionedRegionMetaDataCleanupDUnitTest extends JUnit4CacheTestCase {

  public PartitionedRegionMetaDataCleanupDUnitTest() {
    super();
  }
  
  @Test
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
  
  @Test
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
  
  @Test
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
