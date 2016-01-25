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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.DistributedCacheOperation;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

/**
 * @author Xiaojian Zhou
 *
 */
public class DistributedAckPersistentRegionCCEDUnitTest extends DistributedAckRegionCCEDUnitTest {

  /**
   * @param name
   */
  public DistributedAckPersistentRegionCCEDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

//  public void testClearWithConcurrentEventsAsync() throws Exception {
//    int end = 100;
//    for (int i=0; i<end; i++) {
//      System.out.println("Starting run #" + i);
//      super.testClearWithConcurrentEventsAsync();
//      if (i<(end-1)) {
//        tearDown();
//        setUp();
//      }
//    }
//  }
  
  public void testClearOnNonReplicateWithConcurrentEvents() {}
  
  public void testConcurrentEventsOnNonReplicatedRegion() {}
  
  public void testGetAllWithVersions() {}

  private VersionTag getVersionTag(VM vm, final String key) {
    SerializableCallable getVersionTag = new SerializableCallable("verify recovered entry") {
      public Object call() {
        VersionTag tag = CCRegion.getVersionTag(key);
        return tag;

      }
    };
    return (VersionTag)vm.invoke(getVersionTag);
  }
  
//  protected void do_version_recovery_if_necessary(final VM vm0, final VM vm1, final VM vm2, final Object[] params) {
//    final String name = (String)params[0];
//    final String hostName = (String)params[1];
//    final String key = (String)params[2];
//    final int ds1Port = (Integer)params[3];
//    final int hub0Port = (Integer)params[4];
//    final int hub2Port = (Integer)params[5];
//    
//    VersionTag tag0 = getVersionTag(vm0, key);
//    VersionTag tag1 = getVersionTag(vm1, key);
//    VersionTag tag2 = getVersionTag(vm2, key);
//
//    // shutdown all the systems and restart to recover from disk
//    disconnect(vm0);
//    disconnect(vm1);
//    disconnect(vm2);
//
//    createRegionWithGatewayHub(vm1, name, hostName, ds1Port, hub0Port, hub2Port, true);
//    createRegionWithGatewayHub(vm0, name, hostName, ds1Port, hub0Port, hub2Port, true);
//    createRegionWithGatewayHubAtOtherSide(vm2, name, hostName, hub0Port, hub2Port, true);
//
//    startHub(vm2);
//    startHub(vm0);
//
//    VersionTag tag00 = getVersionTag(vm0, key);
//    VersionTag tag11 = getVersionTag(vm1, key);
//    VersionTag tag22 = getVersionTag(vm2, key);
//    assertTrue(tag00.equals(tag0));
//    assertTrue(tag11.equals(tag1));
//    assertTrue(tag22.equals(tag2));
//  }
//
//  /**
//   * vm0 and vm1 are peers, each holds a DR. 
//   * vm0 do 3 puts to change the version to be 3, wait for distributions to vm1 
//   * Shutdown and restart both of them. 
//   * Make sure vm0 and vm1 are both at version 3. 
//   * Do local clear at vm0. Then do a put. It will use version 1.
//   * This new operation should be rejected as conflicts at vm1.
//   * That means, recovered version 3 can reject the new operation at version 1.
//   */
//  public void testNewOperationConflictWithRecoveredVersion() throws Throwable {
//    Host host = Host.getHost(0);
//    VM vm0 = host.getVM(0);
//    VM vm1 = host.getVM(1);
//    final String key = "Object2";
//
//    SerializableRunnable disconnect = new SerializableRunnable("disconnect") {
//      public void run() {
//        GatewayBatchOp.VERSION_WITH_OLD_WAN = false;
//        distributedSystemID = -1;
//        CCRegion.getCache().getDistributedSystem().disconnect();
//      }
//    };
//
//    final String name = this.getUniqueName() + "-CC";
//    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
//      public void run() {
//        try {
//          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
//          CCRegion = (LocalRegion)f.create(name);
//        } catch (CacheException ex) {
//          fail("While creating region", ex);
//        }
//      }
//    };
//    vm0.invoke(createRegion);
//    vm1.invoke(createRegion);
//
//    vm0.invoke(new SerializableRunnable("put several times to bump version to 3") {
//      public void run() {
//        CCRegion.put(key, "dummy"); // v1
//        CCRegion.put(key, "dummy"); // v2
//        CCRegion.put(key, "dummy"); // v3
//      }
//    });
//
//    VersionTag tag0 = getVersionTag(vm0, key);
//    VersionTag tag1 = getVersionTag(vm1, key);
//    assertEquals(3, tag0.getRegionVersion());
//    assertEquals(3, tag0.getEntryVersion());
//    assertEquals(3, tag1.getRegionVersion());
//    assertEquals(3, tag1.getEntryVersion());
//
//    // shutdown and recover
//    vm0.invoke(disconnect);
//    vm1.invoke(disconnect);
//    vm1.invoke(createRegion);
//    vm0.invoke(createRegion);
//
//    tag0 = getVersionTag(vm0, key);
//    tag1 = getVersionTag(vm1, key);
//    assertEquals(3, tag0.getRegionVersion());
//    assertEquals(3, tag0.getEntryVersion());
//    assertEquals(3, tag1.getRegionVersion());
//    assertEquals(3, tag1.getEntryVersion());
//
//    vm0.invoke(new SerializableRunnable("put with version 1, value vm1") {
//      public void run() {
//        // clear the region using a test hook so that a new version will be
//        // generated, starting at 1.  This should be rejected by vm1 and not
//        // queued to the WAN by vm0
//        DistributedRegion.LOCALCLEAR_TESTHOOK = true;
//        try {
//          CCRegion.localClear();
//        } finally {
//          DistributedRegion.LOCALCLEAR_TESTHOOK = false;
//        }
//        CCRegion.put(key, "vm0");
//        Assert.assertEquals("vm0", CCRegion.get(key));
//      }
//    });
//    vm1.invoke(new SerializableRunnable("verify that value has not been updated") {
//      public void run() {
//        Assert.assertEquals("dummy", CCRegion.get(key));
//      }
//    });
//
//    tag0 = getVersionTag(vm0, key);
//    tag1 = getVersionTag(vm1, key);
//    assertEquals(4, tag0.getRegionVersion());
//    assertEquals(1, tag0.getEntryVersion());
//    assertEquals(3, tag1.getRegionVersion());
//    assertEquals(3, tag1.getEntryVersion());
//  }
}
