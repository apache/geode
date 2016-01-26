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
 * File comment
 */
package com.gemstone.gemfire.cache30;

import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache30.RegionMembershipListenerDUnitTest.MyRML;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author Mitch Thomas
 * @since 6.0
 */
public class PartitionedRegionMembershipListenerDUnitTest extends
    RegionMembershipListenerDUnitTest {
  
  private transient MyRML myPRListener;
  private transient Region prr; // root region

  public PartitionedRegionMembershipListenerDUnitTest(String name) {
    super(name);
  }

  @Override
  protected RegionAttributes createSubRegionAttributes(
      CacheListener[] cacheListeners) {
    AttributesFactory af = new AttributesFactory();
    if (cacheListeners != null) {
      af.initCacheListeners(cacheListeners);
    }
    af.setPartitionAttributes(
        new PartitionAttributesFactory()
        .setTotalNumBuckets(5)
        .setRedundantCopies(0)
        .create());
    return af.create();   
  }

  @Override
  protected List<DistributedMember> assertInitialMembers(DistributedMember otherId) {
    List<DistributedMember> l = super.assertInitialMembers(otherId);
    assertTrue(this.myPRListener.lastOpWasInitialMembers());
    assertEquals(l, this.myPRListener.getInitialMembers());
    return l;
  }

  @Override
  protected void closeRoots() {
    super.closeRoots();
    this.prr.close();
  }

  @Override
  protected void createRootRegionWithListener(final String rName)
      throws CacheException {
    super.createRootRegionWithListener(rName);
    int to = getOpTimeout();
    this.myPRListener = new MyRML(to);
    AttributesFactory af = new AttributesFactory();
    af.initCacheListeners(new CacheListener[]{this.myPRListener});
    af.setPartitionAttributes(
        new PartitionAttributesFactory()
        .setTotalNumBuckets(5)
        .setRedundantCopies(0)
        .create());
    this.prr = createRootRegion(rName + "-pr", af.create());
  }

  @Override
  protected void createRootOtherVm(final String rName) {
    // TODO Auto-generated method stub
    super.createRootOtherVm(rName);
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create PR root") {
      public void run2() throws CacheException {
        AttributesFactory af = new AttributesFactory();
        af.setPartitionAttributes(
            new PartitionAttributesFactory()
            .setTotalNumBuckets(5)
            .setRedundantCopies(0)
            .create());
        createRootRegion(rName + "-pr", af.create());
      }
    });
  }

  @Override
  protected void destroyRootOtherVm(final String rName) {
    // TODO Auto-generated method stub
    super.destroyRootOtherVm(rName);
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("local destroy PR root") {
        public void run2() throws CacheException {
          getRootRegion(rName + "-pr").localDestroyRegion();
        }
      });
  }

  @Override
  protected void closeRootOtherVm(final String rName) {
    super.closeRootOtherVm(rName);
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("close PR root") {
        public void run2() throws CacheException {
          getRootRegion(rName + "-pr").close();
        }
      });
  }

  @Override
  protected void assertOpWasCreate() {
    super.assertOpWasCreate();
    assertTrue(this.myPRListener.lastOpWasCreate());
  }

  @Override
  protected void assertOpWasDeparture() {
    super.assertOpWasDeparture();
    assertTrue(this.myPRListener.lastOpWasDeparture());
    assertEventStuff(this.myPRListener.getLastEvent(), this.otherId, this.prr);
  }
  
  
}
