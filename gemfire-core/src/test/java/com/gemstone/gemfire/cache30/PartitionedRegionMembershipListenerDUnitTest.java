/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

import dunit.VM;

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
