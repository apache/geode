/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import dunit.*;
/**
 * Make sure that create are distributed and done in
 * remote regions that are CACHED_ALL_EVENTS*.
 *
 * @author darrel
 * @since 5.0
 */
public class CachedAllEventsDUnitTest extends CacheTestCase {

//  private transient Region r;
//  private transient DistributedMember otherId;
//  private transient int invokeCount;
  
  public CachedAllEventsDUnitTest(String name) {
    super(name);
  }

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }
    
  private void initOtherId() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("Connect") {
        public void run2() throws CacheException {
          getCache();
        }
      });
    vm.invoke(CachedAllEventsDUnitTest.class, "getVMDistributedMember");
  }
  private void doCreateOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
        public void run2() throws CacheException {
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_ACK);
          Region r1 = createRootRegion("r1", af.create());
          r1.create("key", "value");
        }
      });
  }

  public static DistributedMember getVMDistributedMember() {
    return InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
  
  //////////////////////  Test Methods  //////////////////////

  /**
   * make sure a remote create will be done in a NORMAL+ALL region
   * @param rmtCreate is true if create should happen in remote region
   */
  private void remoteCreate(DataPolicy dp, InterestPolicy ip, boolean rmtCreate) throws CacheException {
    initOtherId();
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(dp);
    af.setSubscriptionAttributes(new SubscriptionAttributes(ip));
    af.setScope(Scope.DISTRIBUTED_ACK);
    Region r1 = createRootRegion("r1", af.create());

    assertEquals(false, r1.containsKey("key"));
    doCreateOtherVm();
    if (rmtCreate) {
      assertEquals(true, r1.containsKey("key"));
      assertEquals("value", r1.getEntry("key").getValue());
    } else {
      assertEquals(false, r1.containsKey("key"));
    }
  }
  // TODO these are never used
  public void testRemoteCreate_CAE() throws CacheException {
    remoteCreate(DataPolicy.NORMAL, InterestPolicy.ALL, true);
  }
  public void testRemoteCreate_CAER() throws CacheException {
    remoteCreate(DataPolicy.REPLICATE, InterestPolicy.CACHE_CONTENT, true);
  }
  public void testRemoteCreate_C() throws CacheException {
    remoteCreate(DataPolicy.NORMAL, InterestPolicy.CACHE_CONTENT, false);
  }
}
