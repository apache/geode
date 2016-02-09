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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

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
