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
package org.apache.geode.cache30;

import static org.apache.geode.test.dunit.Assert.*;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure that create are distributed and done in
 * remote regions that are CACHED_ALL_EVENTS*.
 *
 * @since GemFire 5.0
 */
@Category(DistributedTest.class)
public class CachedAllEventsDUnitTest extends JUnit4CacheTestCase {

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
    vm.invoke(() -> CachedAllEventsDUnitTest.getVMDistributedMember());
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

  @Test
  public void testRemoteCreate_CAE() throws CacheException {
    remoteCreate(DataPolicy.NORMAL, InterestPolicy.ALL, true);
  }

  @Test
  public void testRemoteCreate_CAER() throws CacheException {
    remoteCreate(DataPolicy.REPLICATE, InterestPolicy.CACHE_CONTENT, true);
  }

  @Test
  public void testRemoteCreate_C() throws CacheException {
    remoteCreate(DataPolicy.NORMAL, InterestPolicy.CACHE_CONTENT, false);
  }
}
