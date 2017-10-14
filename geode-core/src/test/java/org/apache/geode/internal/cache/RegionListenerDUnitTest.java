/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class RegionListenerDUnitTest implements Serializable {

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().disconnectAfter().build();

  @Test
  public void testCleanupFailedInitializationInvoked() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Add RegionListener in both members
    vm0.invoke(() -> installRegionListener());
    vm1.invoke(() -> installRegionListener());

    // Create region in one member
    String regionName = "testCleanupFailedInitializationInvoked";
    vm0.invoke(() -> createRegion(regionName, false));

    // Attempt to create the region with incompatible configuration in another member
    try {
      vm1.invoke(() -> createRegion(regionName, true));
      fail("should not have succeeded in creating region");
    } catch (Exception e) {
      assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
    }

    // Verify the RegionListener cleanupFailedInitialization callback was invoked
    vm1.invoke(() -> verifyRegionListenerCleanupFailedInitializationInvoked());
  }

  private void installRegionListener() {
    this.cacheRule.getCache().addRegionListener(new TestRegionListener());
  }

  private void createRegion(String regionName, boolean addAsyncEventQueueId) {
    RegionFactory rf = this.cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    if (addAsyncEventQueueId) {
      rf.addAsyncEventQueueId("aeqId");
    }
    rf.create(regionName);
  }

  private void verifyRegionListenerCleanupFailedInitializationInvoked() {
    Set<RegionListener> listeners = this.cacheRule.getCache().getRegionListeners();
    assertThat(listeners.size()).isEqualTo(1);
    RegionListener listener = listeners.iterator().next();
    assertThat(listener).isInstanceOf(TestRegionListener.class);
    TestRegionListener trl = (TestRegionListener) listener;
    assertThat(trl.getCleanupFailedInitializationInvoked()).isTrue();
  }

  private static class TestRegionListener implements RegionListener {

    final AtomicBoolean cleanupFailedInitializationInvoked = new AtomicBoolean();

    @Override
    public void cleanupFailedInitialization(Region region) {
      cleanupFailedInitializationInvoked.set(true);
    }

    public boolean getCleanupFailedInitializationInvoked() {
      return this.cleanupFailedInitializationInvoked.get();
    }
  }
}
