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

import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class RegionListenerDistributedTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    invokeInEveryVM(() -> cacheRule.createCache());
  }

  @Test
  public void testCleanupFailedInitializationInvoked() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Add RegionListener in both members
    vm0.invoke(() -> installRegionListener());
    vm1.invoke(() -> installRegionListener());

    // Create region in one member
    String regionName = "testCleanupFailedInitializationInvoked";
    vm0.invoke(() -> createRegion(regionName, false));

    // Attempt to create the region with incompatible configuration in another member. Verify that
    // it throws an IllegalStateException.
    vm1.invoke(() -> createRegion(regionName, IllegalStateException.class));

    // Verify the RegionListener cleanupFailedInitialization callback was invoked
    vm1.invoke(() -> verifyRegionListenerCleanupFailedInitializationInvoked());
  }

  private void installRegionListener() {
    cacheRule.getCache().addRegionListener(new TestRegionListener());
  }

  private void createRegion(String regionName, boolean addAsyncEventQueueId) {
    RegionFactory rf = cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    if (addAsyncEventQueueId) {
      rf.addAsyncEventQueueId("aeqId");
    }
    rf.create(regionName);
  }

  private void createRegion(String regionName, Class exception) {
    RegionFactory rf = cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE)
        .addAsyncEventQueueId("aeqId");
    assertThatThrownBy(() -> rf.create(regionName)).isInstanceOf(exception);
  }

  private void verifyRegionListenerCleanupFailedInitializationInvoked() {
    Set<RegionListener> listeners = cacheRule.getCache().getRegionListeners();
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
      return cleanupFailedInitializationInvoked.get();
    }
  }
}
