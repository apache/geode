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

package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DistributionAdvisorIntegrationTest {

  @Rule
  // The embedded locator causes a ClusterDistributionManager to be created rather than a
  // LonerDistributionManager which is necessary for this test. Disabling auto-reconnect prevents
  // the DistributedSystem from being recreated and causing issues with repeatIntegrationTest
  public ServerStarterRule server =
      new ServerStarterRule().withProperty(DISABLE_AUTO_RECONNECT, "true").withNoCacheServer()
          .withEmbeddedLocator().withAutoStart();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void verifyMembershipListenerIsRemovedAfterForceDisconnect() {
    // Create a region
    DistributionAdvisee region = (DistributionAdvisee) server.createRegion(RegionShortcut.REPLICATE,
        testName.getMethodName());

    // Get the DistributionManager and MembershipListener before force disconnecting the server
    DistributionManager manager = region.getDistributionManager();
    MembershipListener listener = region.getDistributionAdvisor().getMembershipListener();

    // Verify the MembershipListener is added to the DistributionManager
    assertThat(manager.getMembershipListeners().contains(listener)).isTrue();

    // Force disconnect the server
    server.forceDisconnectMember();

    // Verify the MembershipListener is removed from the DistributionManager
    await().untilAsserted(
        () -> assertThat(manager.getMembershipListeners().contains(listener)).isFalse());
  }
}
