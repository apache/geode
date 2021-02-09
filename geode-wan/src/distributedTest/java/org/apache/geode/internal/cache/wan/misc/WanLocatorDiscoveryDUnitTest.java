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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.version.VersionManager;

public class WanLocatorDiscoveryDUnitTest {

  private static MemberVM locator_ln1;
  private static MemberVM locator_ln2;

  private static MemberVM locator_ny1;
  private static MemberVM locator_ny2;
  private int[] ports;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setupCluster() throws Exception {
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("could not get remote locator information");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  private void setupWanSites() throws IOException {
    ports = AvailablePortHelper.getRandomAvailableTCPPorts(5);
    int site1Port =
        setupWanSite1();
    setupWanSite2(site1Port);
  }

  private int setupWanSite1() throws IOException {
    locator_ln1 = cluster.startLocatorVM(0, ports[0], VersionManager.CURRENT_VERSION,
        i -> i.withProperty(DISTRIBUTED_SYSTEM_ID, "1"));

    int locator_ln1_port = locator_ln1.getPort();

    locator_ln2 = cluster.startLocatorVM(1, ports[1], VersionManager.CURRENT_VERSION,
        i -> i.withProperty(DISTRIBUTED_SYSTEM_ID, "1")
            .withConnectionToLocator(locator_ln1_port));

    return locator_ln1_port;
  }

  private void setupWanSite2(int site1Port) throws IOException {
    locator_ny1 = cluster.startLocatorVM(2, ports[2], VersionManager.CURRENT_VERSION,
        i -> i.withProperty(DISTRIBUTED_SYSTEM_ID, "2")
            .withProperty(REMOTE_LOCATORS, "localhost[" + site1Port + "]"));

    int locator_ny1_port = locator_ny1.getPort();

    locator_ny2 = cluster.startLocatorVM(3, ports[3], VersionManager.CURRENT_VERSION,
        i -> i.withProperty(DISTRIBUTED_SYSTEM_ID, "2")
            .withProperty(REMOTE_LOCATORS, "localhost[" + site1Port + "]")
            .withConnectionToLocator(locator_ny1_port));
  }

  @Test
  public void testLocatorList() throws Exception {
    setupWanSites();
    locator_ny1.invoke(() -> {
      LocatorMembershipListener listener =
          ClusterStartupRule.getLocator().getLocatorMembershipListener();
      GeodeAwaitility.await()
          .untilAsserted(() -> assertThat(listener.getAllLocatorsInfo().size()).isEqualTo(2));
      for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : listener.getAllLocatorsInfo()
          .entrySet()) {
        GeodeAwaitility.await()
            .untilAsserted(() -> assertThat(entry.getValue().size()).isEqualTo(2));
      }

    });

    locator_ln2.stop();

    int locator_ln1_port = locator_ln1.getPort();

    locator_ln2 = cluster.startLocatorVM(1, ports[4], VersionManager.CURRENT_VERSION,
        i -> i.withProperty(DISTRIBUTED_SYSTEM_ID, "1")
            .withConnectionToLocator(locator_ln1_port));

    locator_ny2.invoke(() -> {
      LocatorMembershipListener listener =
          ClusterStartupRule.getLocator().getLocatorMembershipListener();
      GeodeAwaitility.await()
          .untilAsserted(() -> assertThat(listener.getAllLocatorsInfo().size()).isEqualTo(2));

      for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : listener.getAllLocatorsInfo()
          .entrySet()) {
        GeodeAwaitility.await()
            .untilAsserted(() -> assertThat(entry.getValue().size()).isEqualTo(2));
      }

    });
  }

}
