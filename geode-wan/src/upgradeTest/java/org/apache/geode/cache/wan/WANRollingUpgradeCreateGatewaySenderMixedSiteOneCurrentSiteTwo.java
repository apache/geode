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
package org.apache.geode.cache.wan;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.version.VersionManager;

public class WANRollingUpgradeCreateGatewaySenderMixedSiteOneCurrentSiteTwo
    extends WANRollingUpgradeDUnitTest {

  @Test
  public void CreateGatewaySenderMixedSiteOneCurrentSiteTwo() throws Exception {
    final Host host = Host.getHost(0);

    // Get mixed site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);

    // Get current site members
    VM site2Locator = host.getVM(VersionManager.CURRENT_VERSION, 4);
    VM site2Server1 = host.getVM(VersionManager.CURRENT_VERSION, 5);
    VM site2Server2 = host.getVM(VersionManager.CURRENT_VERSION, 6);

    int[] locatorPorts = getRandomAvailableTCPPorts(2);
    // Get mixed site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = locatorPorts[0];
    site1Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort));
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get current site locator properties
    final int site2LocatorPort = locatorPorts[1];
    site2Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort));
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start mixed site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Locators before 1.4 handled configuration asynchronously.
    // We must wait for configuration configuration to be ready, or confirm that it is disabled.
    site1Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));

    // Start current site locator
    site2Locator.invoke(
        () -> await().untilAsserted(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
            site2Locators, site1Locators)));

    // Start current site servers with receivers
    site2Server1.invoke(() -> createCache(site2Locators));
    site2Server1.invoke(() -> createGatewayReceiver());
    site2Server2.invoke(() -> createCache(site2Locators));
    site2Server2.invoke(() -> createGatewayReceiver());

    // Start mixed site servers
    site1Server1.invoke(() -> createCache(site1Locators));
    site1Server2.invoke(() -> createCache(site1Locators));

    // Roll mixed site locator to current with jmx manager
    site1Locator.invoke(() -> stopLocator());
    VM site1RolledLocator = host.getVM(VersionManager.CURRENT_VERSION, site1Locator.getId());
    int jmxManagerPort =
        site1RolledLocator.invoke(() -> startLocatorWithJmxManager(site1LocatorPort,
            site1DistributedSystemId, site1Locators, site2Locators));

    // Roll one mixed site server to current
    site1Server2.invoke(() -> closeCache());
    VM site1Server2RolledServer = host.getVM(VersionManager.CURRENT_VERSION, site1Server2.getId());
    site1Server2RolledServer.invoke(() -> createCache(site1Locators));

    // Use gfsh to attempt to create a gateway sender in the mixed site servers
    this.gfsh.connectAndVerify(jmxManagerPort, GfshCommandRule.PortType.jmxManager);
    CommandResultAssert cmd = this.gfsh
        .executeAndAssertThat(getCreateGatewaySenderCommand("toSite2", site2DistributedSystemId));
    if (!majorMinor(oldVersion).equals(majorMinor(KnownVersion.CURRENT.getName()))) {
      cmd.statusIsError()
          .containsOutput(CliStrings.CREATE_GATEWAYSENDER__MSG__CAN_NOT_CREATE_DIFFERENT_VERSIONS);
    } else {
      // generally serialization version is unchanged between patch releases
      cmd.statusIsSuccess();
    }
  }

  @Test
  public void CreateGatewaySenderMixedSiteOneCurrentSiteOtherOld() throws Exception {
    final Host host = Host.getHost(0);

    // Get mixed site members
    VM site1Locator = host.getVM(oldVersion, 0);
    VM site1Server1 = host.getVM(oldVersion, 1);
    VM site1Server2 = host.getVM(oldVersion, 2);

    // Get current site members
    VM site2Locator = host.getVM(VersionManager.CURRENT_VERSION, 4);
    VM site2Server1 = host.getVM(VersionManager.CURRENT_VERSION, 5);
    VM site2Server2 = host.getVM(VersionManager.CURRENT_VERSION, 6);

    int[] locatorPorts = getRandomAvailableTCPPorts(2);
    // Get mixed site locator properties
    String hostName = NetworkUtils.getServerHostName(host);
    final int site1LocatorPort = locatorPorts[0];
    site1Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort));
    final String site1Locators = hostName + "[" + site1LocatorPort + "]";
    final int site1DistributedSystemId = 0;

    // Get current site locator properties
    final int site2LocatorPort = locatorPorts[1];
    site2Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort));
    final String site2Locators = hostName + "[" + site2LocatorPort + "]";
    final int site2DistributedSystemId = 1;

    // Start mixed site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Locators before 1.4 handled configuration asynchronously.
    // We must wait for configuration configuration to be ready, or confirm that it is disabled.
    site1Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));

    // Start current site locator
    site2Locator.invoke(
        () -> await().untilAsserted(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
            site2Locators, site1Locators)));

    // Start current site servers with receivers
    site2Server1.invoke(() -> createCache(site2Locators));
    site2Server1.invoke(() -> createGatewayReceiver());
    site2Server2.invoke(() -> createCache(site2Locators));
    site2Server2.invoke(() -> createGatewayReceiver());

    // Start mixed site servers
    site1Server1.invoke(() -> createCache(site1Locators));
    site1Server2.invoke(() -> createCache(site1Locators));

    // Roll mixed site locator to current with jmx manager
    site1Locator.invoke(() -> stopLocator());
    VM site1RolledLocator = host.getVM(VersionManager.CURRENT_VERSION, site1Locator.getId());
    int jmxManagerPort =
        site1RolledLocator.invoke(() -> startLocatorWithJmxManager(site1LocatorPort,
            site1DistributedSystemId, site1Locators, site2Locators));

    // Roll one mixed site server to current
    site1Server2.invoke(() -> closeCache());
    VM site1Server2RolledServer = host.getVM(VersionManager.CURRENT_VERSION, site1Server2.getId());
    site1Server2RolledServer.invoke(() -> createCache(site1Locators));

    // Use gfsh to attempt to create a gateway sender in the mixed site servers
    this.gfsh.connectAndVerify(jmxManagerPort, GfshCommandRule.PortType.jmxManager);
    CommandResultAssert cmd = this.gfsh
        .executeAndAssertThat(
            getCreateGatewaySenderCommandWithOptions("toSite2", site2DistributedSystemId));
    if (!majorMinor(oldVersion).equals(majorMinor(KnownVersion.CURRENT.getName()))) {
      cmd.statusIsError()
          .containsOutput(CliStrings.CREATE_GATEWAYSENDER__MSG__CAN_NOT_CREATE_DIFFERENT_VERSIONS);
    } else {
      // generally serialization version is unchanged between patch releases
      cmd.statusIsSuccess();
    }
  }


  /**
   * returns the major.minor prefix of a semver
   */
  private static String majorMinor(String version) {
    String[] parts = version.split("\\.");
    Assertions.assertThat(parts.length).isGreaterThanOrEqualTo(2);
    return parts[0] + "." + parts[1];
  }
}
