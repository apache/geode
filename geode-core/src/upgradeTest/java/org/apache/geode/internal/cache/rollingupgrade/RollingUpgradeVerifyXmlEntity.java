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
package org.apache.geode.internal.cache.rollingupgrade;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.Version;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.VersionManager;

public class RollingUpgradeVerifyXmlEntity extends RollingUpgrade2DUnitTestBase {

  @Test
  // This test verifies that an XmlEntity created in the current version serializes properly to
  // previous versions and vice versa.
  public void testVerifyXmlEntity() {
    final Host host = Host.getHost(0);
    VM oldLocator = host.getVM(oldVersion, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 3);

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName = NetworkUtils.getServerHostName();
    String locatorsString = getLocatorString(locatorPorts);
    DistributedTestUtils.deleteLocatorStateFile(locatorPorts);

    try {
      // Start locator
      oldLocator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorsString, false), true));

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      // Start servers
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), oldServer,
          currentServer1, currentServer2);
      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));

      // Get DistributedMembers of the servers
      DistributedMember oldServerMember = oldServer.invoke(() -> getDistributedMember());
      DistributedMember currentServer1Member = currentServer1.invoke(() -> getDistributedMember());
      DistributedMember currentServer2Member = currentServer2.invoke(() -> getDistributedMember());

      // Register function in all servers
      Function function = new GetDataSerializableFunction();
      invokeRunnableInVMs(invokeRegisterFunction(function), oldServer, currentServer1,
          currentServer2);

      // Execute the function in the old server against the other servers to verify the
      // DataSerializable can be serialized from a newer server to an older one.
      oldServer.invoke(() -> executeFunctionAndVerify(function.getId(),
          "org.apache.geode.management.internal.configuration.domain.XmlEntity",
          currentServer1Member, currentServer2Member));

      // Execute the function in a new server against the other servers to verify the
      // DataSerializable can be serialized from an older server to a newer one.
      currentServer1.invoke(() -> executeFunctionAndVerify(function.getId(),
          "org.apache.geode.management.internal.configuration.domain.XmlEntity", oldServerMember,
          currentServer2Member));
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), oldLocator);
      invokeRunnableInVMs(true, invokeCloseCache(), oldServer, currentServer1, currentServer2);
    }
  }

}
