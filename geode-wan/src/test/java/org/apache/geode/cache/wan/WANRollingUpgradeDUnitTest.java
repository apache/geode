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

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@Category({DistributedTest.class, BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class WANRollingUpgradeDUnitTest extends JUnit4CacheTestCase {

  @Parameterized.Parameters
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    } else {
      System.out.println("running against these versions: " + result);
    }
    return result;
  }

  // the old version of Geode we're testing against
  private String oldVersion;

  public WANRollingUpgradeDUnitTest(String version) {
    oldVersion = version;
  }

  @Test
  // This test verifies that a GatewaySenderProfile serializes properly between versions.
  public void testVerifyGatewaySenderProfile() throws Exception {
    doTestVerifyGatewaySenderProfile(oldVersion);
  }

  private void doTestVerifyGatewaySenderProfile(String oldVersion) throws Exception {
    final Host host = Host.getHost(0);
    VM oldLocator = host.getVM(oldVersion, 0);
    VM oldServer = host.getVM(oldVersion, 1);
    VM currentServer = host.getVM(2);

    // Start locator
    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";
    oldLocator.invoke(() -> startLocator(port, locators));

    IgnoredException ie =
        IgnoredException.addIgnoredException("could not get remote locator information");
    try {
      // Start old server
      oldServer.invoke(() -> startServer(locators));

      // Create GatewaySender in old server
      oldServer.invoke(() -> createGatewaySender());

      // Start current server
      currentServer.invoke(() -> startServer(locators));

      // Attempt to create GatewaySender in new server
      currentServer.invoke(() -> createGatewaySender());
    } finally {
      ie.remove();
    }
  }

  private void startLocator(int port, String locators) throws IOException {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    Locator.startLocatorAndDS(port, null, props);
  }

  private void startServer(String locators) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, DUnitLauncher.logLevel);
    getCache(props);
  }

  protected void createGatewaySender() throws Exception {
    GatewaySenderFactory gsf = getCache().createGatewaySenderFactory();
    gsf.setParallel(true);
    gsf.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
    gsf.create(getName() + "_gatewaysender", 10);
  }
}
