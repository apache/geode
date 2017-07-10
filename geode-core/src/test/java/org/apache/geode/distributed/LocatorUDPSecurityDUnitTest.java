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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_UDP_DHALGO;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({DistributedTest.class, MembershipTest.class, FlakyTest.class}) // Flaky: GEODE-2542
public class LocatorUDPSecurityDUnitTest extends LocatorDUnitTest {

  public LocatorUDPSecurityDUnitTest() {}

  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }

  @Override
  @Test
  @Ignore // GEODE-3094
  public void testMultipleLocatorsRestartingAtSameTimeWithMissingServers() throws Exception {
    super.testMultipleLocatorsRestartingAtSameTimeWithMissingServers();
  }

  @Test
  public void testLocatorWithUDPSecurityButServer() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";

    vm0.invoke("Start locator " + locators, () -> startLocator(port));
    try {

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(MEMBER_TIMEOUT, "1000");
      // addDSProps(props);
      system = (InternalDistributedSystem) DistributedSystem.connect(props);

    } catch (GemFireConfigException gce) {
      Assert.assertTrue(gce.getMessage().contains("Rejecting findCoordinatorRequest"));
    } finally {
      vm0.invoke(() -> stopLocator());
    }
  }
}
