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

import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_UDP_DHALGO;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class LocatorUDPSecurityDUnitTest extends LocatorDUnitTest {
  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }

  @Test
  public void testLocatorWithUDPSecurityButServer() {
    disconnectAllFromDS();
    VM vm = VM.getVM(0);

    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName() + "[" + port + "]";

    startLocatorWithSomeBasicProperties(vm, port);

    try {
      Properties props = getBasicProperties(locators);
      props.setProperty(MEMBER_TIMEOUT, "1000");
      system = getConnectedDistributedSystem(props);

    } catch (GemFireConfigException gce) {
      Assert.assertTrue(gce.getMessage().contains("Rejecting findCoordinatorRequest"));
    } finally {
      vm.invoke(LocatorDUnitTest::stopLocator);
    }
  }
}
