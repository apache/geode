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
package org.apache.geode.distributed;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

import static org.apache.geode.distributed.ConfigurationProperties.*;

public class LocatorUDPSecurityDUnitTest extends LocatorDUnitTest{

  public LocatorUDPSecurityDUnitTest() {
  }
  
  @Test
  public void testLoop() throws Exception {
    for(int i=0; i < 1; i++) {
      testMultipleLocatorsRestartingAtSameTime();
      tearDown();
      setUp();
    }
  }
  
  @Override
  protected void addDSProps(Properties p) {
    p.setProperty(SECURITY_UDP_DHALGO, "AES:128");
  }
  
  @Test
  public void testLocatorWithUDPSecurityButServer() throws Exception {
    disconnectAllFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final int port =
        AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(port1);
    final String locators = NetworkUtils.getServerHostName(host) + "[" + port + "]";
    final String uniqueName = getUniqueName();

    vm0.invoke(new SerializableRunnable("Start locator " + locators) {
      public void run() {
        File logFile = new File("");
        try {
          Properties locProps = new Properties();
          locProps.setProperty(MCAST_PORT, "0");
          locProps.setProperty(MEMBER_TIMEOUT, "1000");
          locProps.put(ENABLE_CLUSTER_CONFIGURATION, "false");

          addDSProps(locProps);  
          Locator.startLocatorAndDS(port, logFile, locProps);
        } catch (IOException ex) {
          org.apache.geode.test.dunit.Assert.fail("While starting locator on port " + port, ex);
        }
      }
    });
    try {

      Properties props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, locators);
      props.setProperty(MEMBER_TIMEOUT, "1000");
     // addDSProps(props);
      system = (InternalDistributedSystem) DistributedSystem.connect(props);
      
    } catch(GemFireConfigException gce){
      Assert.assertTrue(gce.getMessage().contains("Rejecting findCoordinatorRequest"));
    } finally {
      vm0.invoke(getStopLocatorRunnable());
    }
  }
}
