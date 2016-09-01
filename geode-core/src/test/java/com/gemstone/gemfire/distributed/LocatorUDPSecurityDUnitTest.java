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
package com.gemstone.gemfire.distributed;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

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
          com.gemstone.gemfire.test.dunit.Assert.fail("While starting locator on port " + port, ex);
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
