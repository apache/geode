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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LocatorJUnitTest {

  /**
   *
   */
  private static final int REQUEST_TIMEOUT = 5 * 1000;
  private Locator locator;
  private int port;
  private File tmpFile;

  @Before
  public void setUp() throws IOException {
    tmpFile = File.createTempFile("locator", ".log");
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File locatorFile = new File("locator"+port+".dat");
    if (locatorFile.exists()) {
      locatorFile.delete();
    }
  }

  @After
  public void tearDown() {
    if(locator != null) {
      locator.stop();
    }
    Assert.assertEquals(false, Locator.hasLocator());
  }

  @Test
  public void testBug45804() throws Exception {
    Properties dsprops = new Properties();
    int jmxPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    dsprops.setProperty("mcast-port", "0");
    dsprops.setProperty("locators", "localhost[" + port + "]");
    dsprops.setProperty("jmx-manager-port", ""+jmxPort);
    dsprops.setProperty("jmx-manager-start", "true");
    dsprops.setProperty("jmx-manager-http-port", "0");
    dsprops.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    System.setProperty("gemfire.disableManagement", "false"); // not needed
    try {
      locator = Locator.startLocatorAndDS(port, new File("testJmxManager.log"), dsprops);
      List<JmxManagerProfile> alreadyManaging = GemFireCacheImpl.getInstance().getJmxManagerAdvisor().adviseAlreadyManaging();
      assertEquals(1, alreadyManaging.size());
      assertEquals(GemFireCacheImpl.getInstance().getMyId(), alreadyManaging.get(0).getDistributedMember());
    } finally {
      System.clearProperty("gemfire.enabledManagement");
    }
  }

  @Test
  public void testBasicInfo() throws Exception {
    locator = Locator.startLocator(port, tmpFile);
    Assert.assertTrue(locator.isPeerLocator());
    Assert.assertFalse(locator.isServerLocator());
    String[] info = InternalLocator.getLocatorInfo(InetAddress.getLocalHost(), port);
    Assert.assertNotNull(info);
    Assert.assertTrue(info.length > 1);
  }

  @Test
  public void testNoThreadLeftBehind() throws Exception {
    Properties dsprops = new Properties();
    dsprops.setProperty("mcast-port", "0");
    dsprops.setProperty("locators", "localhost[" + port + "]");
    dsprops.setProperty("jmx-manager-start", "false");
    dsprops.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    JGroupsMessenger.THROW_EXCEPTION_ON_START_HOOK = true;
    int threadCount = Thread.activeCount();
    try {
      locator = Locator.startLocatorAndDS(port, new File(""), dsprops);
      locator.stop();
      fail("expected an exception");
    } catch (SystemConnectException e) {
      
      for (int i=0; i<10; i++) {
        if (threadCount < Thread.activeCount()) {
          Thread.sleep(1000);
        }
      }
      if (threadCount < Thread.activeCount()) {
          OSProcess.printStacks(0);
          fail("expected " + threadCount + " threads or fewer but found " + Thread.activeCount()
              +".  Check log file for a thread dump.");
        }
    }
  }

  @Test
  public void testServerOnly() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    locator = Locator.startLocatorAndDS(port, tmpFile, null, props, false, true, null);
    Assert.assertFalse(locator.isPeerLocator());
    Assert.assertTrue(locator.isServerLocator());
    Thread.sleep(1000);
    doServerLocation();
  }

  @Test
  public void testBothPeerAndServer() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME , getGemFireLogLevel());
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");

    locator = Locator.startLocatorAndDS(port, tmpFile, null, props);
    Assert.assertTrue(locator.isPeerLocator());
    Assert.assertTrue(locator.isServerLocator());
    Thread.sleep(1000);
    doServerLocation();
    locator.stop();
  }

  /**
   * Make sure two ServerLocation objects on different hosts but with the same port
   * are not equal
   */
  @Test
  public void testBug42040() {
    ServerLocation sl1 = new ServerLocation("host1", 777);
    ServerLocation sl2 = new ServerLocation("host2", 777);
    if (sl1.equals(sl2)) {
      fail("ServerLocation instances on different hosts should not test equal");
    }
  }

  private void doServerLocation() throws Exception {
    {
      ClientConnectionRequest request = new ClientConnectionRequest(Collections.EMPTY_SET, "group1");
      ClientConnectionResponse response = (ClientConnectionResponse) TcpClient.requestToServer(InetAddress.getLocalHost(), port, request, REQUEST_TIMEOUT);
      Assert.assertEquals(null, response.getServer());
    }

    {
      QueueConnectionRequest request = new QueueConnectionRequest(ClientProxyMembershipID.getNewProxyMembership(InternalDistributedSystem.getAnyInstance()), 3, Collections.EMPTY_SET, "group1",true);
      QueueConnectionResponse response = (QueueConnectionResponse) TcpClient.requestToServer(InetAddress.getLocalHost(), port, request, REQUEST_TIMEOUT);
      Assert.assertEquals(new ArrayList(), response.getServers());
      Assert.assertFalse(response.isDurableQueueFound());
    }
  }

}
