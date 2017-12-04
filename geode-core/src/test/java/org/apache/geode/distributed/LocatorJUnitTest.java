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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATOR_WAIT_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.IntSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({IntegrationTest.class, MembershipTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorJUnitTest {

  private static final int REQUEST_TIMEOUT = 5 * 1000;

  private Locator locator;
  private File tmpFile;
  private int port;

  @Parameterized.Parameters
  public static Collection<Object> data() {
    return Arrays.asList(new Object[] {(IntSupplier) () -> 0,
        (IntSupplier) () -> AvailablePortHelper.getRandomAvailableTCPPort()});
  }

  @Parameterized.Parameter
  public IntSupplier portSupplier;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    tmpFile = File.createTempFile("locator", ".log");
    this.port = portSupplier.getAsInt();
    File locatorFile = new File("locator" + this.port + ".dat");
    if (locatorFile.exists()) {
      locatorFile.delete();
    }
  }

  @After
  public void tearDown() {
    if (locator != null) {
      locator.stop();
    }
    assertEquals(false, Locator.hasLocator());
  }

  /**
   * TRAC #45804: if jmx-manager-start is true in a locator then gfsh connect will fail
   */
  @Test
  public void testGfshConnectShouldSucceedIfJmxManagerStartIsTrueInLocator() throws Exception {
    Properties dsprops = new Properties();
    int jmxPort = getRandomAvailablePort(SOCKET);
    dsprops.setProperty(MCAST_PORT, "0");
    dsprops.setProperty(JMX_MANAGER_PORT, "" + jmxPort);
    dsprops.setProperty(JMX_MANAGER_START, "true");
    dsprops.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    dsprops.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    dsprops.setProperty(LOG_FILE, "");
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "disableManagement", "false"); // not
                                                                                          // needed
    try {
      locator = Locator.startLocatorAndDS(port, null, dsprops);
      List<JmxManagerProfile> alreadyManaging =
          GemFireCacheImpl.getInstance().getJmxManagerAdvisor().adviseAlreadyManaging();
      assertEquals(1, alreadyManaging.size());
      assertEquals(GemFireCacheImpl.getInstance().getMyId(),
          alreadyManaging.get(0).getDistributedMember());
    } finally {
      System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "enabledManagement");
    }
  }


  @Test
  public void testHandlersAreWaitedOn() throws Exception {
    Properties dsprops = new Properties();
    int jmxPort = getRandomAvailablePort(SOCKET);
    dsprops.setProperty(MCAST_PORT, "0");
    dsprops.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    dsprops.setProperty(LOCATOR_WAIT_TIME, "1"); // seconds
    dsprops.setProperty(LOG_FILE, "");
    locator = Locator.startLocatorAndDS(port, null, dsprops);
    InternalLocator internalLocator = (InternalLocator) locator;
    // the locator should always install a SharedConfigurationStatusRequest handler
    assertTrue(internalLocator.hasHandlerForClass(SharedConfigurationStatusRequest.class));
  }


  @Test
  public void testBasicInfo() throws Exception {
    locator = Locator.startLocator(port, tmpFile);
    int boundPort = (port == 0) ? locator.getPort() : port;
    TcpClient client = new TcpClient();
    String[] info = client.getInfo(InetAddress.getLocalHost(), boundPort);
    assertNotNull(info);
    assertTrue(info.length > 1);
  }

  @Test
  public void testNoThreadLeftBehind() throws Exception {
    Properties dsprops = new Properties();
    dsprops.setProperty(MCAST_PORT, "0");
    dsprops.setProperty(JMX_MANAGER_START, "false");
    dsprops.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    JGroupsMessenger.THROW_EXCEPTION_ON_START_HOOK = true;
    int threadCount = Thread.activeCount();
    try {
      locator = Locator.startLocatorAndDS(port, new File(""), dsprops);
      locator.stop();
      fail("expected an exception");
    } catch (SystemConnectException expected) {

      for (int i = 0; i < 10; i++) {
        if (threadCount < Thread.activeCount()) {
          Thread.sleep(1000);
        }
      }
      if (threadCount < Thread.activeCount()) {
        OSProcess.printStacks(0);
        fail("expected " + threadCount + " threads or fewer but found " + Thread.activeCount()
            + ".  Check log file for a thread dump.");
      }
    } finally {
      JGroupsMessenger.THROW_EXCEPTION_ON_START_HOOK = false;
    }
  }

  /**
   * Make sure two ServerLocation objects on different hosts but with the same port are not equal
   * <p/>
   * TRAC #42040: LoadBalancing directs all traffic to a single cache server if all servers are
   * started on the same port
   */
  @Test
  public void testServerLocationOnDifferentHostsShouldNotTestEqual() {
    ServerLocation sl1 = new ServerLocation("host1", 777);
    ServerLocation sl2 = new ServerLocation("host2", 777);
    if (sl1.equals(sl2)) {
      fail("ServerLocation instances on different hosts should not test equal");
    }
  }

}
