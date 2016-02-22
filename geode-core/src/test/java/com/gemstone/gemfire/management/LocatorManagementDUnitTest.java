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
package com.gemstone.gemfire.management;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;


/**
 * Test cases
 * 
 * DistributedSystem Cache Locator no no yes yes no yes yes yes yes
 * 
 * 
 * @author rishim
 * 
 */

public class LocatorManagementDUnitTest extends ManagementTestBase {

  /** Default file name for locator log: <code>"locator.log"</code> */
  public static final String DEFAULT_LOG_FILE = "locator.log";

  private static final int MAX_WAIT = 8 * ManagementConstants.REFRESH_TIME;

  private static Properties props = new Properties();

  private VM locator;
  

  public LocatorManagementDUnitTest(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1L;

  public void setUp() throws Exception {
    super.setUp();
    locator = managedNode1;
  }

  @Override
  protected final void preTearDownManagementTestBase() throws Exception {
    stopLocator(locator);
  }

  /**
   * When plan is to start Distributed System later so that the system can use
   * this locator
   * 
   * @throws Exception
   */
  public void testPeerLocation() throws Exception {
    int locPort = AvailablePortHelper.getRandomAvailableTCPPort();
    startLocator(locator, true, locPort);
    locatorMBeanExist(locator, locPort, true);

    Host host = Host.getHost(0);
    String host0 = getServerHostName(host);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, host0 + "[" + locPort
        + "]");
    props.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    props.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "false");
    props.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, "0");
    props.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME, "0");
    createCache(managingNode, props);
    startManagingNode(managingNode);
    DistributedMember locatorMember = getMember(locator);
    remoteLocatorMBeanExist(managingNode,locatorMember);

  }

  /**
   * Tests a locator which is co-located with already existing cache
   * 
   * @throws Exception
   */
  public void testColocatedLocator() throws Exception {
    initManagement(false);
    int locPort = AvailablePortHelper.getRandomAvailableTCPPort();
    startLocator(locator, false, locPort);
    locatorMBeanExist(locator, locPort, false);

  }

  public void testListManagers() throws Exception {
    initManagement(false);
    int locPort = AvailablePortHelper.getRandomAvailableTCPPort();
    startLocator(locator, false, locPort);
    listManagers(locator, locPort, false);
  }

  public void testWillingManagers() throws Exception {
    int locPort = AvailablePortHelper.getRandomAvailableTCPPort();
    startLocator(locator, true, locPort);

    Host host = Host.getHost(0);
    String host0 = getServerHostName(host);
    
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, host0 + "[" + locPort
        + "]");
    props.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");

    createCache(managedNode2, props);
    createCache(managedNode3, props);

    listWillingManagers(locator, locPort, false);
  }


  /**
   * Starts a locator with given configuration.
   * If DS is already started it will use the same DS
   * 
   * @param vm
   *          reference to VM
   */
  protected String startLocator(final VM vm, final boolean isPeer,
      final int port) {

    return (String) vm.invoke(new SerializableCallable("Start Locator In VM") {

      public Object call() throws Exception {

        assertFalse(InternalLocator.hasLocator());

        Properties props = new Properties();
        props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
        
        props.setProperty(DistributionConfig.LOCATORS_NAME, "");
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());

        InetAddress bindAddr = null;
        try {
          bindAddr = InetAddress.getByName(getServerHostName(vm.getHost()));
        } catch (UnknownHostException uhe) {
          Assert.fail("While resolving bind address ", uhe);
        }

        try {
          File logFile = new File(getTestMethodName() + "-locator" + port + ".log");
          Locator locator = Locator.startLocatorAndDS(port, logFile, bindAddr,
              props, isPeer, true, null);
        } catch (IOException ex) {
          Assert.fail("While starting locator on port " + port, ex);
        }

        assertTrue(InternalLocator.hasLocator());
        return null;
      }
    });
  }

  /**
   * Creates a persistent region
   * 
   * @param vm
   *          reference to VM
   */
  protected String stopLocator(VM vm) {

    return (String) vm.invoke(new SerializableCallable("Stop Locator In VM") {

      public Object call() throws Exception {

        assertTrue(InternalLocator.hasLocator());
        InternalLocator.getLocator().stop();
        return null;
      }
    });
  }

  /**
   * Creates a persistent region
   * 
   * @param vm
   *          reference to VM
   */
  protected void locatorMBeanExist(VM vm, final int locPort,
      final boolean isPeer) {

    vm.invoke(new SerializableCallable("Locator MBean created") {

      public Object call() throws Exception {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

        ManagementService service = ManagementService
            .getExistingManagementService(cache);
        assertNotNull(service);
        LocatorMXBean bean = service.getLocalLocatorMXBean();
        assertNotNull(bean);
        assertEquals(locPort, bean.getPort());
        LogWriterUtils.getLogWriter().info("Log of Locator" + bean.viewLog());
        LogWriterUtils.getLogWriter().info("BindAddress" + bean.getBindAddress());
        assertEquals(isPeer, bean.isPeerLocator());
        return null;
      }
    });
  }

  /**
   * Creates a persistent region
   * 
   * @param vm
   *          reference to VM
   */
  protected void remoteLocatorMBeanExist(VM vm, final DistributedMember member) {

    vm.invoke(new SerializableCallable("Locator MBean created") {

      public Object call() throws Exception {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getExistingManagementService(cache);
        assertNotNull(service);
        LocatorMXBean bean = MBeanUtil.getLocatorMbeanProxy(member);
        assertNotNull(bean);

        LogWriterUtils.getLogWriter().info("Log of Locator" + bean.viewLog());
        LogWriterUtils.getLogWriter().info("BindAddress" + bean.getBindAddress());

        return null;
      }
    });
  }

  /**
   * Creates a persistent region
   * 
   * @param vm
   *          reference to VM
   */
  protected void listManagers(VM vm, final int locPort, final boolean isPeer) {

    vm.invoke(new SerializableCallable("List Managers") {

      public Object call() throws Exception {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

        ManagementService service = ManagementService
            .getExistingManagementService(cache);
        assertNotNull(service);
        final LocatorMXBean bean = service.getLocalLocatorMXBean();
        assertNotNull(bean);

        Wait.waitForCriterion(new WaitCriterion() {

          public String description() {
            return "Waiting for the managers List";
          }

          public boolean done() {

            boolean done = bean.listManagers().length == 1;
            return done;
          }

        }, MAX_WAIT, 500, true);

        return null;
      }
    });
  }

  /**
   * Creates a persistent region
   * 
   * @param vm
   *          reference to VM
   */
  protected void listWillingManagers(VM vm, final int locPort,
      final boolean isPeer) {

    vm.invoke(new SerializableCallable("List Willing Managers") {

      public Object call() throws Exception {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

        ManagementService service = ManagementService
            .getExistingManagementService(cache);
        assertNotNull(service);
        final LocatorMXBean bean = service.getLocalLocatorMXBean();
        assertNotNull(bean);

        Wait.waitForCriterion(new WaitCriterion() {

          public String description() {
            return "Waiting for the Willing managers List";
          }

          public boolean done() {

            boolean done = bean.listPotentialManagers().length == 3;
            return done;
          }

        }, MAX_WAIT, 500, true);

        return null;
      }
    });
  }
  
  /** get the host name to use for a server cache in client/server dunit
   * testing
   * @param host
   * @return the host name
   */
  public static String getServerHostName(Host host) {
    return System.getProperty("gemfire.server-bind-address") != null?
        System.getProperty("gemfire.server-bind-address")
        : host.getHostName();
  }

}
