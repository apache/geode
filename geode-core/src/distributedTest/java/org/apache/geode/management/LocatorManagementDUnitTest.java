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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.management.MXBeanAwaitility.awaitLocalLocatorMXBean;
import static org.apache.geode.management.MXBeanAwaitility.awaitLocatorMXBeanProxy;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * Distributed tests for {@link LocatorMXBean}.
 */

public class LocatorManagementDUnitTest extends ManagementTestBase {

  private VM managerVM;
  private VM locatorVM;

  private String hostName;
  private int locatorPort;

  @Before
  public void setUp() throws Exception {
    managerVM = managingNode;
    locatorVM = managedNode1;

    hostName = Host.getHost(0).getHostName();
    locatorPort = getRandomAvailableTCPPort();
  }

  @After
  public void tearDown() throws Exception {
    stopLocator(locatorVM);
  }

  /**
   * When plan is to start Distributed System later so that the system can use this locator
   */
  @Test
  public void testPeerLocation() throws Exception {
    startLocator(locatorVM, locatorPort);
    validateLocatorMXBean(locatorVM, locatorPort);

    Properties config = new Properties();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "false");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(JMX_MANAGER_HTTP_PORT, "0");

    createCache(managerVM, config);
    startManagingNode(managerVM);
    DistributedMember locatorMember = getMember(locatorVM);

    validateLocatorMXBean(managerVM, locatorMember);
  }

  @Test
  public void testPeerLocationWithPortZero() throws Exception {
    locatorPort = startLocator(locatorVM, 0);

    validateLocatorMXBean(locatorVM, locatorPort);

    Properties config = new Properties();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "false");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(JMX_MANAGER_HTTP_PORT, "0");

    createCache(managerVM, config);
    startManagingNode(managerVM);
    DistributedMember locatorMember = getMember(locatorVM);

    validateLocatorMXBean(managerVM, locatorMember);
  }

  /**
   * Tests a locator which is co-located with already existing cache
   */
  @Test
  public void testColocatedLocator() throws Exception {
    initManagement(false);

    startLocator(locatorVM, locatorPort);

    validateLocatorMXBean(locatorVM, locatorPort);
  }

  @Test
  public void testColocatedLocatorWithPortZero() throws Exception {
    initManagement(false);

    locatorPort = startLocator(locatorVM, 0);

    validateLocatorMXBean(locatorVM, locatorPort);
  }

  @Test
  public void testListManagers() throws Exception {
    initManagement(false);

    startLocator(locatorVM, locatorPort);

    validateManagers(locatorVM);
  }

  @Test
  public void testListManagersWithPortZero() throws Exception {
    initManagement(false);

    startLocator(locatorVM, 0);

    validateManagers(locatorVM);
  }

  @Test
  public void testWillingManagers() throws Exception {
    startLocator(locatorVM, locatorPort);

    Properties config = new Properties();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    config.setProperty(JMX_MANAGER, "true");

    createCache(managedNode2, config);
    createCache(managedNode3, config);

    validatePotentialManagers(locatorVM, 3);
  }

  @Test
  public void testWillingManagersWithPortZero() throws Exception {
    locatorPort = startLocator(locatorVM, 0);

    Properties config = new Properties();
    config.setProperty(LOCATORS, hostName + "[" + locatorPort + "]");
    config.setProperty(JMX_MANAGER, "true");

    createCache(managedNode2, config);
    createCache(managedNode3, config);

    validatePotentialManagers(locatorVM, 3);
  }

  /**
   * Starts a locator with given configuration. If DS is already started it will use the same DS
   */
  private int startLocator(final VM locatorVM, final int port) {
    return locatorVM.invoke("Start Locator In VM", () -> {
      assertThat(InternalLocator.hasLocator()).isFalse();

      Properties config = new Properties();
      config.setProperty(LOCATORS, "");

      InetAddress bindAddress = InetAddress.getByName(hostName);

      File logFile = new File(getTestMethodName() + "-locator" + port + ".log");
      Locator locator = Locator.startLocatorAndDS(port, logFile, bindAddress, config);

      assertThat(InternalLocator.hasLocator()).isTrue();
      return locator.getPort();
    });
  }

  private void stopLocator(final VM vm) {
    vm.invoke("Stop Locator In VM", () -> {
      assertThat(InternalLocator.hasLocator()).isTrue();

      InternalLocator.getLocator().stop();
    });
  }

  private void validateLocatorMXBean(final VM locatorVM, final int port) {
    locatorVM.invoke("validateLocatorMXBean", () -> {
      LocatorMXBean locatorMXBean = awaitLocalLocatorMXBean();

      assertThat(locatorMXBean.getPort()).isEqualTo(port);
    });
  }

  private void validateLocatorMXBean(final VM vm, final DistributedMember member) {
    vm.invoke("validateLocatorMXBean", () -> {
      LocatorMXBean locatorMXBean = awaitLocatorMXBeanProxy(member);
      assertThat(locatorMXBean).isNotNull();
    });
  }

  private void validateManagers(final VM locatorVM) {
    locatorVM.invoke("validateManagers", () -> {
      LocatorMXBean locatorMXBean = awaitLocalLocatorMXBean();

      await()
          .untilAsserted(() -> assertThat(locatorMXBean.listManagers()).hasSize(1));
    });
  }

  private void validatePotentialManagers(final VM locatorVM,
      final int expectedNumberPotentialManagers) {
    locatorVM.invoke("List Willing Managers", () -> {
      LocatorMXBean locatorMXBean = awaitLocalLocatorMXBean();

      await()
          .untilAsserted(() -> assertThat(locatorMXBean.listPotentialManagers())
              .hasSize(expectedNumberPotentialManagers));
    });
  }
}
