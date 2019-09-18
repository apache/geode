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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.management.ManagementService.getExistingManagementService;
import static org.apache.geode.test.dunit.DistributedTestUtils.deleteLocatorStateFile;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.net.AvailablePortHelper;
import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class SecurityManagerLifecycleDistributedTest extends CacheTestCase {

  private final AvailablePortHelper availablePortHelper = AvailablePortHelper.create();

  private String locators;
  private VM locatorVM;

  @Before
  public void setUp() throws Exception {
    locatorVM = getVM(0);

    int[] ports = availablePortHelper.getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int managerPort = ports[1];

    locators = getServerHostName() + "[" + locatorPort + "]";

    locatorVM.invoke(() -> {
      deleteLocatorStateFile(locatorPort);

      Properties config = new Properties();
      config.setProperty(LOCATORS, locators);
      config.setProperty(MCAST_PORT, "0");
      config.setProperty(START_LOCATOR, locators);
      config.setProperty(JMX_MANAGER, "true");
      config.setProperty(JMX_MANAGER_PORT, String.valueOf(managerPort));
      config.setProperty(JMX_MANAGER_START, "true");
      config.setProperty(USE_CLUSTER_CONFIGURATION, "false");
      config.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());
      config.setProperty("security-username", "secure");
      config.setProperty("security-password", "secure");

      getSystem(config);
      getCache();
    });
  }

  @After
  public void after() throws Exception {
    closeAllCache();
  }

  @Test
  public void callbacksShouldBeInvoked() throws Exception {
    connectServer();

    verifyCallbacksRegardlessOfManager(false);

    locatorVM.invoke(() -> {
      verifyCallbacksRegardlessOfManager(true);
    });
  }

  private void connectServer() throws IOException {
    Properties config = new Properties();
    config.setProperty(LOCATORS, locators);
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    config.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());
    config.setProperty("security-username", "secure");
    config.setProperty("security-password", "secure");

    getSystem(config);

    CacheServer server1 = getCache().addCacheServer();
    server1.setPort(0);
    server1.start();

    getCache();
  }

  private void verifyCallbacksRegardlessOfManager(final boolean isManager) {
    ManagementService ms = getExistingManagementService(getCache());
    assertThat(ms).isNotNull();
    assertThat(ms.isManager()).isEqualTo(isManager);

    verifyInitAndCloseInvoked();
  }

  private void verifyInitAndCloseInvoked() {
    SecurityService securityService = getCache().getSecurityService();
    assertThat(securityService).isNotNull().isInstanceOf(IntegratedSecurityService.class);

    SpySecurityManager ssm =
        (SpySecurityManager) getCache().getSecurityService().getSecurityManager();

    assertThat(ssm.getInitInvocationCount()).isEqualTo(1);
    assertThat(ssm.getCloseInvocationCount()).isEqualTo(0);

    getCache().close();

    assertThat(ssm.getInitInvocationCount()).isEqualTo(1);
    assertThat(ssm.getCloseInvocationCount()).isEqualTo(1);
  }
}
