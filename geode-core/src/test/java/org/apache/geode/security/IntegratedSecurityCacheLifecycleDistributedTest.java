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
package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.Properties;

import org.apache.geode.security.templates.SampleSecurityManager;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Ignore("This is broken but fixed on feature/GEODE-1673")
@Category({DistributedTest.class, SecurityTest.class})
public class IntegratedSecurityCacheLifecycleDistributedTest extends JUnit4CacheTestCase {

  private String locators;
  private VM locator;
  private SecurityService securityService;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    locator = host.getVM(0);

    securityService = IntegratedSecurityService.getSecurityService();

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int managerPort = ports[1];

    locators =  NetworkUtils.getServerHostName(host) + "[" + locatorPort + "]";

    locator.invoke(() -> {
      DistributedTestUtils.deleteLocatorStateFile(locatorPort);

      final Properties properties = new Properties();
      properties.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/clientServer.json");
      properties.setProperty(LOCATORS, locators);
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(SECURITY_ENABLED_COMPONENTS, "");
      properties.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());
      properties.setProperty(START_LOCATOR, locators);
      properties.setProperty(JMX_MANAGER, "true");
      properties.setProperty(JMX_MANAGER_START, "true");
      properties.setProperty(JMX_MANAGER_PORT, String.valueOf(managerPort));
      properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
      getSystem(properties);
      getCache();
    });
  }

  @Test
  public void initAndCloseTest() throws Exception {
    connect();

    {
      ManagementService ms = ManagementService.getExistingManagementService(getCache());
      assertThat(ms).isNotNull();
      assertThat(ms.isManager()).isFalse();

      verifyInitCloseInvoked();
    }

    locator.invoke(() -> {
      ManagementService ms = ManagementService.getExistingManagementService(getCache());
      assertThat(ms).isNotNull();
      assertThat(ms.isManager()).isTrue();

      verifyInitCloseInvoked();
    });
  }

  private void connect() throws IOException {
    final Properties properties = new Properties();
    properties.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/management/internal/security/clientServer.json");
    properties.setProperty(LOCATORS, locators);
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(SECURITY_ENABLED_COMPONENTS, "");
    properties.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");

    getSystem(properties);

    CacheServer server1 = getCache().addCacheServer();
    server1.setPort(0);
    server1.start();

    getCache();
  }

  @Override
  public void postTearDownCacheTestCase() throws Exception {
    closeAllCache();
  }

  private void verifyInitCloseInvoked() {
    SpySecurityManager ssm = (SpySecurityManager) this.securityService.getSecurityManager();
    assertThat(ssm.initInvoked).isEqualTo(1);
    getCache().close();
    assertThat(ssm.closeInvoked).isEqualTo(1);
  }
}
