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
package com.gemstone.gemfire.security;


import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.security.Principal;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.internal.security.JSONAuthorization;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class IntegratedSecurityCacheLifecycleDistributedTest extends JUnit4CacheTestCase {

  private static SpySecurityManager spySecurityManager;

  private VM locator;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    locator = host.getVM(0);
    JSONAuthorization.setUpWithJsonFile("clientServer.json");
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String locators =  NetworkUtils.getServerHostName(host) + "[" + locatorPort + "]";

    spySecurityManager = new SpySecurityManager();

    locator.invoke(() -> {
      spySecurityManager = new SpySecurityManager();
      DistributedTestUtils.deleteLocatorStateFile(locatorPort);

      final Properties properties = new Properties();
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(START_LOCATOR, locators);
      properties.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName()+".create");
      properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
      getSystem(properties);
      getCache();
    });

    final Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName()+".create");
    properties.setProperty(LOCATORS, locators);
    properties.setProperty(JMX_MANAGER, "false");
    properties.setProperty(JMX_MANAGER_PORT, "0");
    properties.setProperty(JMX_MANAGER_START, "false");
    properties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    getSystem(properties);

    CacheServer server1 = getCache().addCacheServer();
    server1.setPort(0);
    server1.start();

    getCache();
  }

  @Test
  public void initAndCloseTest () {
    verifyInitCloseInvoked();

    locator.invoke(() -> {
      verifyInitCloseInvoked();
    });
  }

  @Override
  public void postTearDownCacheTestCase() throws Exception {
    closeAllCache();
  }

  private void verifyInitCloseInvoked() {
    assertThat(spySecurityManager.initInvoked).isEqualTo(1);
    getCache().close();
    assertThat(spySecurityManager.closeInvoked).isEqualTo(1);
  }

  public static class SpySecurityManager extends JSONAuthorization {

    private static int initInvoked = 0;
    private static int closeInvoked = 0;

    public static SpySecurityManager create() {
      return spySecurityManager;
    }

    @Override
    public void init(final Properties securityProps) {
      initInvoked++;
    }

    @Override
    public Principal authenticate(final Properties props) throws AuthenticationFailedException {
      return null;
    }

    @Override
    public void close() {
      closeInvoked++;
    }
  }
}
