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
import static org.apache.geode.test.dunit.Invoke.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedSecurityPeerAuthDistributedTest extends JUnit4CacheTestCase{

  private static SpySecurityManager spySecurityManager;

  private VM locator;
  private VM server1;
  private VM server2;

  private String locators;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    locator = host.getVM(0);
    server1 = host.getVM(1);
    server2 = host.getVM(2);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    locators =  NetworkUtils.getServerHostName(host) + "[" + locatorPort + "]";

    locator.invoke(() -> {
      spySecurityManager = new SpySecurityManager();

      DistributedTestUtils.deleteLocatorStateFile(locatorPort);

      final Properties properties = createProperties(locators);
      properties.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/security/peerAuth.json");
      properties.setProperty(UserPasswordAuthInit.USER_NAME, "locator1");
      properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");
      properties.setProperty(START_LOCATOR, locators);

      getSystem(properties);
      getCache();
    });

    server1.invoke(()-> {
      spySecurityManager = new SpySecurityManager();

      final Properties properties = createProperties(locators);
      properties.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/security/peerAuth.json");
      properties.setProperty(UserPasswordAuthInit.USER_NAME, "server1");
      properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");

      getSystem(properties);
      getCache();
    });

    server2.invoke(()-> {
      spySecurityManager = new SpySecurityManager();

      final Properties properties = createProperties(locators);
      properties.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/security/peerAuth.json");
      properties.setProperty(UserPasswordAuthInit.USER_NAME, "server2");
      properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");

      getSystem(properties);
      getCache();
    });
  }

  @Test
  public void initAndCloseTest() throws Exception {
    spySecurityManager = new SpySecurityManager();

    final Properties properties = createProperties(locators);
    properties.setProperty(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/security/peerAuth.json");
    properties.setProperty(UserPasswordAuthInit.USER_NAME, "stranger");
    properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");

    assertThatThrownBy(() -> getSystem(properties)).isExactlyInstanceOf(GemFireSecurityException.class);
  }

  @Override
  public void postTearDownCacheTestCase() throws Exception {
    closeAllCache();
    spySecurityManager = null;
    invokeInEveryVM(() -> { spySecurityManager = null; });
  }

  private static Properties createProperties(String locators) {
    Properties allProperties = new Properties();
    allProperties.setProperty(LOCATORS, locators);
    allProperties.setProperty(MCAST_PORT, "0");
    allProperties.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());
    allProperties.setProperty(SECURITY_PEER_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    allProperties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    return allProperties;
  }

  public static class SpySecurityManager extends SampleSecurityManager {

    static int initInvoked = 0;
    static int closeInvoked = 0;

    @Override
    public void init(final Properties securityProps) {
      initInvoked++;
      super.init(securityProps);
    }

    @Override
    public void close() {
      closeInvoked++;
      super.close();
    }
  }
}
