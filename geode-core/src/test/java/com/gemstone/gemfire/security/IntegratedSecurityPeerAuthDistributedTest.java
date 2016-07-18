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
import static com.gemstone.gemfire.security.JSONAuthorization.*;
import static com.gemstone.gemfire.test.dunit.Invoke.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.security.templates.UserPasswordAuthInit;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedSecurityPeerAuthDistributedTest extends JUnit4CacheTestCase{

  private static SpyJSONAuthorization spyJSONAuthorization;

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
      JSONAuthorization.setUpWithJsonFile(PEER_AUTH_JSON);
      spyJSONAuthorization = new SpyJSONAuthorization();

      DistributedTestUtils.deleteLocatorStateFile(locatorPort);

      final Properties properties = createProperties(locators);
      properties.setProperty(UserPasswordAuthInit.USER_NAME, "locator1");
      properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");
      properties.setProperty(START_LOCATOR, locators);

      getSystem(properties);
      getCache();
    });

    server1.invoke(()-> {
      JSONAuthorization.setUpWithJsonFile(PEER_AUTH_JSON);
      spyJSONAuthorization = new SpyJSONAuthorization();

      final Properties properties = createProperties(locators);
      properties.setProperty(UserPasswordAuthInit.USER_NAME, "server1");
      properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");

      getSystem(properties);
      getCache();
    });

    server2.invoke(()-> {
      JSONAuthorization.setUpWithJsonFile(PEER_AUTH_JSON);
      spyJSONAuthorization = new SpyJSONAuthorization();

      final Properties properties = createProperties(locators);
      properties.setProperty(UserPasswordAuthInit.USER_NAME, "server2");
      properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");

      getSystem(properties);
      getCache();
    });
  }

  @Test
  public void initAndCloseTest() throws Exception {
    JSONAuthorization.setUpWithJsonFile(PEER_AUTH_JSON);
    spyJSONAuthorization = new SpyJSONAuthorization();

    final Properties properties = createProperties(locators);
    properties.setProperty(UserPasswordAuthInit.USER_NAME, "stranger");
    properties.setProperty(UserPasswordAuthInit.PASSWORD, "1234567");

    assertThatThrownBy(() -> getSystem(properties)).isExactlyInstanceOf(AuthenticationFailedException.class);
  }

  @Override
  public void postTearDownCacheTestCase() throws Exception {
    closeAllCache();
    spyJSONAuthorization = null;
    invokeInEveryVM(() -> { spyJSONAuthorization = null; });
  }

  private static Properties createProperties(String locators) {
    Properties allProperties = new Properties();
    allProperties.setProperty(LOCATORS, locators);
    allProperties.setProperty(MCAST_PORT, "0");
    allProperties.setProperty(SECURITY_MANAGER, SpyJSONAuthorization.class.getName());
    allProperties.setProperty(SECURITY_PEER_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
    allProperties.setProperty(USE_CLUSTER_CONFIGURATION, "false");
    return allProperties;
  }

  public static class SpyJSONAuthorization extends JSONAuthorization {

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
