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

package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, SecurityTest.class})
public class MultiUserAuthenticationDUnitTest {

  private static int SESSION_COUNT = 2;
  private static int KEY_COUNT = 2;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule(3);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1, server2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.security.AuthenticationFailedException");
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getCanonicalName());
    locator = lsRule.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    server2 = lsRule.startServerVM(2, serverProps, locator.getPort());

    // create region and put in some values
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=region --type=PARTITION").statusIsSuccess();
  }

  @Test
  public void multiAuthenticatedView() throws Exception {
    int locatorPort = locator.getPort();
    for (int i = 0; i < SESSION_COUNT; i++) {
      ClientCache cache = new ClientCacheFactory(getClientCacheProperties("stranger", "stranger"))
          .setPoolSubscriptionEnabled(true).setPoolMultiuserAuthentication(true)
          .addPoolLocator("localhost", locatorPort).create();

      RegionService regionService1 =
          cache.createAuthenticatedView(getClientCacheProperties("data", "data"));
      RegionService regionService2 =
          cache.createAuthenticatedView(getClientCacheProperties("cluster", "cluster"));

      cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");

      Region region = regionService1.getRegion("/region");
      Region region2 = regionService2.getRegion("/region");
      for (int j = 0; j < KEY_COUNT; j++) {
        String value = i + "" + j;
        region.put(value, value);
        assertThatThrownBy(() -> region2.put(value, value))
            .isInstanceOf(ServerOperationException.class);
      }
      regionService1.close();
      regionService2.close();
      cache.close();
    }
  }

  private static Properties getClientCacheProperties(String username, String password) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    return props;
  }

}
