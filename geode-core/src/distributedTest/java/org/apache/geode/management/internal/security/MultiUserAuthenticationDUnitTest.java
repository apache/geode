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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({SecurityTest.class})
public class MultiUserAuthenticationDUnitTest {

  private static int SESSION_COUNT = 2;
  private static int KEY_COUNT = 2;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule client = new ClientCacheRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.security.AuthenticationFailedException");

    locator = lsRule.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));

    Properties serverProps = new Properties();
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    lsRule.startServerVM(1, serverProps, locator.getPort());
    lsRule.startServerVM(2, serverProps, locator.getPort());

    // create region and put in some values
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=region --type=PARTITION").statusIsSuccess();
  }

  @Test
  public void multiAuthenticatedView() throws Exception {
    int locatorPort = locator.getPort();
    for (int i = 0; i < SESSION_COUNT; i++) {
      ClientCache cache = client.withCacheSetup(f -> f.setPoolSubscriptionEnabled(true)
          .setPoolMultiuserAuthentication(true)
          .addPoolLocator("localhost", locatorPort))
          .createCache();

      RegionService regionService1 = client.createAuthenticatedView("data", "data");
      RegionService regionService2 = client.createAuthenticatedView("cluster", "cluster");

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
}
