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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({SecurityTest.class})
public class PDXGfshPostProcessorOnRemoteServerTest {

  private static final String REGION_NAME = "AuthRegion";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void testGfshCommand() throws Exception {
    Properties locatorProps = new Properties();
    locatorProps.setProperty(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/management/internal/security/clientServer.json");
    locatorProps.setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());
    locatorProps.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());

    MemberVM locatorVM = lsRule.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties(locatorProps);
    serverProps.setProperty(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/management/internal/security/clientServer.json");
    serverProps.setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());
    serverProps.setProperty(SECURITY_POST_PROCESSOR, PDXPostProcessor.class.getName());
    serverProps.setProperty("security-username", "super-user");
    serverProps.setProperty("security-password", "1234567");

    MemberVM serverVM = lsRule.startServerVM(1, serverProps, locatorVM.getPort());

    serverVM.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache.getSecurityService()).isNotNull();
      assertThat(cache.getSecurityService().getSecurityManager()).isNotNull();
      assertThat(cache.getSecurityService().getPostProcessor()).isNotNull();

      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
      for (int i = 0; i < 5; i++) {
        SimpleClass obj = new SimpleClass(i, (byte) i);
        region.put("key" + i, obj);
      }
    });

    // wait until the region bean is visible
    locatorVM.invoke(() -> {
      await()
          .until(() -> {
            Cache cache = CacheFactory.getAnyInstance();
            Object bean = ManagementService.getManagementService(cache)
                .getDistributedRegionMXBean("/" + REGION_NAME);
            return bean != null;
          });
    });

    gfsh.connectAndVerify(locatorVM.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        CliStrings.CONNECT__USERNAME, "dataUser", CliStrings.CONNECT__PASSWORD, "1234567");

    // get command
    gfsh.executeAndAssertThat("get --key=key1 --region=AuthRegion").statusIsSuccess()
        .containsOutput(SimpleClass.class.getName());

    gfsh.executeAndAssertThat("query --query=\"select * from /AuthRegion\"").statusIsSuccess();

    serverVM.invoke(() -> {
      PDXPostProcessor pp =
          (PDXPostProcessor) ClusterStartupRule.getCache().getSecurityService().getPostProcessor();
      // verify that the post processor is called 6 times. (5 for the query, 1 for the get)
      assertEquals(pp.getCount(), 6);
    });
  }

}
