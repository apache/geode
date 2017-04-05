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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.security.TestPostProcessor;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, SecurityTest.class})
public class GfshCommandsPostProcessorTest {

  @ClassRule
  public static LocalServerStarterRule server = new ServerStarterBuilder().withJMXManager()
      .withProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName())
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty("security-json",
          "org/apache/geode/management/internal/security/cacheServer.json")
      .buildInThisVM();

  @Rule
  public GfshShellConnectionRule gfshConnection =
      new GfshShellConnectionRule(server.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);

  @BeforeClass
  public static void beforeClass() throws Exception {
    server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region1");
  }

  @Test
  @ConnectionConfiguration(user = "data-user", password = "1234567")
  public void testGetPostProcess() throws Exception {
    gfshConnection.executeCommand("put --region=region1 --key=key1 --value=value1");
    gfshConnection.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfshConnection.executeCommand("put --region=region1 --key=key3 --value=value3");

    // for get command, assert the return value is processed
    String result = gfshConnection.execute("get --region=region1 --key=key1");
    assertThat(result).contains("data-user/region1/key1/value1");

    // for query command, assert the return values are processed
    result = gfshConnection.execute("query --query=\"select * from /region1\"");
    assertThat(result).contains("data-user/null/null/value1");
    assertThat(result).contains("data-user/null/null/value2");
    assertThat(result).contains("data-user/null/null/value3");
  }
}
