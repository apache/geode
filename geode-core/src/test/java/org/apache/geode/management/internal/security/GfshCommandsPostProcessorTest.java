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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.internal.Assert.assertTrue;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.security.TestPostProcessor;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({IntegrationTest.class, SecurityTest.class})
public class GfshCommandsPostProcessorTest {

  protected static int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
  static Properties properties = new Properties() {
    {
      setProperty(JMX_MANAGER_PORT, jmxPort + "");
      setProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName());
      setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());
      setProperty("security-json",
          "org/apache/geode/management/internal/security/cacheServer.json");
    }
  };
  @Rule
  public GfshShellConnectionRule gfshConnection =
      new GfshShellConnectionRule(jmxPort, GfshShellConnectionRule.PortType.jmxManger);
  private HeadlessGfsh gfsh = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    ServerStarterRule serverStarter = new ServerStarterRule(properties);
    serverStarter.startServer();
    serverStarter.cache.createRegionFactory(RegionShortcut.REPLICATE).create("region1");
  }

  @Before
  public void before() {
    gfsh = gfshConnection.getGfsh();
  }

  @Test
  @ConnectionConfiguration(user = "data-user", password = "1234567")
  public void testGetPostProcess() throws Exception {
    gfsh.executeCommand("put --region=region1 --key=key1 --value=value1");
    gfsh.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfsh.executeCommand("put --region=region1 --key=key3 --value=value3");

    // for get command, assert the return value is processed
    gfsh.executeCommand("get --region=region1 --key=key1");
    assertTrue(gfsh.outputString.contains("data-user/region1/key1/value1"), gfsh.outputString);

    // for query command, assert the return values are processed
    gfsh.executeCommand("query --query=\"select * from /region1\"");
    assertTrue(gfsh.outputString.contains("data-user/null/null/value1"), gfsh.outputString);
    assertTrue(gfsh.outputString.contains("data-user/null/null/value2"), gfsh.outputString);
    assertTrue(gfsh.outputString.contains("data-user/null/null/value3"), gfsh.outputString);
  }
}
