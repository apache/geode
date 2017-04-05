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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ConnectionConfiguration;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

@Category({IntegrationTest.class, SecurityTest.class})
public class CliCommandsSecurityTest {
  private MemberMXBean bean;

  private List<TestCommand> commands = TestCommand.getCommands();

  @ClassRule
  public static LocalServerStarterRule server = new ServerStarterBuilder().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .buildInThisVM();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server.getJmxPort());

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() {
    for (TestCommand command : commands) {
      // skip query commands since query commands are only available in client shell
      if (command.getCommand().startsWith("query"))
        continue;
      LogService.getLogger().info("processing: " + command.getCommand());
      // for those commands that requires a permission, we expect an exception to be thrown
      if (command.getPermission() != null) {
        try {
          String result = bean.processCommand(command.getCommand());
          fail(command.getCommand() + " has result: " + result);
        } catch (NotAuthorizedException e) {
          assertTrue(e.getMessage() + " should contain " + command.getPermission(),
              e.getMessage().contains(command.getPermission().toString()));
        }
      }
    }
  }

  @Test
  @ConnectionConfiguration(user = "super-user", password = "1234567")
  public void testAdminUser() throws Exception {
    for (TestCommand command : commands) {
      LogService.getLogger().info("processing: " + command.getCommand());
      bean.processCommand(command.getCommand());
    }
  }

}
