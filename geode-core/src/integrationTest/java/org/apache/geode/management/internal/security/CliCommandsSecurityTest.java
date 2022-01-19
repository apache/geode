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

import java.util.List;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class CliCommandsSecurityTest {
  private MemberMXBean bean;

  private final List<TestCommand> commands = TestCommand.getCommands();

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_MANAGER, TestSecurityManager.class.getName())
      .withProperty(TestSecurityManager.SECURITY_JSON,
          "org/apache/geode/management/internal/security/cacheServer.json")
      .withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMXBean(MemberMXBean.class);
  }

  @Test
  @ConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() {
    SoftAssertions softly = new SoftAssertions();
    for (TestCommand command : commands) {
      // skip query commands since query commands are only available in client shell
      if (command.getCommand().startsWith("query")) {
        continue;
      }
      LogService.getLogger().info("processing: " + command.getCommand());
      // for those commands that requires a permission, we expect an exception to be thrown
      // This has the potential to become flaky for commands with more than one permission.
      if (command.getPermissions() != null && command.getPermissions().length > 0) {
        softly.assertThatThrownBy(() -> bean.processCommand(command.getCommand()))
            .describedAs(command.getCommand()).isInstanceOf(NotAuthorizedException.class)
            .hasMessageContaining(command.getPermissions()[0].toString());
        // }
      }
    }
    softly.assertAll();
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
