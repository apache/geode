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
package com.gemstone.gemfire.management.internal.security;

import static org.assertj.core.api.AssertionsForClassTypes.*;

import java.util.List;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 */

@Category(IntegrationTest.class)
public class CliCommandsSecurityTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;

  private List<TestCommand> commands = TestCommand.getCommands();

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess(){
   for (TestCommand command:commands) {
      LogService.getLogger().info("processing: "+command.getCommand());
      // for those commands that don't require any permission, any user can execute them
      if(command.getPermission()==null){
        bean.processCommand(command.getCommand());
      }
      else {
        assertThatThrownBy(() -> bean.processCommand(command.getCommand()))
            .hasMessageContaining(command.getPermission().toString());
      }
    }
  }

  @Test
  @JMXConnectionConfiguration(user = "super-user", password = "1234567")
  public void testAdminUser() throws Exception {
    for (TestCommand command:commands) {
      LogService.getLogger().info("processing: "+command.getCommand());
      bean.processCommand(command.getCommand());
    }
  }


}
