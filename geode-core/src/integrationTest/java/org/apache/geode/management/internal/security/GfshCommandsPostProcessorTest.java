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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.TestPostProcessor;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class GfshCommandsPostProcessorTest {

  @ClassRule
  public static ServerStarterRule serverStarter = new ServerStarterRule().withJMXManager()
      .withProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName())
      .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName()).withAutoStart();

  @Rule
  public GfshCommandRule gfshConnection =
      new GfshCommandRule(serverStarter::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @BeforeClass
  public static void beforeClass() throws Exception {
    serverStarter.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region1");
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite,dataRead", password = "dataWrite,dataRead")
  public void testGetPostProcess() throws Exception {
    gfshConnection.executeCommand("put --region=region1 --key=key1 --value=value1");
    gfshConnection.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfshConnection.executeCommand("put --region=region1 --key=key3 --value=value3");

    // for get command, assert the return value is processed
    gfshConnection.executeAndAssertThat("get --region=region1 --key=key1")
        .containsOutput("dataWrite,dataRead/region1/key1/value1");

    // for query command, assert the return values are processed
    gfshConnection.executeAndAssertThat("query --query=\"select * from /region1\"")
        .containsOutput("dataWrite,dataRead/null/null/value1")
        .containsOutput("dataWrite,dataRead/null/null/value2")
        .containsOutput("dataWrite,dataRead/null/null/value3");
  }
}
