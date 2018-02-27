/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(IntegrationTest.class)
public class CreateRegionSecurityTest {
  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(SimpleTestSecurityManager.class).withJMXManager().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule(server::getJmxPort, jmxManager);

  @Rule
  public TestName testName = new SerializableTestName();

  @Test
  @ConnectionConfiguration(user = "cluster", password = "cluster")
  public void clusterNotAuthorized() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=" + regionName).statusIsError()
        .containsOutput("cluster not authorized for DATA:MANAGE");
  }

  // this test is to make sure that getting bean info to check name collision does not need
  // further permission.
  @Test
  @ConnectionConfiguration(user = "dataManage", password = "dataManage")
  public void dataManageAuthorized() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=" + regionName)
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=" + regionName).statusIsError()
        .containsOutput("Region /dataManageAuthorized already exists on the cluster");

    gfsh.executeAndAssertThat("create region --type=REPLICATE_PROXY --name=" + regionName)
        .statusIsError()
        .containsOutput("You can only create proxy regions with the same name on other members");
  }
}
