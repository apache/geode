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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.http;
import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;


@Category({DistributedTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class GcCommandDUnitTest {
  private static final String MANAGER_NAME = "Manager";
  private static final String SERVER1_NAME = "Server1";
  private static final String SERVER2_NAME = "Server2";
  private static final String GROUP0 = "Group0";
  private static final String GROUP1 = "Group1";
  private static final String GROUP2 = "Group2";

  @Parameterized.Parameter
  public static boolean useHttp;

  @Parameterized.Parameters(name = "Use http: {0}")
  public static Object[] data() {
    return new Object[] {true, false};
  }


  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setup() throws Exception {
    Properties managerProps = new Properties();
    managerProps.setProperty(NAME, MANAGER_NAME);
    managerProps.setProperty(GROUPS, GROUP0);
    managerProps.setProperty(LOG_FILE, "someLog.log");

    MemberVM manager = clusterStartupRule.startLocatorVM(0, managerProps);

    Properties server1Props = new Properties();
    server1Props.setProperty(NAME, SERVER1_NAME);
    server1Props.setProperty(GROUPS, GROUP1);
    clusterStartupRule.startServerVM(1, server1Props, manager.getPort());

    Properties server2Props = new Properties();
    server2Props.setProperty(NAME, SERVER2_NAME);
    server2Props.setProperty(GROUPS, GROUP2);
    clusterStartupRule.startServerVM(2, server2Props, manager.getPort());

    if (useHttp) {
      gfsh.connectAndVerify(manager.getHttpPort(), http);
    } else {
      gfsh.connectAndVerify(manager.getJmxPort(), jmxManager);
    }
  }

  @Test
  public void testGCForGroup() {
    String gcCommand = "gc --group=" + GROUP0;
    gfsh.executeAndAssertThat(gcCommand).statusIsSuccess();

    assertThat(gfsh.getGfshOutput()).contains(MANAGER_NAME);
  }

  @Test
  public void testGCForMemberID() {
    String gcCommand = "gc --member=" + MANAGER_NAME;

    gfsh.executeAndAssertThat(gcCommand).statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains(MANAGER_NAME);
  }

  @Test
  public void testGCForEntireCluster() {
    String command = "gc";
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    String output = gfsh.getGfshOutput();
    assertThat(output).contains(SERVER1_NAME);
    assertThat(output).contains(SERVER2_NAME);
    assertThat(output).doesNotContain(MANAGER_NAME);
  }

  @Test
  public void testGCForInvalidMember() throws Exception {
    String gcCommand = "gc --member=NotAValidMember";

    CommandResult result = gfsh.executeCommand(gcCommand);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains("Member NotAValidMember could not be found.");
  }
}
