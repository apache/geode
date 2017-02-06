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

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;

@Category(DistributedTest.class)
public class ClusterConfigDeployJarDUnitTest extends ClusterConfigBaseTest {
  private GfshShellConnectionRule gfshConnector;

  private String clusterJar, group1Jar, group2Jar;

  @Before
  public void before() throws Exception {
    super.before();

    clusterJar = createJarFileWithClass("Cluster", "cluster.jar", lsRule.getTempFolder().getRoot());
    group1Jar = createJarFileWithClass("Group1", "group1.jar", lsRule.getTempFolder().getRoot());
    group2Jar = createJarFileWithClass("Group2", "group2.jar", lsRule.getTempFolder().getRoot());
  }

  @After
  public void after() throws Exception {
    if (gfshConnector != null) {
      gfshConnector.close();
    }
  }

  @Test
  public void testDeployToNoServer() throws Exception {
    String clusterJarPath = clusterJar;
    // set up the locator/servers
    Locator locator = lsRule.startLocatorVM(0, locatorProps);

    gfshConnector = new GfshShellConnectionRule(locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + clusterJarPath);
    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);

    expectedClusterConfig.verify(locator);

    // start a server and verify that the server gets the jar
    Server server1 = lsRule.startServerVM(1, locator.getPort());
    expectedClusterConfig.verify(server1);
  }

  @Test
  public void testDeployToMultipleLocators() throws Exception {
    Locator locator = lsRule.startLocatorVM(0, locatorProps);
    locatorProps.setProperty(LOCATORS, "localhost[" + locator.getPort() + "]");
    Locator locator2 = lsRule.startLocatorVM(1, locatorProps);
    locatorProps.setProperty(LOCATORS,
        "localhost[" + locator.getPort() + "],localhost[" + locator2.getPort() + "]");
    Locator locator3 = lsRule.startLocatorVM(2, locatorProps);

    // has to start a server in order to run deploy command
    lsRule.startServerVM(3, serverProps, locator.getPort());

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result = gfshConnector.executeCommand("deploy --jar=" + clusterJar);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);

    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(locator2);
    expectedClusterConfig.verify(locator3);
  }


  @Test
  public void testDeploy() throws Exception {
    // set up the locator/servers
    Locator locator = lsRule.startLocatorVM(0, locatorProps);
    // server1 in no group
    Server server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    // server2 in group1
    serverProps.setProperty(GROUPS, "group1");
    Server server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    // server3 in group1 and group2
    serverProps.setProperty(GROUPS, "group1,group2");
    Server server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result = gfshConnector.executeCommand("deploy --jar=" + clusterJar);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    result = gfshConnector.executeCommand("deploy --jar=" + group1Jar + " --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ConfigGroup group1 = new ConfigGroup("group1").jars("group1.jar");
    ClusterConfig expectedGroup1Config = new ClusterConfig(cluster, group1);
    expectedGroup1Config.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedGroup1Config.verify(server2);
    expectedGroup1Config.verify(server3);

    result = gfshConnector.executeCommand("deploy --jar=" + group2Jar + " --group=group2");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ConfigGroup group2 = new ConfigGroup("group2").jars("group2.jar");
    ClusterConfig expectedGroup1and2Config = new ClusterConfig(cluster, group1, group2);

    expectedGroup1and2Config.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedGroup1Config.verify(server2);
    expectedGroup1and2Config.verify(server3);
  }


  @Test
  public void testUndeploy() throws Exception {
    // set up the locator/servers
    Locator locator = lsRule.startLocatorVM(0, locatorProps);
    serverProps.setProperty(GROUPS, "group1");
    Server server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group2");
    Server server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group1,group2");
    Server server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    ConfigGroup cluster = new ConfigGroup("cluster");
    ConfigGroup group1 = new ConfigGroup("group1");
    ConfigGroup group2 = new ConfigGroup("group2");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    ClusterConfig server1Config = new ClusterConfig(cluster, group1);
    ClusterConfig server2Config = new ClusterConfig(cluster, group2);
    ClusterConfig server3Config = new ClusterConfig(cluster, group1, group2);

    gfshConnector =
        new GfshShellConnectionRule(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    gfshConnector.connect();
    assertThat(gfshConnector.isConnected()).isTrue();

    CommandResult result = gfshConnector.executeCommand("deploy --jar=" + clusterJar);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    // deploy cluster.jar to the cluster
    cluster.addJar("cluster.jar");
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    // deploy group1.jar to both group1 and group2
    result = gfshConnector.executeCommand("deploy --jar=" + group1Jar + " --group=group1,group2");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    group1.addJar("group1.jar");
    group2.addJar("group1.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    // test undeploy cluster
    result = gfshConnector.executeCommand("undeploy --jar=cluster.jar");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    cluster = cluster.removeJar("cluster.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    result = gfshConnector.executeCommand("undeploy --jar=group1.jar --group=group1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    group1 = group1.removeJar("group1.jar");
    /*
     * TODO: This is the current (weird) behavior If you started server4 with group1,group2 after
     * this undeploy command, it would have group1.jar (brought from
     * cluster_config/group2/group1.jar on locator) whereas server3 (also in group1,group2) does not
     * have this jar.
     */
    ClusterConfig weirdServer3Config =
        new ClusterConfig(cluster, group1, new ConfigGroup(group2).removeJar("group1.jar"));

    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    weirdServer3Config.verify(server3);
  }
}
