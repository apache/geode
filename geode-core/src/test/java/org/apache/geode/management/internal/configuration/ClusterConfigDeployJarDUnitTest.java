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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class ClusterConfigDeployJarDUnitTest extends ClusterConfigTestBase {

  private String clusterJar;
  private String group1Jar;
  private String group2Jar;

  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    clusterJar = createJarFileWithClass("Cluster", "cluster.jar", lsRule.getTempFolder().getRoot());
    group1Jar = createJarFileWithClass("Group1", "group1.jar", lsRule.getTempFolder().getRoot());
    group2Jar = createJarFileWithClass("Group2", "group2.jar", lsRule.getTempFolder().getRoot());
  }

  @Test
  public void testDeployToNoServer() throws Exception {
    String clusterJarPath = clusterJar;
    // set up the locator/servers
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + clusterJarPath);
    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);

    expectedClusterConfig.verify(locator);

    // start a server and verify that the server gets the jar
    MemberVM server1 = lsRule.startServerVM(1, locator.getPort());
    expectedClusterConfig.verify(server1);
  }

  @Test
  public void testDeployToMultipleLocators() throws Exception {
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);
    locatorProps.setProperty(LOCATORS, "localhost[" + locator.getPort() + "]");
    MemberVM locator2 = lsRule.startLocatorVM(1, locatorProps);
    locatorProps.setProperty(LOCATORS,
        "localhost[" + locator.getPort() + "],localhost[" + locator2.getPort() + "]");
    MemberVM locator3 = lsRule.startLocatorVM(2, locatorProps);

    // has to start a server in order to run deploy command
    lsRule.startServerVM(3, serverProps, locator.getPort());

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + clusterJar);

    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);

    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(locator2);
    expectedClusterConfig.verify(locator3);
  }

  @Test
  public void testDeploy() throws Exception {
    // set up the locator/servers
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);
    // server1 in no group
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    // server2 in group1
    serverProps.setProperty(GROUPS, "group1");
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    // server3 in group1 and group2
    serverProps.setProperty(GROUPS, "group1,group2");
    MemberVM server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + clusterJar);

    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + group1Jar + " --group=group1");

    ConfigGroup group1 = new ConfigGroup("group1").jars("group1.jar");
    ClusterConfig expectedGroup1Config = new ClusterConfig(cluster, group1);
    expectedGroup1Config.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedGroup1Config.verify(server2);
    expectedGroup1Config.verify(server3);

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + group2Jar + " --group=group2");

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
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);
    serverProps.setProperty(GROUPS, "group1");
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group2");
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group1,group2");
    serverProps.setProperty(LOG_LEVEL, "info");
    MemberVM server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    ConfigGroup cluster = new ConfigGroup("cluster");
    ConfigGroup group1 = new ConfigGroup("group1");
    ConfigGroup group2 = new ConfigGroup("group2");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    ClusterConfig server1Config = new ClusterConfig(cluster, group1);
    ClusterConfig server2Config = new ClusterConfig(cluster, group2);
    ClusterConfig server3Config = new ClusterConfig(cluster, group1, group2);

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + clusterJar);

    // deploy cluster.jar to the cluster
    cluster.addJar("cluster.jar");
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    // deploy group1.jar to both group1 and group2
    gfshConnector.executeAndVerifyCommand("deploy --jar=" + group1Jar + " --group=group1,group2");

    group1.addJar("group1.jar");
    group2.addJar("group1.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    // test undeploy cluster
    gfshConnector.executeAndVerifyCommand("undeploy --jar=cluster.jar");

    cluster = cluster.removeJar("cluster.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    gfshConnector.executeAndVerifyCommand("undeploy --jar=group1.jar --group=group1");

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

  @Test
  public void testUndeployWithServerBounce() throws Exception {
    // set up the locator/servers
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);
    serverProps.setProperty(GROUPS, "group1");
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group2");
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locator.getPort());
    serverProps.setProperty(GROUPS, "group1,group2");
    serverProps.setProperty(LOG_LEVEL, "info");
    MemberVM server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    ConfigGroup cluster = new ConfigGroup("cluster");
    ConfigGroup group1 = new ConfigGroup("group1");
    ConfigGroup group2 = new ConfigGroup("group2");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    ClusterConfig server1Config = new ClusterConfig(cluster, group1);
    ClusterConfig server2Config = new ClusterConfig(cluster, group2);
    ClusterConfig server3Config = new ClusterConfig(cluster, group1, group2);

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + clusterJar);

    // deploy cluster.jar to the cluster
    cluster.addJar("cluster.jar");
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    // deploy group1.jar to both group1 and group2
    gfshConnector.executeAndVerifyCommand("deploy --jar=" + group1Jar + " --group=group1,group2");

    group1.addJar("group1.jar");
    group2.addJar("group1.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    server3.getVM().bounce();

    // test undeploy cluster
    gfshConnector.executeAndVerifyCommand("undeploy --jar=cluster.jar");
    server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    cluster = cluster.removeJar("cluster.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    gfshConnector.executeAndVerifyCommand("undeploy --jar=group1.jar --group=group1");

    group1 = group1.removeJar("group1.jar");
    /*
     * TODO: GEODE-2779 This is the current (weird) behavior If you started server4 with
     * group1,group2 after this undeploy command, it would have group1.jar (brought from
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
