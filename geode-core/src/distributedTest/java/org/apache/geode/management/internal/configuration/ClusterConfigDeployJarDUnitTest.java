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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class ClusterConfigDeployJarDUnitTest extends ClusterConfigTestBase {

  private String clusterJar;
  private String group1Jar;
  private String group2Jar;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfshConnector = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    clusterJar = createJarFileWithClass("Cluster", "cluster.jar", temporaryFolder.getRoot());
    group1Jar = createJarFileWithClass("Group1", "group1.jar", temporaryFolder.getRoot());
    group2Jar = createJarFileWithClass("Group2", "group2.jar", temporaryFolder.getRoot());
  }

  @Test
  public void testDeployToNoServer() throws Exception {
    String clusterJarPath = clusterJar;
    // set up the locator/servers
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndAssertThat("deploy --jar=" + clusterJarPath).statusIsSuccess();
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
    MemberVM locator2 = lsRule.startLocatorVM(1, locator.getPort());
    MemberVM locator3 = lsRule.startLocatorVM(2, locator.getPort(), locator2.getPort());

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    gfshConnector.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();

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

    gfshConnector.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();

    ConfigGroup cluster = new ConfigGroup("cluster").jars("cluster.jar");
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    gfshConnector.executeAndAssertThat("deploy --jar=" + group1Jar + " --group=group1")
        .statusIsSuccess();

    ConfigGroup group1 = new ConfigGroup("group1").jars("group1.jar");
    ClusterConfig expectedGroup1Config = new ClusterConfig(cluster, group1);
    expectedGroup1Config.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedGroup1Config.verify(server2);
    expectedGroup1Config.verify(server3);

    gfshConnector.executeAndAssertThat("deploy --jar=" + group2Jar + " --group=group2")
        .statusIsSuccess();

    ConfigGroup group2 = new ConfigGroup("group2").jars("group2.jar");
    ClusterConfig expectedGroup1and2Config = new ClusterConfig(cluster, group1, group2);

    expectedGroup1and2Config.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedGroup1Config.verify(server2);
    expectedGroup1and2Config.verify(server3);
  }

  @Test
  public void testInvalidJarDeploy() throws Exception {
    IgnoredException.addIgnoredException(IllegalArgumentException.class);

    // set up the locator/servers
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);
    // server1 in no group
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locator.getPort());

    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    File junkFile = temporaryFolder.newFile("junk");
    FileWriter writer = new FileWriter(junkFile);
    writer.write("this is not a real jar");
    writer.close();

    // We want to ensure that a mix of good and bad jars does not produce a 'partial' deploy.
    gfshConnector.executeAndAssertThat("deploy --jar=" + clusterJar + ","
        + junkFile.getAbsolutePath()).statusIsError();
    gfshConnector.executeAndAssertThat("list deployed").statusIsSuccess()
        .containsOutput("No JAR Files Found");

    ConfigGroup cluster = new ConfigGroup("cluster").jars();
    ClusterConfig expectedClusterConfig = new ClusterConfig(cluster);
    expectedClusterConfig.verify(locator);
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

    gfshConnector.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();

    // deploy cluster.jar to the cluster
    cluster.addJar("cluster.jar");
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    // deploy group1.jar to both group1 and group2
    gfshConnector.executeAndAssertThat("deploy --jar=" + group1Jar + " --group=group1,group2")
        .statusIsSuccess();

    group1.addJar("group1.jar");
    group2.addJar("group1.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    // test undeploy cluster
    gfshConnector.executeAndAssertThat("undeploy --jar=cluster.jar").statusIsSuccess();

    cluster = cluster.removeJar("cluster.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    gfshConnector.executeAndAssertThat("undeploy --jar=group1.jar --group=group1")
        .statusIsSuccess();

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

    gfshConnector.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();

    // deploy cluster.jar to the cluster
    cluster.addJar("cluster.jar");
    expectedClusterConfig.verify(locator);
    expectedClusterConfig.verify(server1);
    expectedClusterConfig.verify(server2);
    expectedClusterConfig.verify(server3);

    // deploy group1.jar to both group1 and group2
    gfshConnector.executeAndAssertThat("deploy --jar=" + group1Jar + " --group=group1,group2")
        .statusIsSuccess();

    group1.addJar("group1.jar");
    group2.addJar("group1.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    server3.getVM().bounce();

    // test undeploy cluster
    gfshConnector.executeAndAssertThat("undeploy --jar=cluster.jar").statusIsSuccess();
    server3 = lsRule.startServerVM(3, serverProps, locator.getPort());

    cluster = cluster.removeJar("cluster.jar");
    server3Config.verify(locator);
    server1Config.verify(server1);
    server2Config.verify(server2);
    server3Config.verify(server3);

    gfshConnector.executeAndAssertThat("undeploy --jar=group1.jar --group=group1")
        .statusIsSuccess();

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
