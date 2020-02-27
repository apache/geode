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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.test.junit.assertions.ClusterManagementGetResultAssert.assertManagementGetResult;
import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.runtime.DeploymentInfo;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.ClusterManagementGetResultAssert;
import org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert;

public class DeploymentManagementDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server1, server2;

  private static ClusterManagementService client;

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();
  private static File stagingDir, group1Jar, group2Jar, clusterJar;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // prepare the jars to be deployed
    stagingDir = stagingTempDir.newFolder("staging");
    group1Jar = new File(stagingDir, "group1.jar");
    group2Jar = new File(stagingDir, "group2.jar");
    clusterJar = new File(stagingDir, "cluster.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(group1Jar, "Class1");
    jarBuilder.buildJarFromClassNames(group2Jar, "Class2");
    jarBuilder.buildJarFromClassNames(clusterJar, "Class3");

    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withSecurityManager(
        SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort).withProperty(
        DistributionConfig.GROUPS_NAME, "group1").withCredential("cluster", "cluster"));
    server2 = cluster.startServerVM(2, s -> s.withConnectionToLocator(locatorPort).withProperty(
        DistributionConfig.GROUPS_NAME, "group2").withCredential("cluster", "cluster"));

    client = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .setUsername("cluster")
        .setPassword("cluster")
        .build();


    Deployment deployment = new Deployment();
    deployment.setFile(clusterJar);
    client.create(deployment);

    deployment.setFile(group1Jar);
    deployment.setGroup("group1");
    client.create(deployment);

    deployment.setFile(group2Jar);
    deployment.setGroup("group2");
    client.create(deployment);
  }

  @Test
  public void listAll() {
    ClusterManagementListResult<Deployment, DeploymentInfo> list = client.list(new Deployment());
    ClusterManagementListResultAssert<Deployment, DeploymentInfo> resultAssert =
        assertManagementListResult(list).isSuccessful();
    resultAssert.hasConfigurations().extracting(Deployment::getFileName)
        .containsExactlyInAnyOrder("group1.jar", "group2.jar", "cluster.jar");
    resultAssert.hasRuntimeInfos().extracting(DeploymentInfo::getJarLocation).extracting(
        FilenameUtils::getName)
        .containsExactlyInAnyOrder("group1.v1.jar", "group2.v1.jar", "cluster.v1.jar",
            "cluster.v1.jar");
  }

  @Test
  public void listByGroup() throws Exception {
    Deployment filter = new Deployment();
    filter.setGroup("group1");
    ClusterManagementListResult<Deployment, DeploymentInfo> list = client.list(filter);
    ClusterManagementListResultAssert<Deployment, DeploymentInfo> resultAssert =
        assertManagementListResult(list).isSuccessful();
    resultAssert.hasConfigurations().extracting(Deployment::getFileName)
        .containsExactlyInAnyOrder("group1.jar");
    resultAssert.hasRuntimeInfos().extracting(DeploymentInfo::getJarLocation).extracting(
        FilenameUtils::getName).containsExactlyInAnyOrder("group1.v1.jar");
  }

  @Test
  public void listById() throws Exception {
    Deployment filter = new Deployment();
    filter.setFileName("cluster.jar");

    ClusterManagementListResult<Deployment, DeploymentInfo> list = client.list(filter);

    ClusterManagementListResultAssert<Deployment, DeploymentInfo> resultAssert =
        assertManagementListResult(list).isSuccessful();

    assertThat(list.getConfigResult()).hasSize(1);
    Deployment deployment = list.getConfigResult().get(0);
    assertThat(deployment.getFileName()).isEqualTo("cluster.jar");
    assertThat(deployment.getDeployedBy()).isEqualTo("cluster");
    assertThat(deployment.getDeployedTime()).isNotNull();

    List<DeploymentInfo> runtimeResult = resultAssert.getActual().getRuntimeResult();
    assertThat(runtimeResult)
        .extracting(DeploymentInfo::getJarLocation)
        .extracting(FilenameUtils::getName)
        .containsExactlyInAnyOrder("cluster.v1.jar", "cluster.v1.jar");
    assertThat(runtimeResult.get(0).getLastModified()).isNotNull();
  }

  @Test
  public void getById() throws Exception {
    Deployment filter = new Deployment();
    filter.setFileName("cluster.jar");
    ClusterManagementGetResult<Deployment, DeploymentInfo> result = client.get(filter);
    ClusterManagementGetResultAssert<Deployment, DeploymentInfo> resultAssert =
        assertManagementGetResult(result).isSuccessful();
    resultAssert.hasConfiguration().extracting(Deployment::getFileName).isEqualTo("cluster.jar");
    resultAssert.hasRuntimeInfos().extracting(DeploymentInfo::getJarLocation).extracting(
        FilenameUtils::getName).containsExactlyInAnyOrder("cluster.v1.jar", "cluster.v1.jar");
  }
}
