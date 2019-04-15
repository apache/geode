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

package org.apache.geode.management.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class CreateRegionWithDiskstoreAndSecurityDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File diskStoreDir;

  private MemberVM server;
  private MemberVM locator;

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0,
        c -> c.withHttpService().withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    server = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort)
            .withCredential("cluster", "cluster"));

    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator,
        "data,cluster", "data,cluster");

    diskStoreDir = temporaryFolder.newFolder();
  }

  @Test
  public void createReplicateRegionWithDiskstoreWithoutDataManage() throws Exception {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        diskStoreDir.getAbsolutePath())).statusIsSuccess();

    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);

    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDiskStoreName("DISKSTORE");
    regionConfig.setRegionAttributes(attributes);

    ClusterManagementService client =
        ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(), null, null,
            "user", "user");
    ClusterManagementResult result = client.create(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.UNAUTHORIZED);
    assertThat(result.getStatusMessage()).isEqualTo("user not authorized for DATA:MANAGE");
  }

  @Test
  public void createReplicateRegionWithDiskstoreWithoutClusterWrite() throws Exception {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        diskStoreDir.getAbsolutePath())).statusIsSuccess();

    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);

    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDiskStoreName("DISKSTORE");
    regionConfig.setRegionAttributes(attributes);

    ClusterManagementService client =
        ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(), null, null,
            "data", "data");
    ClusterManagementResult result = client.create(regionConfig);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.UNAUTHORIZED);
    assertThat(result.getStatusMessage()).isEqualTo("data not authorized for CLUSTER:WRITE:DISK");
  }

  @Test
  public void createReplicateRegionWithDiskstoreSuccess() throws Exception {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        diskStoreDir.getAbsolutePath())).statusIsSuccess();

    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);

    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setDiskStoreName("DISKSTORE");
    regionConfig.setRegionAttributes(attributes);

    ClusterManagementService client =
        ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(), null, null,
            "data,cluster", "data,cluster");
    ClusterManagementResult result = client.create(regionConfig);
    assertThat(result.isSuccessful()).isTrue();
  }

}
