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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
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

  private MemberVM locator;

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0,
        c -> c.withHttpService().withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort)
            .withProperty("groups", "group-1")
            .withCredential("cluster", "cluster"));

    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator,
        "data,cluster", "data,cluster");

    diskStoreDir = temporaryFolder.newFolder();
  }

  @Test
  public void createReplicateRegionWithDiskstoreWithoutDataManage() {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        diskStoreDir.getAbsolutePath()))
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    Region regionConfig = new Region();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);
    regionConfig.setDiskStoreName("DISKSTORE");

    ClusterManagementService client =
        new ClusterManagementServiceBuilder()
            .setPort(locator.getHttpPort())
            .setUsername("user")
            .setPassword("user")
            .build();

    assertThatThrownBy(() -> client.create(regionConfig))
        .hasMessageContaining("UNAUTHORIZED: User not authorized for DATA:MANAGE");
  }

  @Test
  public void createReplicateRegionWithDiskstoreWithoutClusterWrite() {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        diskStoreDir.getAbsolutePath()))
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    Region regionConfig = new Region();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);
    regionConfig.setDiskStoreName("DISKSTORE");

    ClusterManagementService client =
        new ClusterManagementServiceBuilder()
            .setPort(locator.getHttpPort())
            .setUsername("data")
            .setPassword("data")
            .build();

    assertThatThrownBy(() -> client.create(regionConfig))
        .hasMessageContaining("UNAUTHORIZED: Data not authorized for CLUSTER:WRITE:DISK");
  }

  @Test
  public void createReplicateRegionWithDiskstoreSuccess() {
    gfsh.executeAndAssertThat(String.format("create disk-store --name=DISKSTORE --dir=%s",
        diskStoreDir.getAbsolutePath()))
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    Region regionConfig = new Region();
    regionConfig.setName("REGION1");
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);
    regionConfig.setGroup("group-1");
    regionConfig.setDiskStoreName("DISKSTORE");

    ClusterManagementService client =
        new ClusterManagementServiceBuilder()
            .setPort(locator.getHttpPort())
            .setUsername("data,cluster")
            .setPassword("data,cluster")
            .build();

    ClusterManagementResult result = client.create(regionConfig);
    assertThat(result.isSuccessful()).isTrue();

    gfsh.executeAndAssertThat("describe disk-store --name=DISKSTORE --member=server-1")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("describe region --name=REGION1").statusIsSuccess()
        .hasTableSection().hasColumn("Value").contains("DISKSTORE");
  }
}
