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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.DiskDir;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.rest.LocatorWebContext;
import org.apache.geode.management.internal.rest.PlainLocatorContextLoader;
import org.apache.geode.management.runtime.DiskStoreInfo;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class CreateDiskStoreDUnitTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  private ClusterManagementService client;
  private LocatorWebContext webContext;
  private DiskStore diskStore;
  private final String diskStoreName = "testDiskStore";
  private MemberVM server;

  @Before
  public void before() throws Exception {
    cluster.setSkipLocalDistributedSystemCleanup(true);
    webContext = new LocatorWebContext(webApplicationContext);
    client = new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(
            new RestTemplate(webContext.getRequestFactory())))
        .build();
    diskStore = createDiskStoreConfigObject(diskStoreName);
  }

  @After
  public void after() {
    // for the test to be run multiple times, we need to clean out the cluster config
    InternalConfigurationPersistenceService cps = getLocator().getConfigurationPersistenceService();
    UnaryOperator<CacheConfig> mutator = config -> {
      config.getDiskStores().clear();
      return config;
    };

    cps.updateCacheConfig("cluster", mutator);
    cps.updateCacheConfig("SameGroup", mutator);
    cps.updateCacheConfig("OtherGroup", mutator);
    if (server != null) {
      server.stop(true);
    }
  }

  private DiskStore createDiskStoreConfigObject(String diskStoreName) throws IOException {
    DiskStore diskStore = new DiskStore();
    diskStore.setName(diskStoreName);
    DiskDir diskDir = new DiskDir(
        temporaryFolder.getRoot().getAbsolutePath() + File.pathSeparator + diskStoreName, null);
    List<DiskDir> directories = new ArrayList<>();
    directories.add(diskDir);
    diskStore.setDirectories(directories);
    return diskStore;
  }

  InternalLocator getLocator() {
    return ((PlainLocatorContextLoader) webContext.getLocator()).getLocatorStartupRule()
        .getLocator();
  }

  @Test
  public void createDiskStoreWithNoServer() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    ClusterManagementRealizationResult result = client.create(diskStore);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).hasSize(0);
  }

  @Test
  public void createDuplicateDiskStoreFails() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);

    // call create the 2nd time
    assertThatThrownBy(() -> client.create(diskStore))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining(
            "ENTITY_EXISTS: DiskStore '" + diskStoreName + "' already exists in group cluster");

  }

  @Test
  public void getReturnsDesiredDiskStore() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    // verify the get
    ClusterManagementGetResult<DiskStore, DiskStoreInfo> getResult = client.get(diskStore);
    DiskStore configResult = getResult.getResult().getConfigurations().get(0);
    assertThat(configResult.getName()).isEqualTo(diskStoreName);
    assertThat(configResult.getDirectories().size()).isEqualTo(1);
    assertThat(getResult.getResult().getRuntimeInfos()).hasSize(0);
  }

  @Test
  public void createDiskStoreWithARunningServerShouldSucceed() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    server = cluster.startServerVM(1, webContext.getLocator().getPort());

    ClusterManagementRealizationResult result = client.create(diskStore);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    RealizationResult status = result.getMemberStatuses().get(0);
    assertThat(status.getMemberName()).isEqualTo("server-1");
    assertThat(status.getMessage()).contains(
        "DiskStore " + diskStoreName + " created successfully.");
  }

  @Test
  public void creatingADuplicateDiskStoreWhileServerRunningShouldThrowException() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    server = cluster.startServerVM(1, webContext.getLocator().getPort());

    client.create(diskStore);

    // create the 2nd time
    assertThatThrownBy(() -> client.create(diskStore))
        .isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining(
            "ENTITY_EXISTS: DiskStore '" + diskStoreName + "' already exists in group cluster.");
  }

  @Test
  public void shouldBeAbleToGetACreatedDiskStore() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    server = cluster.startServerVM(1, webContext.getLocator().getPort());

    client.create(diskStore);

    ClusterManagementGetResult<DiskStore, DiskStoreInfo> getResult = client.get(diskStore);
    DiskStore configResult = getResult.getResult().getConfigurations().get(0);
    assertThat(configResult.getName()).isEqualTo(diskStoreName);
    List<DiskStoreInfo> runtimeResults = getResult.getResult().getRuntimeInfos();
    assertThat(runtimeResults).hasSize(1);
  }

  @Test
  public void createDiskStoreWithoutAServerThenStartServerShouldCreateDiskStore() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    server = cluster.startServerVM(1, webContext.getLocator().getPort());

    ClusterManagementGetResult<DiskStore, DiskStoreInfo> getResult = client.get(diskStore);
    DiskStore configResult = getResult.getResult().getConfigurations().get(0);
    assertThat(configResult.getName()).isEqualTo(diskStoreName);
    List<DiskStoreInfo> runtimeResults = getResult.getResult().getRuntimeInfos();
    assertThat(runtimeResults).hasSize(1);
  }

  @Test
  public void destroyingADiskStoreAfterAServerHasBeenTakenOffline() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    server = cluster.startServerVM(1, webContext.getLocator().getPort());

    server.stop();
    ClusterManagementRealizationResult deleteResult = client.delete(diskStore);
    assertThat(deleteResult.getStatusMessage())
        .isEqualTo("Successfully updated configuration for cluster.");

    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void destroyingDiskStoreBeforeDiskStoresActuallyCreatedShouldSucceed() {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    ClusterManagementRealizationResult deleteResult = client.delete(diskStore);
    assertThat(deleteResult.getStatusMessage())
        .isEqualTo("Successfully updated configuration for cluster.");

    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");
  }

  @Test
  public void listDiskStoresShouldReturnAllConfiguredDiskStores() throws Exception {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    client.create(createDiskStoreConfigObject("DiskStore2"));
    client.create(createDiskStoreConfigObject("DiskStore3"));
    assertThat(client.list(new DiskStore()).getResult().size()).isEqualTo(3);
  }

  @Test
  public void listDiskStoresShouldReturnNonDeletedDiskStores() throws Exception {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    client.create(createDiskStoreConfigObject("DiskStore2"));
    client.create(createDiskStoreConfigObject("DiskStore3"));
    client.delete(diskStore);
    assertThat(client.list(new DiskStore()).getResult().size()).isEqualTo(2);
  }

  @Test
  public void cannotRemoveDiskstoreWhileUsedByRegion() {
    IgnoredException.addIgnoredException(".*Disk store is currently in use by these regions.*");
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    client.create(diskStore);
    server = cluster.startServerVM(1, webContext.getLocator().getPort());

    Region region = new Region();
    region.setName("testregion");
    region.setType(RegionType.PARTITION_PERSISTENT);
    region.setDiskStoreName(diskStoreName);
    client.create(region);

    assertThatThrownBy(() -> client.delete(diskStore))
        .hasMessageContaining("Disk store is currently in use by these regions");

    client.delete(region);
    ClusterManagementRealizationResult deleteResult = client.delete(diskStore);
    assertThat(deleteResult.isSuccessful()).isTrue();
  }

  @Test
  public void createDiskStoreOnGroupShouldOnlyExecuteOnServersInThatGroup() throws Exception {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    server = cluster.startServerVM(1, "OtherGroup", webContext.getLocator().getPort());

    diskStore.setGroup("SameGroup");
    ClusterManagementRealizationResult createResult = client.create(diskStore);
    assertThat(createResult.getMemberStatuses().size()).isEqualTo(0);
    assertThat(client.list(new DiskStore()).getResult().size()).isEqualTo(1);
  }

  @Test
  public void deleteDiskStoreShouldThrowExceptionIfGroupSpecified() throws Exception {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    server = cluster.startServerVM(1, "SameGroup", webContext.getLocator().getPort());
    diskStore.setGroup("SameGroup");
    client.create(diskStore);

    assertThatThrownBy(() -> client.delete(diskStore))
        .hasMessageContaining(
            "ILLEGAL_ARGUMENT: Group is an invalid option when deleting disk store");
  }

  @Test
  public void createDiskStoreByGroupShouldSucceed() throws Exception {
    assertThatThrownBy(() -> client.get(diskStore)).isInstanceOf(ClusterManagementException.class)
        .hasMessageContaining("ENTITY_NOT_FOUND");

    diskStore.setGroup("SameGroup");
    client.create(diskStore);
    assertThat(client.list(new DiskStore()).getResult().size()).isEqualTo(1);
  }
}
