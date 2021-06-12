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

import static org.apache.geode.test.junit.assertions.ClusterManagementRealizationResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.DiskDir;
import org.apache.geode.management.configuration.DiskStore;
import org.apache.geode.management.runtime.DiskStoreInfo;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DiskStoreManagementIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  private ClusterManagementService client;

  private DiskStore diskStore;
  private DiskDir diskDir;

  @Before
  public void before() {
    // needs to be used together with any BaseLocatorContextLoader
    LocatorWebContext context = new LocatorWebContext(webApplicationContext);
    client = new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(
            new RestTemplate(context.getRequestFactory())))
        .build();
    diskStore = new DiskStore();
    diskDir = new DiskDir();
  }

  @Test
  @WithMockUser
  public void sanityCheck() {
    diskStore.setName("storeone");
    diskDir.setName("diskdirone");
    diskStore.setDirectories(Collections.singletonList(diskDir));

    assertManagementResult(client.create(diskStore))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
    assertManagementResult(client.delete(diskStore))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
  }

  @Test
  @WithMockUser
  public void createWithInvalidGroupFails() {
    diskStore.setName("storeone");
    diskDir.setName("diskdirone");
    diskStore.setDirectories(Collections.singletonList(diskDir));
    diskStore.setGroup("cluster");

    assertThatThrownBy(() -> client.create(diskStore))
        .hasMessageContaining("ILLEGAL_ARGUMENT: 'cluster' is a reserved group name");
  }

  @Test
  public void createWithMissingDiskDirFails() {
    diskStore.setName("storeone");

    assertThatThrownBy(() -> client.create(diskStore))
        .hasMessageContaining("ILLEGAL_ARGUMENT: At least one DiskDir element required");
  }

  @Test
  public void createDuplicateDiskStoreFails() {
    diskStore.setName("storeone");
    diskDir.setName("diskdirone");
    diskStore.setDirectories(Collections.singletonList(diskDir));

    // trying to create a duplicate diskstore, reusing existing
    assertManagementResult(client.create(diskStore))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);

    assertThatThrownBy(() -> client.create(diskStore))
        .hasMessageContaining(
            "ENTITY_EXISTS: DiskStore 'storeone' already exists in group cluster");

    assertManagementResult(client.delete(diskStore))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
  }

  @Test
  public void createWithIllegalParamFails() {
    diskStore.setName("storeone");
    diskDir.setName("diskdirone");
    diskStore.setDirectories(Collections.singletonList(diskDir));
    diskStore.setDiskUsageCriticalPercentage(120.0F);

    assertThatThrownBy(() -> client.create(diskStore))
        .hasMessageContaining(
            "ILLEGAL_ARGUMENT: Disk usage critical percentage must be set to a value between 0-100.  The value 120.0 is invalid");
  }

  @Test
  public void diskStoresCanBeDeleted() {
    diskStore.setName("storeone");
    diskDir.setName("diskdirone");
    diskStore.setDirectories(Collections.singletonList(diskDir));

    assertManagementResult(client.create(diskStore))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);

    ClusterManagementGetResult<DiskStore, DiskStoreInfo> clusterManagementGetResult =
        client.get(diskStore);
    assertThat(clusterManagementGetResult.isSuccessful());
    assertThat(clusterManagementGetResult.getResult().getId()).isEqualTo("storeone");

    assertManagementResult(client.delete(diskStore))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
  }
}
