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

package org.apache.geode.management.internal.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.exceptions.EntityExistsException;

public class LocatorClusterManagementServiceTest {

  private LocatorClusterManagementService service;
  private DistributionManager distributionManager;
  private ConfigurationPersistenceService persistenceService;
  private RegionConfig regionConfig;
  private ClusterManagementResult result;

  @Before
  public void before() throws Exception {
    distributionManager = mock(DistributionManager.class);
    persistenceService = mock(ConfigurationPersistenceService.class);
    service = spy(new LocatorClusterManagementService(distributionManager, persistenceService));
    regionConfig = new RegionConfig();
  }

  @Test
  public void persistenceIsNull() throws Exception {
    service = new LocatorClusterManagementService(distributionManager, null);
    result = service.create(regionConfig, "cluster");
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getPersistenceStatus().getMessage())
        .contains("Cluster configuration service needs to be enabled");
  }

  @Test
  public void elementAlreadyExist() throws Exception {
    regionConfig.setName("test");
    CacheConfig cacheConfig = new CacheConfig();
    cacheConfig.getRegions().add(regionConfig);
    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(cacheConfig);

    assertThatThrownBy(() -> service.create(regionConfig, "cluster"))
        .isInstanceOf(EntityExistsException.class)
        .hasMessageContaining("cache element test already exists");
  }

  @Test
  public void validationFailed() throws Exception {
    assertThatThrownBy(() -> service.create(regionConfig, "cluster"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Name of the region has to be specified");
  }

  @Test
  public void noMemberFound() throws Exception {
    regionConfig.setName("test");
    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(new CacheConfig());
    doReturn(Collections.emptySet()).when(service).findMembers(any(), any());
    result = service.create(regionConfig, "cluster");
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getPersistenceStatus().getMessage())
        .contains("no members found to create cache element");
  }

  @Test
  public void partialFailureOnMembers() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults.add(new CliFunctionResult("member1", true, "success"));
    functionResults.add(new CliFunctionResult("member2", false, "failed"));
    doReturn(functionResults).when(service).executeAndGetFunctionResult(any(), any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(service).findMembers(any(),
        any());

    when(persistenceService.getCacheConfig("cluster", true)).thenReturn(new CacheConfig());
    regionConfig.setName("test");
    result = service.create(regionConfig, "cluster");
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getPersistenceStatus().getMessage())
        .contains("Failed to apply the update on all members");
  }
}
