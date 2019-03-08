package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

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

public class ClientClusterManagementServiceDunitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(2);

  private static MemberVM locator, server;
  private static ClusterManagementService cmsClient;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server = cluster.startServerVM(1, locator.getPort());
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort());
  }

  @Test
  public void createRegion() {
    RegionConfig region = new RegionConfig();
    region.setName("customer");

    ClusterManagementResult result = cmsClient.create(region, "");

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    result = cmsClient.create(region, "");
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.ENTITY_EXISTS);
  }

  @Test
  public void createRegionWithInvalidName() throws Exception {
    RegionConfig region = new RegionConfig();
    region.setName("__test");

    ClusterManagementResult result = cmsClient.create(region, "");
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT);
  }
}
