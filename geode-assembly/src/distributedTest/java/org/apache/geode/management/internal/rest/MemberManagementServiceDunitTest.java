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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.runtime.MemberInformation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class MemberManagementServiceDunitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(2);

  private static MemberVM locator;
  private static ClusterManagementService cmsClient;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    cluster.startServerVM(1, locator.getPort());
    cmsClient =
        ClusterManagementServiceBuilder.buildWithHostAddress()
            .setHostAddress("localhost", locator.getHttpPort())
            .build();
  }

  @Test
  public void listAllMembers() {
    MemberConfig config = new MemberConfig();
    ClusterManagementResult<MemberConfig, MemberInformation> result = cmsClient.list(config);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getRuntimeResult().size()).isEqualTo(2);

    MemberInformation memberConfig = result.getRuntimeResult().stream()
        .filter(r -> "locator-0".equals(r.getName())).findFirst().orElse(null);
    assertThat(memberConfig.isCoordinator()).isTrue();
    assertThat(memberConfig.isServer()).isFalse();
    assertThat(memberConfig.getLocatorPort()).isEqualTo(locator.getPort());
  }

  @Test
  public void listOneMember() {
    MemberConfig config = new MemberConfig();
    config.setId("locator-0");

    ClusterManagementResult<MemberConfig, MemberInformation> result = cmsClient.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getRuntimeResult().size()).isEqualTo(1);

    MemberInformation memberConfig = result.getRuntimeResult().get(0);
    assertThat(memberConfig.isCoordinator()).isTrue();
    assertThat(memberConfig.isServer()).isFalse();
    assertThat(memberConfig.getLocatorPort()).isEqualTo(locator.getPort());
  }

  @Test
  public void listNonExistentMember() {
    MemberConfig config = new MemberConfig();
    config.setId("locator");
    ClusterManagementResult<MemberConfig, MemberInformation> result = cmsClient.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getRuntimeResult().size()).isEqualTo(0);
  }
}
