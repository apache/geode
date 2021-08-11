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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Member;
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
    cmsClient = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .build();
  }

  @Test
  public void listAllMembers() {
    Member config = new Member();
    ClusterManagementListResult<Member, MemberInformation> result = cmsClient.list(config);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getRuntimeResult().size()).isEqualTo(2);

    MemberInformation memberConfig = result.getRuntimeResult().stream()
        .filter(r -> "locator-0".equals(r.getMemberName())).findFirst().orElse(null);
    assertThat(memberConfig.isCoordinator()).isTrue();
    assertThat(memberConfig.isServer()).isFalse();
    assertThat(memberConfig.getLocatorPort()).isEqualTo(locator.getPort());
  }

  @Test
  public void listOneMember() {
    Member config = new Member();
    config.setId("locator-0");

    ClusterManagementListResult<Member, MemberInformation> result = cmsClient.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getRuntimeResult().size()).isEqualTo(1);

    MemberInformation memberConfig = result.getRuntimeResult().get(0);
    assertThat(memberConfig.isCoordinator()).isTrue();
    assertThat(memberConfig.isServer()).isFalse();
    assertThat(memberConfig.getLocatorPort()).isEqualTo(locator.getPort());
  }

  @Test
  public void getOneMember() {
    Member config = new Member();
    config.setId("locator-0");

    ClusterManagementGetResult<Member, MemberInformation> result = cmsClient.get(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getResult().getRuntimeInfos().size()).isEqualTo(1);

    MemberInformation memberConfig = result.getResult().getRuntimeInfos().get(0);
    assertThat(memberConfig.isCoordinator()).isTrue();
    assertThat(memberConfig.isServer()).isFalse();
    assertThat(memberConfig.getLocatorPort()).isEqualTo(locator.getPort());
  }

  @Test
  public void getNonExistentMember() {
    Member config = new Member();
    config.setId("locator-42");

    assertThatThrownBy(() -> cmsClient.get(config))
        .hasMessageContaining("ENTITY_NOT_FOUND: Member 'locator-42' does not exist.");
  }

  @Test
  public void getImproperlySpecifiedMember() {
    Member config = new Member();
    assertThatThrownBy(() -> cmsClient.get(config))
        .hasMessageContaining("Unable to construct the URI with the current configuration.");
  }

  @Test
  public void listNonExistentMember() {
    Member config = new Member();
    config.setId("locator");
    ClusterManagementListResult<Member, MemberInformation> result = cmsClient.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getRuntimeResult().size()).isEqualTo(0);
  }
}
