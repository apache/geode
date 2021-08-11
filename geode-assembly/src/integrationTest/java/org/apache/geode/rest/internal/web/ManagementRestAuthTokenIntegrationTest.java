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

package org.apache.geode.rest.internal.web;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class ManagementRestAuthTokenIntegrationTest {

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule()
      .withProperty(ConfigurationProperties.SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS,
          "all,management")
      .withSecurityManager(SimpleSecurityManager.class)
      .withHttpService()
      .withAutoStart();

  @Test
  public void validToken() {
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder()
            .setPort(locator.getHttpPort())
            .setAuthToken(SimpleSecurityManager.VALID_TOKEN)
            .build();
    assertThat(cms.isConnected()).isTrue();
  }

  @Test
  public void invalidToken() {
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder()
            .setPort(locator.getHttpPort())
            .setAuthToken("invalidToken")
            .build();
    assertThat(cms.isConnected()).isFalse();
  }
}
