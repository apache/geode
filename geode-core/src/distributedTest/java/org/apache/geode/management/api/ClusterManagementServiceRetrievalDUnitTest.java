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

package org.apache.geode.management.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.web.util.AbstractUriTemplateHandler;

import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.management.internal.ClientClusterManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ClusterManagementServiceRetrievalDUnitTest {

  private static MemberVM locator;
  private static MemberVM server1;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void retrieveClusterManagementServiceFromServer() {
    locator = cluster.startLocatorVM(0,
        x -> x.withHttpService());
    server1 = cluster.startServerVM(1, locator.getPort());

    final String url =
        String.format("http://localhost:%d/geode-management/v2", locator.getHttpPort());
    server1.invoke(() -> {
      ClientClusterManagementService cms =
          (ClientClusterManagementService) ClusterManagementServiceProvider.getService();
      assertThat(
          ((AbstractUriTemplateHandler) cms.getRestTemplate().getUriTemplateHandler()).getBaseUrl())
              .isEqualTo(url);
    });
  }

}
