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

package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeRegionConfig;

public class ClientChecksConfigAndReturnTypeTest {

  private ClientClusterManagementService client;
  private ClusterManagementResult mockedResult;

  @Before
  public void before() {
    client = spy(new ClientClusterManagementService("localhost", 7777));
    RestTemplate template = mock(RestTemplate.class);
    ResponseEntity response = mock(ResponseEntity.class);

    when(template.getForEntity(any(), any(), any(), any())).thenReturn(response);
    mockedResult = new ClusterManagementResult<>();
    when(response.getBody()).thenReturn(mockedResult);

    when(client.getRestTemplate()).thenReturn(template);
  }

  @Test
  public void checkCorrectConfigAndReturnType() {
    assertThat(client.list(new RegionConfig(), RuntimeRegionConfig.class)).isEqualTo(mockedResult);
    assertThat(client.list(new MemberConfig(), MemberConfig.class)).isEqualTo(mockedResult);
  }

  @Test
  public void checkMismatchedConfigAndReturnType() {
    assertThatThrownBy(() -> client.list(new MemberConfig(), RuntimeRegionConfig.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Mismatched request type and return type:");
  }
}
