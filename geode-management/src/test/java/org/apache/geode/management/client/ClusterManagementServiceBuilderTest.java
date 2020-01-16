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

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.api.BasicClusterManagementServiceConnectionConfig;
import org.apache.geode.management.api.ClusterManagementServiceConnectionConfig;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;

public class ClusterManagementServiceBuilderTest {

  private static ClusterManagementServiceConnectionConfig connectionConfig;

  @BeforeClass
  public static void setup() {
    connectionConfig = new BasicClusterManagementServiceConnectionConfig("localhost", 7777);
  }

  @Test
  public void buildWithTransportOnlyHavingConnectionConfig() {
    new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(connectionConfig)).build();
  }

  @Test
  public void buildWithTransportOnlyHavingRestTemplate() {
    new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate())).build();
  }

  @Test
  public void buildWithTransportOnlyHavingRestTemplateAndConnectionConfig() {
    new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate(), connectionConfig))
        .build();
  }

  @Test
  public void buildWithConnectionConfigOnly() {
    new ClusterManagementServiceBuilder()
        .setConnectionConfig(connectionConfig).build();
  }

  @Test
  public void buildWithTransportHavingRestTemplateAndConnectionConfig() {
    new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate()))
        .setConnectionConfig(connectionConfig).build();
  }

  @Test
  public void buildWithTransportHavingConnectionConfigAndConnectionConfig() {
    new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(connectionConfig))
        .setConnectionConfig(connectionConfig).build();
  }

  @Test
  public void buildWithTransportHavingConnectionConfigAndRestTemplateAndConnectionConfig() {
    new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(new RestTemplate(), connectionConfig))
        .setConnectionConfig(connectionConfig).build();
  }

  @Test(expected = IllegalStateException.class)
  public void buildWithNothing() {
    new ClusterManagementServiceBuilder().build();
  }
}
