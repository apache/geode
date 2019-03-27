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

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;

public class GeodeClientClusterManagementSecurityTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(1);

  @ClassRule
  public static ClientCacheRule client = new ClientCacheRule();

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withSecurityManager(
        SimpleSecurityManager.class));
    client.withLocatorConnection(locator.getPort()).withCredential("cluster", "cluster")
        .createCache();
  }

  @Test
  public void withClientConnectionCredential() throws Exception {
    assertThat(ClusterManagementServiceProvider.getService().isConnected()).isTrue();
  }

  @Test
  public void withDifferentCredentials() throws Exception {
    assertThat(ClusterManagementServiceProvider.getService("test", "test").isConnected()).isTrue();
  }

  @Test
  public void withInvalidCredential() throws Exception {
    assertThat(ClusterManagementServiceProvider.getService("test", "wrong").isConnected())
        .isFalse();
  }
}
