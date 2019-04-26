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

import static org.apache.geode.test.junit.assertions.ClusterManagementResultAssert.assertManagementResult;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ServerRestartTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void name() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    cluster.startServerVM(1, locator.getPort());

    // we will stop the 2nd server so that we won't get "loss of qurom" error
    MemberVM server2 = cluster.startServerVM(2, locator.getPort());

    ClusterManagementService cmService =
        ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort());

    RegionConfig region = new RegionConfig();
    region.setName("Foo");
    region.setType(RegionType.REPLICATE);
    assertManagementResult(cmService.create(region)).hasStatusCode(
        ClusterManagementResult.StatusCode.OK);

    // force reconnect and then server should reconnect after 5 seconds
    server2.forceDisconnect();

    server2.waitTilServerFullyReconnected();
  }

}
