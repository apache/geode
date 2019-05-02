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

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class RegionManagementOnMultipleGroup {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void createRegion() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0, l -> l.withHttpService());

    cluster.startServerVM(1, "group1", locator.getPort());
    cluster.startServerVM(2, "group2", locator.getPort());
    cluster.startServerVM(3, "group1,group2", locator.getPort());

    ClusterManagementService cms =
        ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort());
    RegionConfig region = new RegionConfig();
    region.setName("test");
    region.setGroup("group1");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult result = cms.create(region);
    assertManagementResult(result).isSuccessful().hasMemberStatus().containsOnlyKeys("server-1",
        "server-3");

    // create the same region on group2 will be successful even though the region already exists
    // on one member
    region.setGroup("group2");
    assertManagementResult(cms.create(region)).isSuccessful().hasMemberStatus()
        .containsOnlyKeys("server-2", "server-3");
  }
}
