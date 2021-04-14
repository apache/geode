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
package org.apache.geode.modules.distributed;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.version.VersionManager;

/**
 * This class tests cluster tcp/ip communications both with and without SSL enabled
 */
public class ModularDUnitTest implements Serializable {

   @ClassRule
   public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

    private transient static MemberVM locator;

  @Test
  public void test() {
    locator = clusterStartupRule.startLocatorVM(0, 10334, VersionManager.CURRENT_VERSION, MemberStarterRule::withHttpService);

    Region region = new Region();
    region.setName("orders");
    region.setType(RegionType.PARTITION);

    ClusterManagementService cmsClient = new ClusterManagementServiceBuilder()
        .setPort(locator.getHttpPort())
        .build();

    ClusterManagementRealizationResult result = cmsClient.create(region);
  }
}
