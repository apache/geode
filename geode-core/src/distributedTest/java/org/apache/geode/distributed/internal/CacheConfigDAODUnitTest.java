/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;



public class CacheConfigDAODUnitTest {


  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void xmlCreatedByCCServiceCanBeLoadedByServer() {
    MemberVM locator = cluster.startLocatorVM(0);

    locator.invoke(() -> {
      ConfigurationPersistenceService ccService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      ccService.updateCacheConfig("cluster", cc -> {
        RegionConfig regionConfig = new RegionConfig();
        regionConfig.setName("regionB");
        regionConfig.setType("REPLICATE");
        cc.getRegions().add(regionConfig);
        return cc;
      });
    });

    MemberVM server = cluster.startServerVM(1, locator.getPort());

    server.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getRegion("/regionB")).isNotNull();
    });

  }
}
