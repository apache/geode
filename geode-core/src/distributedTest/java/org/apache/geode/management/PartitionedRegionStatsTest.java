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
package org.apache.geode.management;

import static org.apache.geode.security.SecurityTestUtil.createClientCache;
import static org.apache.geode.security.SecurityTestUtil.createProxyRegion;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class PartitionedRegionStatsTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "testRegion";
  private final int numOfEntries = 10;

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withAutoStart().withRegion(RegionShortcut.PARTITION, REGION_NAME);

  @Test
  public void testGetsRate() throws Exception {

    client1.invoke(() -> {

      ClientCache clientCache = createClientCache("superUser", "123", server.getPort());
      Region region = createProxyRegion(clientCache, REGION_NAME);

      for (int i = 1; i < numOfEntries; i++) {
        region.put("key" + i, "value" + i);
        region.get("key" + i);
      }
      region.get("key" + numOfEntries);

    });

    assertThat(server.getCache()).isNotNull();

    CachePerfStats regionStats =
        ((PartitionedRegion) server.getCache().getRegion(REGION_NAME)).getCachePerfStats();

    CachePerfStats cacheStats = server.getCache().getCachePerfStats();

    assertThat(regionStats.getGets()).isEqualTo(numOfEntries);
    assertThat(regionStats.getMisses()).isEqualTo(1);

    assertThat(cacheStats.getGets()).isEqualTo(numOfEntries);
    assertThat(cacheStats.getMisses()).isEqualTo(1);

  }
}
