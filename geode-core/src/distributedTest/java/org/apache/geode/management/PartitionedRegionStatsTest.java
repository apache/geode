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
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class PartitionedRegionStatsTest extends JUnit4DistributedTestCase {

  private static String REGION_NAME = "testRegion";

  final Host host = Host.getHost(0);
  final VM client1 = host.getVM(1);

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withAutoStart().withRegion(RegionShortcut.PARTITION, REGION_NAME);

  @Test
  public void testGetsRate() throws InterruptedException {

    AsyncInvocation ai1 = client1.invokeAsync(() -> {

      ClientCache clientCache = createClientCache("superUser", "123", server.getPort());
      Region region = createProxyRegion(clientCache, REGION_NAME);

      for (int i = 0; i < 1000; i++) {
        region.put("key" + i, "value" + i);
        region.get("key" + i);
      }

    });

    ai1.join();

    assertThat(server.getCache()).isNotNull();
    CachePerfStats stats =
        ((PartitionedRegion) server.getCache().getRegion(REGION_NAME)).getCachePerfStats();

    assertEquals(1000, stats.getGets());

    ai1.checkException();
  }
}
