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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.Region.SEPARATOR;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;

@Category({OQLIndexTest.class})
public class ListIndexCommandDistributedTestBase {

  private static final String REGION_1 = "REGION1";
  private static final String INDEX_REGION_NAME = SEPARATOR + "REGION1";
  private static final String INDEX_1 = "INDEX1";

  private MemberVM server;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    MemberVM locator = lsRule.startLocatorVM(0, MemberStarterRule::withHttpService);
    server = lsRule.startServerVM(1, locator.getPort());

    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<Object, Object> factory =
          cache.createRegionFactory(RegionShortcut.REPLICATE);
      Region<Object, Object> region = factory.create(REGION_1);

      cache.getQueryService().createIndex(INDEX_1, "key", INDEX_REGION_NAME);
      region.put(1, new Stock("SUNW", 10));
      region.get(1);
    });

    connectGfsh(locator);
  }

  public void connectGfsh(MemberVM vm) throws Exception {
    gfsh.connectAndVerify(vm.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void testListIndexes() {
    gfsh.executeAndAssertThat(CliStrings.LIST_INDEX).statusIsSuccess().hasTableSection()
        .hasColumn("Member Name").containsExactlyInAnyOrder(server.getName());
  }

  @Test
  public void testListIndexesWithStats() {
    gfsh.executeAndAssertThat(CliStrings.LIST_INDEX + " --with-stats").statusIsSuccess()
        .hasTableSection().hasColumn("Member Name").containsExactlyInAnyOrder(server.getName())
        .hasColumn("Updates").containsExactlyInAnyOrder("1")
        .hasColumn("Keys").containsExactlyInAnyOrder("1")
        .hasColumn("Values").containsExactlyInAnyOrder("1");
  }
}
