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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.domain.Stock;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.json.JSONArray;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class ListIndexCommandDUnitTest {

  private static final String REGION_1 = "REGION1";
  private static final String INDEX_REGION_NAME = "/REGION1";
  private static final String INDEX_1 = "INDEX1";

  private MemberVM locator, server;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());

    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
      Region region = factory.create(REGION_1);

      cache.getQueryService().createIndex(INDEX_1, "key", INDEX_REGION_NAME);
      region.put(1, new Stock("SUNW", 10));
      region.get(1);
    });

    connectGfsh(locator);
  }

  public void connectGfsh(MemberVM vm) throws Exception {
    gfsh.connectAndVerify(vm.getJmxPort(), GfshShellConnectionRule.PortType.jmxManager);
  }

  @Test
  public void testListIndexes() throws Exception {
    CommandResult result = gfsh.executeAndVerifyCommand(CliStrings.LIST_INDEX);
    assertThat(((JSONArray) result.getContent().get("Member Name")).get(0))
        .isEqualTo(server.getName());
  }

  @Test
  public void testListIndexesWithStats() throws Exception {
    CommandResult result = gfsh.executeAndVerifyCommand(CliStrings.LIST_INDEX + " --with-stats");
    GfJsonObject content = result.getContent();
    assertThat(((JSONArray) content.get("Member Name")).get(0)).isEqualTo(server.getName());
    assertThat(((JSONArray) content.get("Updates")).get(0)).isEqualTo("1");
    assertThat(((JSONArray) content.get("Keys")).get(0)).isEqualTo("1");
    assertThat(((JSONArray) content.get("Values")).get(0)).isEqualTo("1");
  }
}
