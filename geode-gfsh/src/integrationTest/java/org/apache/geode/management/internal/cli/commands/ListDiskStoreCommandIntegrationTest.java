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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.GfshCommandRule.PortType;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({PersistenceTest.class})
public class ListDiskStoreCommandIntegrationTest {
  private static final String REGION_NAME = "test-region";
  private static final String MEMBER_NAME = "testServer";
  private static final String DISK_STORE_NAME = "testDiskStore";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withRegion(RegionShortcut.REPLICATE, REGION_NAME)
          .withName(MEMBER_NAME).withJMXManager().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule().withTimeout(1);

  @Test
  @SuppressWarnings("deprecation")
  public void commandSucceedsWhenConnected() throws Exception {
    Cache cache = server.getCache();
    DiskStore ds = cache.createDiskStoreFactory().create(DISK_STORE_NAME);

    gfsh.connectAndVerify(server.getJmxPort(), PortType.jmxManager);
    gfsh.executeAndAssertThat("list disk-stores").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Member Name", MEMBER_NAME)
        .tableHasColumnWithValuesContaining("Member Id", server.getCache().getMyId().getId())
        .tableHasColumnWithValuesContaining("Disk Store Name", DISK_STORE_NAME)
        .tableHasColumnWithValuesContaining("Disk Store ID", ds.getDiskStoreUUID().toString());
  }

  @Test
  public void commandFailsWhenNotConnected() {
    gfsh.executeAndAssertThat("list disk-stores").statusIsError().containsOutput("Command",
        "was found but is not currently available");
  }
}
