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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StatusClusterConfigServiceCommandDUnitTest {
  private static MemberVM locator1, locator2;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator1 = cluster.startLocatorVM(0, 0);
    locator2 = cluster.startLocatorVM(1, locator1.getPort());

    gfsh.connectAndVerify(locator1);
  }

  @Test
  public void testStatusClusterConfigService() {
    gfsh.executeAndAssertThat("status cluster-config-service")
        .statusIsSuccess()
        .tableHasRowCount(2)
        .hasTableSection().hasColumn("Name").containsOnly(locator1.getName(), locator2.getName())
        .hasColumn("Status").containsOnly("RUNNING", "RUNNING");

    locator2.stop();

    gfsh.executeAndAssertThat("status cluster-config-service")
        .statusIsSuccess()
        .tableHasRowCount(1)
        .hasTableSection().hasColumn("Name").containsOnly(locator1.getName()).hasColumn("Status")
        .containsOnly("RUNNING");
  }
}
