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

import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class GfshRebalanceCompatibilityTest {

  private final String oldVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.11.0") < 0);
    return result;
  }

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  public GfshRebalanceCompatibilityTest(String oldVersion) {
    this.oldVersion = oldVersion;
  }

  @Test
  public void whenCurrentVersionLocatorsExecuteRebalanceOnOldServersThenItMustSucceed()
      throws Exception {
    MemberVM locator1 = cluster.startLocatorVM(0, oldVersion);
    int locatorPort1 = locator1.getPort();
    MemberVM locator2 =
        cluster.startLocatorVM(1, 0, oldVersion, x -> x.withConnectionToLocator(locatorPort1));
    int locatorPort2 = locator2.getPort();
    cluster
        .startServerVM(2, oldVersion, s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));
    cluster
        .startServerVM(3, oldVersion, s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));
    cluster.stop(0);
    locator1 = cluster.startLocatorVM(0, x -> x.withConnectionToLocator(locatorPort2));
    cluster.stop(1);
    int locatorPort1_v2 = locator1.getPort();
    cluster.startLocatorVM(1, x -> x.withConnectionToLocator(locatorPort1_v2));
    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("rebalance ")
        .statusIsSuccess();

  }

}
