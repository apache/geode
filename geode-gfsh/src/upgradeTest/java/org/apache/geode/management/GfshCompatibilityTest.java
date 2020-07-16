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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
public class GfshCompatibilityTest {
  private final String oldVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    return result;
  }

  public GfshCompatibilityTest(String oldVersion) {
    this.oldVersion = oldVersion;
  }

  private MemberVM oldLocator;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void currentGfshConnectToOlderVersionsOfLocator() throws Exception {
    oldLocator = cluster.startLocatorVM(0, oldVersion);
    int locatorPort = oldLocator.getPort();
    cluster.startServerVM(1, oldVersion,
        s -> s.withConnectionToLocator(locatorPort));
    // pre 1.10, it should not be able to connect
    if (TestVersion.compare(oldVersion, "1.5.0") < 0) {
      gfsh.connect(oldLocator.getPort(), GfshCommandRule.PortType.locator);
      assertThat(gfsh.isConnected()).isFalse();
      assertThat(gfsh.getGfshOutput()).contains("Cannot use a")
          .contains("gfsh client to connect to this cluster.");
    } else if (TestVersion.compare(oldVersion, "1.10.0") < 0) {
      gfsh.connect(oldLocator.getPort(), GfshCommandRule.PortType.locator);
      assertThat(gfsh.isConnected()).isFalse();
      assertThat(gfsh.getGfshOutput()).contains("Cannot use a")
          .contains("gfsh client to connect to a " + oldVersion + " cluster.");
    }
    // post 1.10 (including) should connect and be able to execute command
    else {
      gfsh.connectAndVerify(oldLocator);
      assertThat(gfsh.getGfshOutput())
          .contains("You are connected to a cluster of version: " + oldVersion);
      gfsh.executeAndAssertThat("list members")
          .statusIsSuccess();
    }
  }

}
