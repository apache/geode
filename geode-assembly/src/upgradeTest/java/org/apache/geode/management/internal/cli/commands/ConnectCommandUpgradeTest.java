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
package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@Category(GfshTest.class)
@RunWith(Parameterized.class)
public class ConnectCommandUpgradeTest {

  private String oldVersion;

  public ConnectCommandUpgradeTest(String oldVersion) {
    this.oldVersion = oldVersion;
  }

  @Parameterized.Parameters(name = "Locator Version: {0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    return result;
  }

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshRule gfshDefault = new GfshRule();

  @Test
  public void useCurrentGfshToConnectToOlderLocator() {
    assumeFalse(
        "this test can only be run with pre-9 jdk since it needs to run older version of gfsh",
        SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9));

    MemberVM oldVersionLocator = clusterStartupRule.startLocatorVM(0, oldVersion);

    if (TestVersion.compare(oldVersion, "1.10.0") < 0) { // New version gfsh could not connect to
                                                         // locators with version below 1.10.0
      GfshExecution connect = GfshScript
          .of("connect --locator=localhost[" + oldVersionLocator.getPort() + "]")
          .expectFailure()
          .execute(gfshDefault);

      assertThat(connect.getOutputText())
          .contains("Cannot use a")
          .contains("gfsh client to connect to")
          .contains("cluster.");

    } else { // From 1.10.0 new version gfsh are able to connect to old version locators
      GfshExecution connect = GfshScript
          .of("connect --locator=localhost[" + oldVersionLocator.getPort() + "]")
          .expectExitCode(0)
          .execute(gfshDefault);

      assertThat(connect.getOutputText())
          .contains("Successfully connected to:");
    }
  }

  @Test
  public void invalidHostname() {
    MemberVM oldVersionLocator = clusterStartupRule.startLocatorVM(0, oldVersion);

    GfshExecution connect = GfshScript
        .of("connect --locator=\"invalid host name[52326]\"")
        .expectFailure()
        .execute(gfshDefault);

    assertThat(connect.getOutputText())
        .doesNotContain("UnknownHostException")
        .doesNotContain("nodename nor servname")
        .contains("can't be reached. Hostname or IP address could not be found.");
  }
}
