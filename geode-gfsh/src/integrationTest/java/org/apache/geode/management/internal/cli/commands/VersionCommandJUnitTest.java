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

import static org.apache.geode.internal.SystemDescription.RUNNING_ON;
import static org.apache.geode.internal.VersionDescription.BUILD_ID;
import static org.apache.geode.internal.VersionDescription.BUILD_JAVA_VERSION;
import static org.apache.geode.internal.VersionDescription.BUILD_PLATFORM;
import static org.apache.geode.internal.VersionDescription.PRODUCT_NAME;
import static org.apache.geode.internal.VersionDescription.PRODUCT_VERSION;
import static org.apache.geode.internal.VersionDescription.SOURCE_DATE;
import static org.apache.geode.internal.VersionDescription.SOURCE_REPOSITORY;
import static org.apache.geode.internal.VersionDescription.SOURCE_REVISION;
import static org.assertj.core.api.Assertions.assertThat;

import junitparams.Parameters;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({GfshTest.class})
@RunWith(GeodeParamsRunner.class)
public class VersionCommandJUnitTest {
  private static final String[] EXPECTED_FULL_DATA =
      {BUILD_ID, BUILD_JAVA_VERSION, BUILD_PLATFORM, PRODUCT_NAME, PRODUCT_VERSION,
          SOURCE_DATE, SOURCE_REPOSITORY, SOURCE_REVISION, RUNNING_ON};

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  @Parameters({"version", "version --full=false"})
  public void versionShort(String versionCommand) throws Exception {
    String result = gfsh.execute(versionCommand);
    assertThat(result).contains(GemFireVersion.getGemFireVersion());
  }

  @Test
  @Parameters({"version", "version --full=false"})
  public void versionShortConnected(String versionCommand) throws Exception {
    gfsh.connectAndVerify(locator);
    // Behavior should be the same while connected
    versionShort(versionCommand);
  }

  @Test
  @Parameters({"version --full", "version --full=true"})
  public void versionFull(String versionCommand) throws Exception {
    String result = gfsh.execute(versionCommand);
    for (String datum : EXPECTED_FULL_DATA) {
      assertThat(result).contains(datum);
    }
  }

  @Test
  @Parameters({"version --full", "version --full=true"})
  public void versionFullConnected(String versionCommand) throws Exception {
    gfsh.connectAndVerify(locator);
    // Behavior should be the same while connected
    versionFull(versionCommand);
  }
}
