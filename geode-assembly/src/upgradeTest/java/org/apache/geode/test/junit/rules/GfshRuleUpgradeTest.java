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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.version.VersionManager;

@Category(GfshTest.class)
public class GfshRuleUpgradeTest {

  private static final String GEODE_HOME = System.getenv("GEODE_HOME");
  private static final String GFSH_OLD_VERSION = "1.3.0";

  private Path gfshCurrent;
  private Path gfsh130;

  @Rule
  public GfshRule gfsh130Rule = new GfshRule(GFSH_OLD_VERSION);
  @Rule
  public GfshRule gfshCurrentRule = new GfshRule();

  @Before
  public void findGfshCurrentPath() {
    Path geodeHomePath = Paths.get(GEODE_HOME).toAbsolutePath();
    assertThat(geodeHomePath)
        .as("System.getenv(\"GEODE_HOME\")")
        .exists();

    String gfsh = isWindows() ? "gfsh.bat" : "gfsh";
    gfshCurrent = Paths.get(GEODE_HOME, "bin", gfsh);
    assertThat(gfshCurrent)
        .as("GEODE_HOME/bin/" + gfsh + " exists")
        .exists();
  }

  @Before
  public void findGfsh130Path() {
    Path geode130Home = Paths.get(VersionManager.getInstance().getInstall(GFSH_OLD_VERSION));
    assertThat(geode130Home)
        .as("VersionManager installation for " + GFSH_OLD_VERSION)
        .exists();

    String gfsh = isWindows() ? "gfsh.bat" : "gfsh";
    gfsh130 = Paths.get(geode130Home.toString(), "bin", gfsh);
    assertThat(gfsh130)
        .as("geode130Home/bin/" + gfsh + " exists")
        .exists();
  }

  @Test
  public void gfshCurrentRuleUsesCurrentGfsh() {
    assertThat(gfshCurrentRule.getGfshPath())
        .isEqualTo(gfshCurrent);
  }

  @Test
  public void gfsh130RuleUses130Gfsh() {
    assertThat(gfsh130Rule.getGfshPath())
        .isEqualTo(gfsh130);
  }
}
