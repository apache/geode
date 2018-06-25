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

package org.apache.geode.test.dunit.rules.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClusterStartupRuleCanSpecifyOlderVersionsDUnitTest {
  @Parameterized.Parameter
  public String version;

  @Parameterized.Parameters(name = "version={0}")
  public static List<String> versions() {
    return VersionManager.getInstance().getVersionsWithoutCurrent();
  }

  @Rule
  public ClusterStartupRule csRule = new ClusterStartupRule();

  @Test
  public void locatorVersioningTest() throws Exception {
    MemberVM locator = csRule.startLocatorVM(0, version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getDottedVersionString(version));
  }

  @Test
  public void serverVersioningTest() throws Exception {
    MemberVM locator = csRule.startLocatorVM(0, version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getDottedVersionString(version));
  }

  @Test
  public void serverWithEmbeddedLocatorVersioningTest() throws Exception {
    MemberVM locator =
        csRule.startServerVM(0, version, x -> x.withEmbeddedLocator().withJMXManager());
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getDottedVersionString(version));
  }

  @Test
  public void clientVersioningTest() throws Exception {
    ClientVM locator = csRule.startClientVM(0, new Properties(), (cf) -> {
    }, version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getDottedVersionString(version));
  }

  private static String getDottedVersionString(String vmVersionShorthand) throws Exception {
    if (vmVersionShorthand.equals("100")) {
      return "1.0.0-incubating";
    } else {
      return vmVersionShorthand.charAt(0) + "." + vmVersionShorthand.charAt(1) + "."
          + vmVersionShorthand.charAt(2);
    }
  }
}
