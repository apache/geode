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

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.client.ClientCacheFactory;
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
public class ClusterStartupRuleCanSpecifyVersionsDUnitTest {
  @Parameterized.Parameter
  public String version;

  @Parameterized.Parameters
  public static List<String> versions() {
    return VersionManager.getInstance().getVersions();
  }

  @Rule
  public ClusterStartupRule csRule = new ClusterStartupRule();

  private static String getLongVersionFromShorthand(String shorthand) throws Exception {
    switch (shorthand) {
      case "000":
        return "1.5.0-SNAPSHOT";
      case "100":
        return "1.0.0-incubating";
      case "110":
        // fallthrough
      case "111":
        // fallthrough
      case "120":
        // fallthrough
      case "130":
        // fallthrough
      case "140":
        // Inject dots between each digit, e.g., 140 -> 1.4.0
        return shorthand.charAt(0) + "." + shorthand.charAt(1) + "." + shorthand.charAt(2);
      default:
        throw new Exception("Unexpected version shorthand '" + shorthand + "'");
    }
  }

  @Test
  public void locatorVersioningTest() throws Exception {
    MemberVM locator = csRule.startLocatorVM(0, new Properties(), version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getLongVersionFromShorthand(version));
  }

  @Test
  public void serverVersioningTest() throws Exception {
    MemberVM locator = csRule.startLocatorVM(0, new Properties(), version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getLongVersionFromShorthand(version));
  }

  @Test
  public void serverWithEmbeddedLocatorVersioningTest() throws Exception {
    MemberVM locator = csRule.startServerAsEmbeddedLocator(0, new Properties(), version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getLongVersionFromShorthand(version));
  }

  @Test
  public void clientVersioningTest() throws Exception {
    Consumer<ClientCacheFactory> consumer = (Serializable & Consumer<ClientCacheFactory>) cf -> {
    };
    ClientVM locator = csRule.startClientVM(0, new Properties(), consumer, version);
    String locatorVMVersion = locator.getVM().getVersion();
    String locatorActualVersion = locator.invoke(GemFireVersion::getGemFireVersion);
    assertThat(locatorVMVersion).isEqualTo(version);
    assertThat(locatorActualVersion).isEqualTo(getLongVersionFromShorthand(version));
  }
}
