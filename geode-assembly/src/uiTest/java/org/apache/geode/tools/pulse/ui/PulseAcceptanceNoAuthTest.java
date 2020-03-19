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
 *
 */
package org.apache.geode.tools.pulse.ui;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.openqa.selenium.WebDriver;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.rules.EmbeddedPulseRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.tests.rules.ScreenshotOnFailureRule;
import org.apache.geode.tools.pulse.tests.rules.WebDriverRule;

public class PulseAcceptanceNoAuthTest extends PulseAcceptanceTestBase {

  @ClassRule
  public static LocatorStarterRule locator =
      new LocatorStarterRule().withJMXManager().withHttpService().withAutoStart();

  @ClassRule
  public static ClusterStartupRule clusterRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public EmbeddedPulseRule pulseRule = new EmbeddedPulseRule();

  @Rule
  public WebDriverRule webDriverRule = new WebDriverRule("admin", "admin", getPulseURL());

  @Rule
  public ScreenshotOnFailureRule screenshotOnFailureRule =
      new ScreenshotOnFailureRule(this::getWebDriver);

  private Cluster cluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    int locatorPort = locator.getPort();
    clusterRule.startServerVM(1, locatorPort);

    gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat("create region --name=FOO --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("put --key=A --value=APPLE --region=/FOO");
  }

  @AfterClass
  public static void afterClass() {
    gfsh.executeAndAssertThat("destroy region --name=FOO").statusIsSuccess();
  }

  @Before
  public void before() {
    pulseRule.useJmxManager("localhost", locator.getJmxPort());
    cluster = pulseRule.getRepository().getClusterWithUserNameAndPassword("admin", null);
  }

  @Override
  public WebDriver getWebDriver() {
    return webDriverRule.getDriver();
  }

  @Override
  public String getPulseURL() {
    return "http://localhost:" + locator.getHttpPort() + "/pulse/";
  }

  @Override
  public Cluster getCluster() {
    return this.cluster;
  }
}
