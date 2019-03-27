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
package org.apache.geode.tools.pulse.tests.ui;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import org.apache.geode.tools.pulse.tests.PulseTestLocators;
import org.apache.geode.tools.pulse.tests.rules.ScreenshotOnFailureRule;
import org.apache.geode.tools.pulse.tests.rules.ServerRule;
import org.apache.geode.tools.pulse.tests.rules.WebDriverRule;

@FixMethodOrder(MethodSorters.JVM)
public class PulseAuthorizationTest {

  @ClassRule
  public static ServerRule serverRule = new ServerRule("pulse-auth.json");

  @Rule
  public WebDriverRule webDriverRule = new WebDriverRule(serverRule.getPulseURL());

  @Rule
  public ScreenshotOnFailureRule screenshotOnFailureRule =
      new ScreenshotOnFailureRule(() -> webDriverRule.getDriver());

  @Before
  public void setup() {
    webDriverRule.getDriver().get(serverRule.getPulseURL() + "clusterDetail.html");
    PulseTestUtils.setDriverProvider(() -> webDriverRule.getDriver());
  }

  private void searchByXPathAndClick(String xpath) {
    WebElement element = webDriverRule.getDriver().findElement(By.xpath(xpath));
    assertThat(element).isNotNull();
    element.click();
  }

  // Checks whether the general options are still available in the UI.
  private void checkGeneralOptions() {
    assertThat(
        webDriverRule.getDriver().findElement(By.xpath(PulseTestLocators.TopHeader.aboutLinkXpath)))
            .isNotNull();
    assertThat(
        webDriverRule.getDriver().findElement(By.xpath(PulseTestLocators.TopHeader.helpLinkXpath)))
            .isNotNull();
    assertThat(webDriverRule.getDriver()
        .findElement(By.xpath(PulseTestLocators.TopHeader.welcomeLinkXpath))).isNotNull();
    assertThat(webDriverRule.getDriver()
        .findElement(By.xpath(PulseTestLocators.TopHeader.signOutLinkXpath))).isNotNull();
  }

  @Test
  public void authenticatedUserWithNoClusterReadPermissionShouldGetAccessDeniedPage() {
    webDriverRule.login("manager", "12345");

    // Try to access clusterView
    searchByXPathAndClick(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    String clusterMsg = webDriverRule.getDriver().findElement(By.id("errorText")).getText();
    assertThat(clusterMsg).isEqualTo("You don't have permissions to access this resource.");
    checkGeneralOptions();

    // Try to access dataBrowser
    searchByXPathAndClick(PulseTestLocators.TopNavigation.dataBrowserViewLinkXpath);
    String dataMsg = webDriverRule.getDriver().findElement(By.id("errorText")).getText();
    assertThat(dataMsg).isEqualTo("You don't have permissions to access this resource.");
    checkGeneralOptions();
  }

  @Test
  public void authenticatedUserWithClusterReadButNoDataReadPermissionShouldSeeClusterDetailsButGetAccessDeniedPageForDataBrowser() {
    webDriverRule.login("cluster", "12345");

    // Try to access clusterView
    searchByXPathAndClick(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    assertThat(webDriverRule.getDriver().findElements(By.id("errorText"))).isEmpty();
    checkGeneralOptions();

    // Try to access dataBrowser
    searchByXPathAndClick(PulseTestLocators.TopNavigation.dataBrowserViewLinkXpath);
    String dataMsg = webDriverRule.getDriver().findElement(By.id("errorText")).getText();
    assertThat(dataMsg).isEqualTo("You don't have permissions to access this resource.");
    checkGeneralOptions();

    // Go back to clusterView
    searchByXPathAndClick(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    assertThat(webDriverRule.getDriver().findElements(By.id("errorText"))).isEmpty();
    checkGeneralOptions();
  }
}
