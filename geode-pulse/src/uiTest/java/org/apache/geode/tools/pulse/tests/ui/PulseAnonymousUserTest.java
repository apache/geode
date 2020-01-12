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

import org.apache.geode.tools.pulse.tests.rules.ScreenshotOnFailureRule;
import org.apache.geode.tools.pulse.tests.rules.ServerRule;
import org.apache.geode.tools.pulse.tests.rules.WebDriverRule;

@FixMethodOrder(MethodSorters.JVM)
public class PulseAnonymousUserTest {

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
  }

  @Test
  public void userCanGetToPulseLoginPage() {
    webDriverRule.getDriver().get(serverRule.getPulseURL() + "login.html");

    WebElement userNameElement = webDriverRule.getDriver().findElement(By.id("user_name"));
    WebElement passwordElement = webDriverRule.getDriver().findElement(By.id("user_password"));

    assertThat(userNameElement).isNotNull();
    assertThat(passwordElement).isNotNull();
  }

  @Test
  public void userCanGetToPulseDetails() {
    webDriverRule.getDriver().get(serverRule.getPulseURL() + "pulseVersion");

    // pulseVersion is visible to public
    assertThat(webDriverRule.getDriver().getPageSource()).contains("sourceRevision");
  }
}
