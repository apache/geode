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

package org.apache.geode.tools.pulse.tests.rules;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.junit.rules.ExternalResource;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

public class WebDriverRule extends ExternalResource {
  private WebDriver driver;
  static final int MAX_RETRIES = 10;
  private String pulseUrl;
  private String username;
  private String password;

  public WebDriverRule(String pulseUrl) {
    this.pulseUrl = pulseUrl;
  }

  public WebDriverRule(String username, String password, String pulseUrl) {
    this(pulseUrl);
    this.username = username;
    this.password = password;
  }

  public WebDriver getDriver() {
    return this.driver;
  }

  public String getPulseURL() {
    return pulseUrl;
  }

  @Override
  public void before() throws Throwable {
    setUpWebDriver();

    try {
      driver.get(getPulseURL() + "login.html");
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("before: driver get exception " + e.getMessage());
      throw e;
    }

    if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
      try {
        login();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("before: login exception " + e.getMessage());
        throw e;
      }
    }

    driver.navigate().refresh();
  }

  @Override
  public void after() {
    driver.quit();
  }

  private void login() {
    WebElement userNameElement = waitForElementById("user_name", 60);
    WebElement passwordElement = waitForElementById("user_password");
    userNameElement.sendKeys(username);
    passwordElement.sendKeys(password);
    passwordElement.submit();

    driver.get(getPulseURL() + "clusterDetail.html");
    WebElement userNameOnPulsePage =
        (new WebDriverWait(driver, 30, 1000)).until(
            (ExpectedCondition<WebElement>) d -> d.findElement(By.id("userName")));
    assertNotNull(userNameOnPulsePage);
  }

  private void setUpWebDriver() {
    ChromeOptions options = new ChromeOptions();
    options.addArguments("headless");
    options.addArguments("no-sandbox");
    options.addArguments("window-size=1200x600");
    driver = new ChromeDriver(options);
    driver.manage().window().maximize();
    driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
    driver.manage().timeouts().pageLoadTimeout(300, TimeUnit.SECONDS);
  }

  private WebElement waitForElementById(final String id) {
    return waitForElementById(id, 10);
  }

  private WebElement waitForElementById(final String id, int timeoutInSeconds) {
    WebElement element =
        (new WebDriverWait(driver, timeoutInSeconds, 1000))
            .until((ExpectedCondition<WebElement>) d -> d.findElement(By.id(id)));
    assertNotNull(element);
    return element;
  }
}
