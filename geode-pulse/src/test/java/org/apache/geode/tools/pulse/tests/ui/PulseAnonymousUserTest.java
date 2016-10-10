/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.geode.tools.pulse.tests.ui;

import static org.assertj.core.api.Assertions.*;


import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.management.internal.JettyHelper;
import org.apache.geode.test.junit.categories.UITest;
import org.apache.geode.tools.pulse.tests.PulseTestLocators;
import org.apache.geode.tools.pulse.tests.Server;

@Category(UITest.class)
@FixMethodOrder(MethodSorters.JVM)
public class PulseAnonymousUserTest {

  private static String path;

  private static org.eclipse.jetty.server.Server jetty = null;
  private static Server server = null;
  private static String pulseURL = null;
  public static WebDriver driver;

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    setUpServer("pulseUser", "12345", "pulse-auth.json");
  }

  @Before
  public void setup(){
    driver.get(pulseURL + "/clusterDetail.html");
  }

  @Test
  public void userCanGetToPulseLoginPage() {
    driver.get(pulseURL + "/Login.html");
    System.err.println("Pulse url: " + pulseURL);
    System.err.println(driver.getPageSource().toString());

    WebElement userNameElement = driver.findElement(By.id("user_name"));
    WebElement passwordElement = driver.findElement(By.id("user_password"));

    assertThat(userNameElement).isNotNull();
    assertThat(passwordElement).isNotNull();
  }

  @Test
  public void userCannotGetToPulseDetails() {
    driver.get(pulseURL + "/pulse/pulseVersion");

    assertThat(driver.getPageSource()).doesNotContain("sourceRevision");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    driver.close();
    jetty.stop();
  }


  public static void setUpServer(String username, String password, String jsonAuthFile) throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    final String jmxPropertiesFile = classLoader.getResource("test.properties").getPath();
    path = getPulseWarPath();
    server = Server.createServer(9999, jmxPropertiesFile, jsonAuthFile);

    String host = "localhost";
    int port = 8080;
    String context = "/pulse";

    jetty = JettyHelper.initJetty(host, port, new SSLConfig());
    JettyHelper.addWebApplication(jetty, context, getPulseWarPath());
    jetty.start();

    pulseURL = "http://" + host + ":" + port + context;

    Awaitility.await().until(() -> jetty.isStarted());

    setUpWebDriver();
  }

  private static void setUpWebDriver() {
    DesiredCapabilities capabilities = new DesiredCapabilities();
    capabilities.setJavascriptEnabled(true);
    capabilities.setCapability("takesScreenshot", true);
    capabilities.setCapability("phantomjs.page.settings.userAgent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:16.0) Gecko/20121026 Firefox/16.0");

    driver = new PhantomJSDriver(capabilities);
    driver.manage().window().maximize();
    driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
  }

  public static String getPulseWarPath() throws Exception {
    String warPath = null;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("GemFireVersion.properties");
    Properties properties = new Properties();
    properties.load(inputStream);
    String version = properties.getProperty("Product-Version");
    warPath = "geode-pulse-" + version + ".war";
    String propFilePath = classLoader.getResource("GemFireVersion.properties").getPath();
    warPath = propFilePath.substring(0, propFilePath.indexOf("generated-resources")) + "libs/" + warPath;
    return warPath;
  }

  protected void searchByXPathAndClick(String xpath) {
    WebElement element = driver.findElement(By.xpath(xpath));
    assertThat(element).isNotNull();
    element.click();
  }
}
