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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.openqa.selenium.By;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import org.apache.geode.tools.pulse.tests.JMXProperties;
import org.apache.geode.tools.pulse.tests.Member;
import org.apache.geode.tools.pulse.tests.PulseTestData;
import org.apache.geode.tools.pulse.tests.PulseTestLocators;
import org.apache.geode.tools.pulse.tests.Region;

public class PulseTestUtils {
  private static Supplier<WebDriver> driverProvider;

  public static void setDriverProvider(Supplier<WebDriver> driverProvider) {
    PulseTestUtils.driverProvider = driverProvider;
  }

  public static WebDriver getDriver() {
    if (driverProvider == null) {
      throw new IllegalStateException("No WebdriverProvider has been set.");
    } else {
      return driverProvider.get();
    }
  }

  public static int maxWaitTime = 30;
  public static int pollInterval = 1000;

  public static WebElement waitForElementWithId(String id) {
    return waitForElement(By.id(id));
  }

  public static WebElement waitForElement(By by) {
    WebElement element = (new WebDriverWait(driverProvider.get(), maxWaitTime))
        .until((ExpectedCondition<WebElement>) d -> d.findElement(by));
    assertNotNull(element);
    return element;
  }

  public static void clickElementUsingId(String id) {
    WebDriverException lastException = null;
    int attempts = 3;
    while (attempts > 0) {
      try {
        waitForElementWithId(id).click();
        return;
      } catch (StaleElementReferenceException sere) {
        lastException = sere;
      }
      attempts++;
    }

    throw lastException;
  }

  public static void clickElementUsingXpath(String xpath) {
    findElementByXpath(xpath).click();
  }

  public static void sendKeysUsingId(String Id, String textToEnter) {
    waitForElementWithId(Id).sendKeys(textToEnter);
  }

  public static WebElement findElementByXpath(String xpath) {
    return waitForElement(By.xpath(xpath));
  }

  public static String getTextUsingXpath(String xpath) {
    return findElementByXpath(xpath).getText();
  }

  public static String getTextUsingId(String id) {
    return waitForElementWithId(id).getText();
  }

  public static String getPersistanceEnabled(Region r) {
    String persistence = null;

    if (r.getPersistentEnabled()) {
      persistence = "ON";
    } else if (!r.getPersistentEnabled()) {
      persistence = "OFF";
    }
    return persistence;
  }

  public static String getPersistanceEnabled(String trueOrFalse) {
    String persistence = null;

    if (trueOrFalse.contains("true")) {
      persistence = "ON";
    } else if (trueOrFalse.contains("false")) {
      persistence = "OFF";
    }
    return persistence;
  }

  public static void validateServerGroupGridData() {
    List<WebElement> serverGridRows =
        getDriver().findElements(By.xpath("//table[@id='memberListSG']/tbody/tr"));
    int rowsCount = serverGridRows.size();
    String[][] gridDataFromUI = new String[rowsCount][7];

    for (int j = 2, x = 0; j <= serverGridRows.size(); j++, x++) {
      for (int i = 0; i <= 6; i++) {
        gridDataFromUI[x][i] = getDriver()
            .findElement(
                By.xpath("//table[@id='memberListSG']/tbody/tr[" + j + "]/td[" + (i + 1) + "]"))
            .getText();
      }
    }

    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, HashMap<String, Member>> sgMap = new HashMap<String, HashMap<String, Member>>();

    for (String member : memberNames) {
      Member thisMember = new Member(member);
      String[] sgs = thisMember.getGroups();

      for (String sgName : sgs) {
        HashMap<String, Member> sgMembers = sgMap.get(sgName);
        if (sgMembers == null) {
          sgMembers = new HashMap<String, Member>();
          sgMap.put(sgName, sgMembers);
        }
        sgMembers.put(thisMember.getMember(), thisMember);
      }
    }

    for (int i = 0; i < gridDataFromUI.length - 1; i++) {
      String sgName = gridDataFromUI[i][0];
      String memName = gridDataFromUI[i][1];
      Member m = sgMap.get(sgName).get(memName);

      assertEquals(sgName, gridDataFromUI[i][0]);
      assertEquals(memName, gridDataFromUI[i][1]);
      assertEquals(m.getMember(), gridDataFromUI[i][2]);
      assertEquals(m.getHost(), gridDataFromUI[i][3]);
      String cupUsage = String.valueOf(m.getCpuUsage());
      assertEquals(cupUsage, gridDataFromUI[i][5]);
    }

  }

  public static void validateRedundancyZonesGridData() {
    List<WebElement> rzGridRows =
        getDriver().findElements(By.xpath("//table[@id='memberListRZ']/tbody/tr"));
    int rowsCount = rzGridRows.size();
    String[][] gridDataFromUI = new String[rowsCount][7];

    for (int j = 2, x = 0; j <= rzGridRows.size(); j++, x++) {
      for (int i = 0; i <= 6; i++) {
        gridDataFromUI[x][i] = getDriver()
            .findElement(
                By.xpath("//table[@id='memberListRZ']/tbody/tr[" + j + "]/td[" + (i + 1) + "]"))
            .getText();
      }
    }

    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, HashMap<String, Member>> rzMap = new HashMap<String, HashMap<String, Member>>();

    for (String member : memberNames) {
      Member thisMember = new Member(member);
      // String[] rz = thisMember.getRedundancyZone();
      String sgName = thisMember.getRedundancyZone();

      // for (String sgName : rz) {
      HashMap<String, Member> rzMembers = rzMap.get(sgName);

      if (rzMembers == null) {
        rzMembers = new HashMap<String, Member>();
        rzMap.put(sgName, rzMembers);
      }

      rzMembers.put(thisMember.getMember(), thisMember);
      // }
    }

    for (int i = 0; i < gridDataFromUI.length - 1; i++) {
      String sgName = gridDataFromUI[i][0];
      String memName = gridDataFromUI[i][1];
      Member m = rzMap.get(sgName).get(memName);

      assertEquals(sgName, gridDataFromUI[i][0]);
      assertEquals(memName, gridDataFromUI[i][1]);
      assertEquals(m.getMember(), gridDataFromUI[i][2]);
      assertEquals(m.getHost(), gridDataFromUI[i][3]);
      String cupUsage = String.valueOf(m.getCpuUsage());
      assertEquals(cupUsage, gridDataFromUI[i][5]);
    }

  }

  public static void validateTopologyGridData() {
    List<WebElement> rzGridRows =
        getDriver().findElements(By.xpath("//table[@id='memberList']/tbody/tr"));
    int rowsCount = rzGridRows.size();
    String[][] gridDataFromUI = new String[rowsCount][8];

    for (int j = 2, x = 0; j <= rzGridRows.size(); j++, x++) {
      for (int i = 0; i <= 7; i++) {
        gridDataFromUI[x][i] = getDriver()
            .findElement(
                By.xpath("//table[@id='memberList']/tbody/tr[" + j + "]/td[" + (i + 1) + "]"))
            .getText();
      }
    }

    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, Member> tpMap = new HashMap<String, Member>();

    for (String member : memberNames) {
      Member thisMember = new Member(member);
      tpMap.put(thisMember.getMember(), thisMember);

    }

    for (int i = 0; i < gridDataFromUI.length - 1; i++) {

      String memName = gridDataFromUI[i][0];
      Member m = tpMap.get(memName);

      assertEquals(m.getMember(), gridDataFromUI[i][0]);
      assertEquals(m.getMember(), gridDataFromUI[i][1]);
      assertEquals(m.getHost(), gridDataFromUI[i][2]);
      String cupUsage = String.valueOf(m.getCpuUsage());
      assertEquals(cupUsage, gridDataFromUI[i][5]);
    }
  }

  public static void validateDataPrespectiveGridData() {
    List<WebElement> serverGridRows =
        getDriver().findElements(By.xpath("//table[@id='regionsList']/tbody/tr"));
    int rowsCount = serverGridRows.size();
    String[][] gridDataFromUI = new String[rowsCount][7];

    for (int j = 2, x = 0; j <= serverGridRows.size(); j++, x++) {
      for (int i = 0; i <= 6; i++) {
        if (i < 5) {
          gridDataFromUI[x][i] = getDriver()
              .findElement(
                  By.xpath("//table[@id='regionsList']/tbody/tr[" + j + "]/td[" + (i + 1) + "]"))
              .getText();
        } else if (i == 5) {
          gridDataFromUI[x][i] = getDriver()
              .findElement(
                  By.xpath("//table[@id='regionsList']/tbody/tr[" + j + "]/td[" + (i + 4) + "]"))
              .getText();
        }
      }
    }

    String[] regionNames = JMXProperties.getInstance().getProperty("regions").split(" ");
    HashMap<String, Region> dataMap = new HashMap<String, Region>();

    for (String region : regionNames) {
      Region thisRegion = new Region(region);
      dataMap.put(thisRegion.getName(), thisRegion);

    }

    for (int i = 0; i < gridDataFromUI.length - 1; i++) {
      String memName = gridDataFromUI[i][0];
      Region r = dataMap.get(memName);

      assertEquals(r.getName(), gridDataFromUI[i][0]);
      assertEquals(r.getRegionType(), gridDataFromUI[i][1]);

      assertEquals(String.valueOf(r.getSystemRegionEntryCount()), gridDataFromUI[i][2]);
      assertEquals(r.getFullPath(), gridDataFromUI[i][4]);
      assertEquals(getPersistanceEnabled(r), gridDataFromUI[i][5]);
    }
  }

  public static void validateRegionDetailsGridData() {
    List<WebElement> serverGridRows =
        getDriver().findElements(By.xpath("//table[@id='memberList']/tbody/tr"));
    int rowsCount = serverGridRows.size();
    String[][] gridDataFromUI = new String[rowsCount][7];

    for (int j = 2, x = 0; j <= serverGridRows.size(); j++, x++) {
      for (int i = 0; i < 2; i++) {
        gridDataFromUI[x][i] = getDriver()
            .findElement(
                By.xpath("//table[@id='memberList']/tbody/tr[" + j + "]/td[" + (i + 1) + "]"))
            .getText();
      }
    }

    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, Member> tpMap = new HashMap<String, Member>();

    for (String member : memberNames) {
      Member thisMember = new Member(member);
      tpMap.put(thisMember.getMember(), thisMember);
    }

    for (int i = 0; i < gridDataFromUI.length - 1; i++) {

      String memName = gridDataFromUI[i][0];
      Member m = tpMap.get(memName);
      assertEquals(m.getMember(), gridDataFromUI[i][0]);
    }

  }

  public static void navigateToToplogyView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.TopologyView.radioButtonXpath);
  }

  // ------ Topology / Server Group / Redundancy Group - Tree View

  public static void navigateToTopologyTreeView() {
    navigateToToplogyView();
    clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
  }

  public static void navigateToServerGroupTreeView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.ServerGroups.radioButtonXpath);
  }

  public static void navigateToRedundancyZonesTreeView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.RedundancyZone.radioButtonXpath);
  }

  // ------ Topology / Server Group / Redundancy Group - Grid View

  public static void navigateToTopologyGridView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.TopologyView.radioButtonXpath);
    clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
  }

  public static void navigateToServerGroupGridView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.ServerGroups.radioButtonXpath);
    clickElementUsingId(PulseTestLocators.ServerGroups.gridButtonId);
  }

  public static void navigateToRedundancyZonesGridView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.RedundancyZone.radioButtonXpath);
    clickElementUsingId(PulseTestLocators.RedundancyZone.gridButtonId);
  }

  // ----- Data perspective / region details

  public static void navigateToDataPrespectiveGridView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.downarrowButtonXpath);
    clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.dataViewButtonXpath);
    clickElementUsingId(PulseTestLocators.DataPerspectiveView.gridButtonId);
  }

  public static void navigateToRegionDetailsView() {
    clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.downarrowButtonXpath);
    clickElementUsingXpath(PulseTestLocators.DataPerspectiveView.dataViewButtonXpath);
    // clickElementUsingXpath(PulseTestLocators.RegionDetailsView.regionNameXpath);
    // // WIP - region navigation defect needs to fixed
    clickElementUsingXpath(PulseTestLocators.RegionDetailsView.treeMapCanvasXpath);
  }

  public static void navigateToRegionDetailsGridView() {
    navigateToRegionDetailsView();
    clickElementUsingXpath(PulseTestLocators.RegionDetailsView.gridButtonXpath);
  }

  public static String getPropertyValue(String propertyKey) {
    String propertyValue = JMXProperties.getInstance().getProperty(propertyKey);
    return propertyValue;
  }

  public static void verifyElementPresentById(String id) {
    WebDriverWait wait = new WebDriverWait(getDriver(), maxWaitTime, pollInterval);
    wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.id(id)));
  }

  public static void verifyElementPresentByLinkText(String lnkText) {
    WebDriverWait wait = new WebDriverWait(getDriver(), maxWaitTime, pollInterval);
    wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.linkText(lnkText)));
  }

  public static void verifyElementPresentByXpath(String xpath) {
    WebDriverWait wait = new WebDriverWait(getDriver(), maxWaitTime, pollInterval);
    wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.xpath(xpath)));
  }

  public static void verifyTextPresrntById(String id, String text) {
    WebDriverWait wait = new WebDriverWait(getDriver(), maxWaitTime, pollInterval);
    wait.until(ExpectedConditions.textToBePresentInElementLocated(By.id(id), text));
  }

  public static void verifyTextPresrntByXpath(String xpath, String text) {
    WebDriverWait wait = new WebDriverWait(getDriver(), maxWaitTime, pollInterval);
    wait.until(ExpectedConditions.textToBePresentInElementLocated(By.xpath(xpath), text));
  }

  public static void verifyElementAttributeById(String id, String attribute, String value) {
    String actualValue = waitForElementWithId(id).getAttribute(attribute);
    assertTrue(actualValue.equals(value) || actualValue.contains(value));
  }


  public static void mouseReleaseById(String id) {
    verifyElementPresentById(id);
    Actions action = new Actions(getDriver());
    WebElement we = getDriver().findElement(By.id(id));
    action.moveToElement(we).release().perform();
  }

  public static void mouseClickAndHoldOverElementById(String id) {
    verifyElementPresentById(id);
    Actions action = new Actions(getDriver());
    WebElement we = getDriver().findElement(By.id(id));
    action.moveToElement(we).clickAndHold().perform();
  }

  public static String[] splitString(String stringToSplit, String splitDelimiter) {
    String[] stringArray = stringToSplit.split(splitDelimiter);
    return stringArray;
  }

  public static void assertMemberSortingByCpuUsage() {
    Map<Double, String> memberMap = new TreeMap<>(Collections.reverseOrder());
    String[] membersNames = splitString(JMXProperties.getInstance().getProperty("members"), " ");
    for (String member : membersNames) {
      Member thisMember = new Member(member);
      memberMap.put(thisMember.getCpuUsage(), thisMember.getMember());
    }
    for (Map.Entry<Double, String> entry : memberMap.entrySet()) {
      // here matching painting style to validation that the members are painted according to their
      // cpu usage
      String refMemberCPUUsage = null;
      if (entry.getValue().equalsIgnoreCase("M1")) {
        refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM1;
      } else if (entry.getValue().equalsIgnoreCase("M2")) {
        refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM2;
      } else {
        refMemberCPUUsage = PulseTestData.Topology.cpuUsagePaintStyleM3;
      }
      assertTrue(
          waitForElementWithId(entry.getValue()).getAttribute("style").contains(refMemberCPUUsage));
    }
  }

  public static void assertMemberSortingByHeapUsage() {
    Map<Long, String> memberMap = new TreeMap<Long, String>(Collections.reverseOrder());
    String[] membersNames = splitString(JMXProperties.getInstance().getProperty("members"), " ");
    for (String member : membersNames) {
      Member thisMember = new Member(member);
      memberMap.put(thisMember.getCurrentHeapSize(), thisMember.getMember());
    }
    for (Map.Entry<Long, String> entry : memberMap.entrySet()) {
      // here matching painting style to validation that the members are painted according to their
      // cpu usage
      String refMemberHeapUsage = null;
      if (entry.getValue().equalsIgnoreCase("M1")) {
        refMemberHeapUsage = PulseTestData.Topology.heapUsagePaintStyleM1;
      } else if (entry.getValue().equalsIgnoreCase("M2")) {
        refMemberHeapUsage = PulseTestData.Topology.heapUsagePaintStyleM2;
      } else {
        refMemberHeapUsage = PulseTestData.Topology.heapUsagePaintStyleM3;
      }
      assertTrue(waitForElementWithId(entry.getValue()).getAttribute("style")
          .contains(refMemberHeapUsage));
    }
  }


  public static void assertMemberSortingBySgHeapUsage() {
    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, HashMap<String, Member>> sgMap = new HashMap<String, HashMap<String, Member>>();
    for (String member : memberNames) {
      Member thisMember = new Member(member);
      String[] sgs = thisMember.getGroups();

      for (String sgName : sgs) {
        HashMap<String, Member> sgMembers = sgMap.get(sgName);
        if (sgMembers == null) {
          sgMembers = new HashMap<String, Member>();
          sgMap.put(sgName, sgMembers);
        }
        sgMembers.put(thisMember.getMember(), thisMember);
      }
    }
    Map<Float, String> memberMap = new TreeMap<Float, String>(Collections.reverseOrder());

    for (int sgId = 1; sgId <= 3; sgId++) {
      String sgName = "SG1";
      String memName = "M" + sgId;
      Member m = sgMap.get(sgName).get(memName);
      memberMap.put((float) m.getCurrentHeapSize(), m.getMember());
    }

    for (Map.Entry<Float, String> entry : memberMap.entrySet()) {
      // here matching painting style to validation that the members are painted according to their
      // cpu usage
      String refMemberCPUUsage = null;
      if (entry.getValue().equalsIgnoreCase("M1")) {
        refMemberCPUUsage = PulseTestData.ServerGroups.heapUsagePaintStyleSG1M1;
      } else if (entry.getValue().equalsIgnoreCase("M2")) {
        refMemberCPUUsage = PulseTestData.ServerGroups.heapUsagePaintStyleSG1M2;
      } else {
        refMemberCPUUsage = PulseTestData.ServerGroups.heapUsagePaintStyleSG1M3;
      }
      assertTrue(waitForElementWithId("SG1(!)" + entry.getValue()).getAttribute("style")
          .contains(refMemberCPUUsage));
    }
  }



  public static void assertMemberSortingBySgCpuUsage() {
    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, HashMap<String, Member>> sgMap = new HashMap<String, HashMap<String, Member>>();
    for (String member : memberNames) {
      Member thisMember = new Member(member);
      String[] sgs = thisMember.getGroups();

      for (String sgName : sgs) {
        HashMap<String, Member> sgMembers = sgMap.get(sgName);
        if (sgMembers == null) {
          sgMembers = new HashMap<String, Member>();
          sgMap.put(sgName, sgMembers);
        }
        sgMembers.put(thisMember.getMember(), thisMember);
      }
    }
    Map<Double, String> memberMap = new TreeMap<>(Collections.reverseOrder());
    // SG3(!)M3
    for (int sgId = 1; sgId <= 3; sgId++) {
      String sgName = "SG1";
      String memName = "M" + sgId;
      Member m = sgMap.get(sgName).get(memName);
      memberMap.put(m.getCpuUsage(), m.getMember());
    }

    for (Map.Entry<Double, String> entry : memberMap.entrySet()) {
      // here matching painting style to validation that the members are painted according to their
      // cpu usage
      String refMemberCPUUsage = null;
      if (entry.getValue().equalsIgnoreCase("M1")) {
        refMemberCPUUsage = PulseTestData.ServerGroups.cpuUsagePaintStyleSG1M1;
      } else if (entry.getValue().equalsIgnoreCase("M2")) {
        refMemberCPUUsage = PulseTestData.ServerGroups.cpuUsagePaintStyleSG1M2;
      } else {
        refMemberCPUUsage = PulseTestData.ServerGroups.cpuUsagePaintStyleSG1M3;
      }
      assertTrue(waitForElementWithId("SG1(!)" + entry.getValue()).getAttribute("style")
          .contains(refMemberCPUUsage));
    }
  }

  public static void assertMemberSortingByRzHeapUsage() {
    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, HashMap<String, Member>> rzMap = new HashMap<String, HashMap<String, Member>>();
    for (String member : memberNames) {
      Member thisMember = new Member(member);
      String sgName = thisMember.getRedundancyZone();
      HashMap<String, Member> rzMembers = rzMap.get(sgName);

      if (rzMembers == null) {
        rzMembers = new HashMap<String, Member>();
        rzMap.put(sgName, rzMembers);
      }

      rzMembers.put(thisMember.getMember(), thisMember);
    }
    Map<Float, String> memberMap = new TreeMap<Float, String>(Collections.reverseOrder());
    String rzName = "RZ1 RZ2";
    String memName = "M1";
    Member m = rzMap.get(rzName).get(memName);
    memberMap.put((float) m.getCurrentHeapSize(), m.getMember());

    for (Map.Entry<Float, String> entry : memberMap.entrySet()) {
      // here matching painting style to validation that the members are painted according to their
      // cpu usage
      String refMemberHeapUsage = null;
      if (entry.getValue().equalsIgnoreCase("M1")) {
        refMemberHeapUsage = PulseTestData.RedundancyZone.heapUsagePaintStyleRZ1RZ2M1;
      } else if (entry.getValue().equalsIgnoreCase("M2")) {
        refMemberHeapUsage = PulseTestData.RedundancyZone.heapUsagePaintStyleRZ1RZ2M2;
      } else {
        refMemberHeapUsage = PulseTestData.RedundancyZone.heapUsagePaintStyleRZ3M3;
      }
      assertTrue(waitForElementWithId("RZ1 RZ2(!)" + entry.getValue()).getAttribute("style")
          .contains(refMemberHeapUsage));
    }
  }

  public static void assertMemeberSortingByRzCpuUsage() {
    String[] memberNames = JMXProperties.getInstance().getProperty("members").split(" ");
    HashMap<String, HashMap<String, Member>> rzMap = new HashMap<String, HashMap<String, Member>>();
    for (String member : memberNames) {
      Member thisMember = new Member(member);
      String sgName = thisMember.getRedundancyZone();
      HashMap<String, Member> rzMembers = rzMap.get(sgName);

      if (rzMembers == null) {
        rzMembers = new HashMap<String, Member>();
        rzMap.put(sgName, rzMembers);
      }

      rzMembers.put(thisMember.getMember(), thisMember);
    }
    Map<Double, String> memberMap = new TreeMap<>(Collections.reverseOrder());
    String rzName = "RZ1 RZ2";
    String memName = "M1";
    Member m = rzMap.get(rzName).get(memName);
    memberMap.put(m.getCpuUsage(), m.getMember());

    for (Map.Entry<Double, String> entry : memberMap.entrySet()) {
      // here matching painting style to validation that the members are painted according to their
      // cpu usage
      String refMemberCPUUsage = null;
      if (entry.getValue().equalsIgnoreCase("M1")) {
        refMemberCPUUsage = PulseTestData.RedundancyZone.cpuUsagePaintStyleRZ1RZ2M1;
      } else if (entry.getValue().equalsIgnoreCase("M2")) {
        refMemberCPUUsage = PulseTestData.RedundancyZone.cpuUsagePaintStyleRZ1RZ2M2;
      }
      assertTrue(waitForElementWithId("RZ1 RZ2(!)" + entry.getValue()).getAttribute("style")
          .contains(refMemberCPUUsage));
    }
  }

  public static List<WebElement> getRegionsFromDataBrowser() {
    List<WebElement> regionList = getDriver()
        .findElements(By.xpath("//span[starts-with(@ID,'treeDemo_')][contains(@id,'_span')]"));
    return regionList;
  }
}
