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

import static org.apache.geode.tools.pulse.internal.data.PulseConstants.TWO_PLACE_DECIMAL_FORMAT;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_CLIENTS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_FUNCTIONS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_GCPAUSES_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_QUERIESPERSEC_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_READPERSEC_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_SUBSCRIPTION_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_UNIQUECQS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_GRID_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_LOCATORS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_MEMBERS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_VIEW_REGIONS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.CLUSTER_WRITEPERSEC_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_COLOCATED_REGION;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_COLOCATED_REGION_NAME1;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_COLOCATED_REGION_NAME2;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_COLOCATED_REGION_NAME3;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGION1_CHECKBOX;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGION2_CHECKBOX;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGION3_CHECKBOX;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGIONName1;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGIONName2;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_BROWSER_REGIONName3;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_DROPDOWN_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_VIEW_EMPTYNODES;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_VIEW_ENTRYCOUNT;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_VIEW_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_VIEW_READPERSEC;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_VIEW_USEDMEMORY;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.DATA_VIEW_WRITEPERSEC;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_CPUUSAGE_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_JVMPAUSES_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_LOADAVG_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_OFFHEAPFREESIZE_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_OFFHEAPUSEDSIZE_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_READPERSEC_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_REGION_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_SOCKETS_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_THREAD_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.MEMBER_VIEW_WRITEPERSEC_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.QUERY_STATISTICS_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.REDUNDANCY_GRID_ID;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.REGION_NAME_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.REGION_PATH_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.REGION_PERSISTENCE_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.REGION_TYPE_LABEL;
import static org.apache.geode.tools.pulse.tests.ui.PulseTestConstants.SERVER_GROUP_GRID_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.DecimalFormat;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;

import org.apache.geode.tools.pulse.tests.JMXProperties;
import org.apache.geode.tools.pulse.tests.PulseTestLocators;

/**
 * If you try to run the Pulse UI Tests through your IDE without forking enabled, you will see
 * jmxrmi exceptions. This is due to the implementation of org.apache.geode.tools.pulse.tests.Server
 */
public abstract class PulseBase {
  public abstract WebDriver getWebDriver();

  public abstract String getPulseURL();

  @Before
  public void setup() {
    // Make sure we go to the home page first
    searchByXPathAndClick(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
  }

  protected void searchByLinkAndClick(String linkText) {
    WebElement element = By.linkText(linkText).findElement(getWebDriver());
    assertNotNull(element);
    element.click();
  }

  protected void searchByIdAndClick(String id) {
    WebElement element = getWebDriver().findElement(By.id(id));
    assertNotNull(element);
    element.click();
  }

  protected void searchByXPathAndClick(String xpath) {
    WebElement element = getWebDriver().findElement(By.xpath(xpath));
    assertNotNull(element);
    element.click();
  }

  @Test
  public void testClusterLocatorCount() {
    String clusterLocators = getWebDriver().findElement(By.id(CLUSTER_VIEW_LOCATORS_ID)).getText();

    String totallocators = JMXProperties.getInstance().getProperty("server.S1.locatorCount");
    assertEquals(totallocators, clusterLocators);
  }

  @Test
  public void testClusterRegionCount() {
    String clusterRegions = getWebDriver().findElement(By.id(CLUSTER_VIEW_REGIONS_ID)).getText();
    String totalregions = JMXProperties.getInstance().getProperty("server.S1.totalRegionCount");
    assertEquals(totalregions, clusterRegions);
  }

  @Test
  public void testClusterMemberCount() {
    String clusterMembers = getWebDriver().findElement(By.id(CLUSTER_VIEW_MEMBERS_ID)).getText();
    String totalMembers = JMXProperties.getInstance().getProperty("server.S1.memberCount");
    assertEquals(clusterMembers, totalMembers);
  }

  @Test
  public void testClusterNumClient() {
    String clusterClients = getWebDriver().findElement(By.id(CLUSTER_CLIENTS_ID)).getText();
    String totalclients = JMXProperties.getInstance().getProperty("server.S1.numClients");
    assertEquals(totalclients, clusterClients);
  }

  @Test
  public void testClusterNumRunningFunction() {
    String clusterFunctions = getWebDriver().findElement(By.id(CLUSTER_FUNCTIONS_ID)).getText();
    String totalfunctions =
        JMXProperties.getInstance().getProperty("server.S1.numRunningFunctions");
    assertEquals(totalfunctions, clusterFunctions);
  }

  @Test
  public void testClusterRegisteredCQCount() {
    String clusterUniqueCQs = getWebDriver().findElement(By.id(CLUSTER_UNIQUECQS_ID)).getText();
    String totaluniqueCQs = JMXProperties.getInstance().getProperty("server.S1.registeredCQCount");
    assertEquals(totaluniqueCQs, clusterUniqueCQs);
  }

  @Test
  public void testClusterNumSubscriptions() {
    String clusterSubscriptions =
        getWebDriver().findElement(By.id(CLUSTER_SUBSCRIPTION_ID)).getText();
    String totalSubscriptions =
        JMXProperties.getInstance().getProperty("server.S1.numSubscriptions");
    assertEquals(totalSubscriptions, clusterSubscriptions);
  }

  @Test
  public void testClusterJVMPausesWidget() {
    String clusterJVMPauses = getWebDriver().findElement(By.id(CLUSTER_GCPAUSES_ID)).getText();
    String totalgcpauses = JMXProperties.getInstance().getProperty("server.S1.jvmPauses");
    assertEquals(totalgcpauses, clusterJVMPauses);
  }

  @Test
  public void testClusterAverageWritesWidget() {
    String clusterWritePerSec = getWebDriver().findElement(By.id(CLUSTER_WRITEPERSEC_ID)).getText();
    String totalwritepersec = JMXProperties.getInstance().getProperty("server.S1.averageWrites");
    assertEquals(totalwritepersec, clusterWritePerSec);
  }

  @Test
  public void testClusterAverageReadsWidget() {
    String clusterReadPerSec = getWebDriver().findElement(By.id(CLUSTER_READPERSEC_ID)).getText();
    String totalreadpersec = JMXProperties.getInstance().getProperty("server.S1.averageReads");
    assertEquals(totalreadpersec, clusterReadPerSec);
  }

  @Test
  public void testClusterQuerRequestRateWidget() {
    String clusterQueriesPerSec =
        getWebDriver().findElement(By.id(CLUSTER_QUERIESPERSEC_ID)).getText();
    String totalqueriespersec =
        JMXProperties.getInstance().getProperty("server.S1.queryRequestRate");
    assertEquals(totalqueriespersec, clusterQueriesPerSec);
  }

  @Test
  public void testClusterGridViewMemberID() {
    searchByIdAndClick("default_grid_button");
    List<WebElement> elements =
        getWebDriver().findElements(By.xpath("//table[@id='memberList']/tbody/tr"));

    for (int memberCount = 1; memberCount < elements.size(); memberCount++) {
      String memberId = getWebDriver()
          .findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (memberCount + 1) + "]/td"))
          .getText();
      String propertMemeberId =
          JMXProperties.getInstance().getProperty("member.M" + memberCount + ".id");
      assertEquals(memberId, propertMemeberId);
    }
  }

  @Test
  public void testClusterGridViewMemberName() {
    searchByIdAndClick("default_grid_button");
    List<WebElement> elements =
        getWebDriver().findElements(By.xpath("//table[@id='memberList']/tbody/tr"));
    for (int memberNameCount = 1; memberNameCount < elements.size(); memberNameCount++) {
      String gridMemberName = getWebDriver()
          .findElement(
              By.xpath("//table[@id='memberList']/tbody/tr[" + (memberNameCount + 1) + "]/td[2]"))
          .getText();
      String memberName =
          JMXProperties.getInstance().getProperty("member.M" + memberNameCount + ".member");
      assertEquals(gridMemberName, memberName);
    }
  }


  @Test
  public void testClusterGridViewMemberHost() {
    searchByIdAndClick("default_grid_button");
    List<WebElement> elements =
        getWebDriver().findElements(By.xpath("//table[@id='memberList']/tbody/tr"));
    for (int memberHostCount = 1; memberHostCount < elements.size(); memberHostCount++) {
      String MemberHost = getWebDriver()
          .findElement(
              By.xpath("//table[@id='memberList']/tbody/tr[" + (memberHostCount + 1) + "]/td[3]"))
          .getText();
      String gridMemberHost =
          JMXProperties.getInstance().getProperty("member.M" + memberHostCount + ".host");
      assertEquals(gridMemberHost, MemberHost);
    }
  }

  @Test
  public void testClusterGridViewHeapUsage() {
    searchByIdAndClick("default_grid_button");
    for (int i = 1; i <= 3; i++) {
      Float HeapUsage = Float.parseFloat(getWebDriver()
          .findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (i + 1) + "]/td[5]"))
          .getText());
      Float gridHeapUsagestring =
          Float.parseFloat(JMXProperties.getInstance().getProperty("member.M" + i + ".UsedMemory"));
      assertEquals(gridHeapUsagestring, HeapUsage);
    }
  }

  @Test
  public void testClusterGridViewCPUUsage() {
    searchByIdAndClick("default_grid_button");
    for (int i = 1; i <= 3; i++) {
      String CPUUsage = getWebDriver()
          .findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (i + 1) + "]/td[6]"))
          .getText();
      String gridCPUUsage = JMXProperties.getInstance().getProperty("member.M" + i + ".cpuUsage");
      gridCPUUsage = gridCPUUsage.trim();
      assertEquals(gridCPUUsage, CPUUsage);
    }
  }


  public void testRgraphWidget() {
    searchByIdAndClick("default_rgraph_button");
    searchByIdAndClick("h1");
    searchByIdAndClick("M1");
  }

  @Test
  @Ignore("ElementNotVisible with phantomJS")
  public void testMemberTotalRegionCount() {
    testRgraphWidget();
    String RegionCount = getWebDriver().findElement(By.id(MEMBER_VIEW_REGION_ID)).getText();
    String memberRegionCount =
        JMXProperties.getInstance().getProperty("member.M1.totalRegionCount");
    assertEquals(memberRegionCount, RegionCount);
  }

  @Test
  public void testMemberNumThread() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    String ThreadCount = getWebDriver().findElement(By.id(MEMBER_VIEW_THREAD_ID)).getText();
    String memberThreadCount = JMXProperties.getInstance().getProperty("member.M1.numThreads");
    assertEquals(memberThreadCount, ThreadCount);
  }

  @Test
  public void testMemberTotalFileDescriptorOpen() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    String SocketCount = getWebDriver().findElement(By.id(MEMBER_VIEW_SOCKETS_ID)).getText();
    String memberSocketCount =
        JMXProperties.getInstance().getProperty("member.M1.totalFileDescriptorOpen");
    assertEquals(memberSocketCount, SocketCount);
  }

  @Test
  public void testMemberLoadAverage() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    String LoadAvg = getWebDriver().findElement(By.id(MEMBER_VIEW_LOADAVG_ID)).getText();
    String memberLoadAvg = JMXProperties.getInstance().getProperty("member.M1.loadAverage");
    assertEquals(TWO_PLACE_DECIMAL_FORMAT.format(Double.valueOf(memberLoadAvg)), LoadAvg);
  }

  @Ignore("WIP") // May be useful in near future
  @Test
  public void testOffHeapFreeSize() {

    String OffHeapFreeSizeString =
        getWebDriver().findElement(By.id(MEMBER_VIEW_OFFHEAPFREESIZE_ID)).getText();
    String OffHeapFreeSizetemp = OffHeapFreeSizeString.replaceAll("[a-zA-Z]", "");
    float OffHeapFreeSize = Float.parseFloat(OffHeapFreeSizetemp);
    float memberOffHeapFreeSize =
        Float.parseFloat(JMXProperties.getInstance().getProperty("member.M1.OffHeapFreeSize"));
    if (memberOffHeapFreeSize < 1048576) {
      memberOffHeapFreeSize = memberOffHeapFreeSize / 1024;

    } else if (memberOffHeapFreeSize < 1073741824) {
      memberOffHeapFreeSize = memberOffHeapFreeSize / 1024 / 1024;
    } else {
      memberOffHeapFreeSize = memberOffHeapFreeSize / 1024 / 1024 / 1024;
    }
    memberOffHeapFreeSize =
        Float.parseFloat(new DecimalFormat("##.##").format(memberOffHeapFreeSize));
    assertEquals(memberOffHeapFreeSize, OffHeapFreeSize, 0);

  }

  @Ignore("WIP") // May be useful in near future
  @Test
  public void testOffHeapUsedSize() {

    String OffHeapUsedSizeString =
        getWebDriver().findElement(By.id(MEMBER_VIEW_OFFHEAPUSEDSIZE_ID)).getText();
    String OffHeapUsedSizetemp = OffHeapUsedSizeString.replaceAll("[a-zA-Z]", "");
    float OffHeapUsedSize = Float.parseFloat(OffHeapUsedSizetemp);
    float memberOffHeapUsedSize =
        Float.parseFloat(JMXProperties.getInstance().getProperty("member.M1.OffHeapUsedSize"));
    if (memberOffHeapUsedSize < 1048576) {
      memberOffHeapUsedSize = memberOffHeapUsedSize / 1024;

    } else if (memberOffHeapUsedSize < 1073741824) {
      memberOffHeapUsedSize = memberOffHeapUsedSize / 1024 / 1024;
    } else {
      memberOffHeapUsedSize = memberOffHeapUsedSize / 1024 / 1024 / 1024;
    }
    memberOffHeapUsedSize =
        Float.parseFloat(new DecimalFormat("##.##").format(memberOffHeapUsedSize));
    assertEquals(memberOffHeapUsedSize, OffHeapUsedSize, 0);
  }

  @Test
  public void testMemberJVMPauses() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    String JVMPauses = getWebDriver().findElement(By.id(MEMBER_VIEW_JVMPAUSES_ID)).getText();
    String memberGcPausesAvg = JMXProperties.getInstance().getProperty("member.M1.JVMPauses");
    assertEquals(memberGcPausesAvg, JVMPauses);
  }

  @Test
  public void testMemberCPUUsage() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    String CPUUsagevalue = getWebDriver().findElement(By.id(MEMBER_VIEW_CPUUSAGE_ID)).getText();
    String memberCPUUsage = JMXProperties.getInstance().getProperty("member.M1.cpuUsage");
    assertEquals(memberCPUUsage, CPUUsagevalue);
  }

  @Test // difference between UI and properties file
  public void testMemberAverageReads() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    float ReadPerSec =
        Float.parseFloat(getWebDriver().findElement(By.id(MEMBER_VIEW_READPERSEC_ID)).getText());
    float memberReadPerSec =
        Float.parseFloat(JMXProperties.getInstance().getProperty("member.M1.averageReads"));
    memberReadPerSec = Float.parseFloat(new DecimalFormat("##.##").format(memberReadPerSec));
    assertEquals(memberReadPerSec, ReadPerSec, 0.001);
  }

  @Test
  @Ignore("ElementNotVisible with phantomJS")
  public void testMemberAverageWrites() {
    testRgraphWidget();
    String WritePerSec = getWebDriver().findElement(By.id(MEMBER_VIEW_WRITEPERSEC_ID)).getText();
    String memberWritePerSec = JMXProperties.getInstance().getProperty("member.M1.averageWrites");
    assertEquals(memberWritePerSec, WritePerSec);
  }


  @Test
  @Ignore("ElementNotVisible with phantomJS")
  public void testMemberGridViewData() {
    testRgraphWidget();
    searchByXPathAndClick(PulseTestLocators.MemberDetailsView.gridButtonXpath);
    // get the number of rows on the grid
    String MemberRegionName = getWebDriver()
        .findElement(By.xpath("//table[@id='memberRegionsList']/tbody/tr[2]/td[1]")).getText();
    String memberRegionName = JMXProperties.getInstance().getProperty("region.R1.name");
    assertEquals(memberRegionName, MemberRegionName);

    String MemberRegionType = getWebDriver()
        .findElement(By.xpath("//table[@id='memberRegionsList']/tbody/tr[2]/td[2]")).getText();
    String memberRegionType = JMXProperties.getInstance().getProperty("region.R1.regionType");
    assertEquals(memberRegionType, MemberRegionType);

    String MemberRegionEntryCount = getWebDriver()
        .findElement(By.xpath("//table[@id='memberRegionsList']/tbody/tr[2]/td[3]")).getText();
    String memberRegionEntryCount =
        JMXProperties.getInstance().getProperty("regionOnMember./R1.M1.entryCount");
    assertEquals(memberRegionEntryCount, MemberRegionEntryCount);
  }

  @Test
  public void testDropDownList() {
    searchByIdAndClick("default_grid_button");
    searchByIdAndClick("M1&M1");
    searchByIdAndClick("memberName");
    searchByLinkAndClick("M3");
    searchByIdAndClick("memberName");
    searchByLinkAndClick("M2");
  }

  @Ignore("WIP")
  @Test
  public void testDataViewRegionName() throws InterruptedException {
    searchByLinkAndClick(DATA_VIEW_LABEL);
    Thread.sleep(7000);
    searchByIdAndClick("default_grid_button");
    String regionName = getWebDriver().findElement(By.id(REGION_NAME_LABEL)).getText();
    String dataviewregionname = JMXProperties.getInstance().getProperty("region.R1.name");
    assertEquals(dataviewregionname, regionName);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewRegionPath() {
    String regionPath = getWebDriver().findElement(By.id(REGION_PATH_LABEL)).getText();
    String dataviewregionpath = JMXProperties.getInstance().getProperty("region.R1.fullPath");
    assertEquals(dataviewregionpath, regionPath);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewRegionType() {
    String regionType = getWebDriver().findElement(By.id(REGION_TYPE_LABEL)).getText();
    String dataviewregiontype = JMXProperties.getInstance().getProperty("region.R1.regionType");
    assertEquals(dataviewregiontype, regionType);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewEmptyNodes() {
    String regionEmptyNodes = getWebDriver().findElement(By.id(DATA_VIEW_EMPTYNODES)).getText();
    String dataviewEmptyNodes = JMXProperties.getInstance().getProperty("region.R1.emptyNodes");
    assertEquals(dataviewEmptyNodes, regionEmptyNodes);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewSystemRegionEntryCount() {
    String regionEntryCount = getWebDriver().findElement(By.id(DATA_VIEW_ENTRYCOUNT)).getText();
    String dataviewEntryCount =
        JMXProperties.getInstance().getProperty("region.R1.systemRegionEntryCount");
    assertEquals(dataviewEntryCount, regionEntryCount);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewPersistentEnabled() {
    String regionPersistence =
        getWebDriver().findElement(By.id(REGION_PERSISTENCE_LABEL)).getText();
    String dataviewregionpersistence =
        JMXProperties.getInstance().getProperty("region.R1.persistentEnabled");
    assertEquals(dataviewregionpersistence, regionPersistence);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewDiskWritesRate() {
    String regionWrites = getWebDriver().findElement(By.id(DATA_VIEW_WRITEPERSEC)).getText();
    String dataviewRegionWrites =
        JMXProperties.getInstance().getProperty("region.R1.diskWritesRate");
    assertEquals(dataviewRegionWrites, regionWrites);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewDiskReadsRate() {
    String regionReads = getWebDriver().findElement(By.id(DATA_VIEW_READPERSEC)).getText();
    String dataviewRegionReads = JMXProperties.getInstance().getProperty("region.R1.diskReadsRate");
    assertEquals(dataviewRegionReads, regionReads);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewDiskUsage() {
    String regionMemoryUsed = getWebDriver().findElement(By.id(DATA_VIEW_USEDMEMORY)).getText();
    String dataviewMemoryUsed = JMXProperties.getInstance().getProperty("region.R1.diskUsage");
    assertEquals(dataviewMemoryUsed, regionMemoryUsed);
    searchByLinkAndClick(QUERY_STATISTICS_LABEL);
  }

  @Ignore("WIP")
  @Test
  public void testDataViewGridValue() {
    String DataViewRegionName =
        getWebDriver().findElement(By.xpath("//*[id('6')/x:td[1]]")).getText();
    String dataViewRegionName = JMXProperties.getInstance().getProperty("region.R1.name");
    assertEquals(dataViewRegionName, DataViewRegionName);

    String DataViewRegionType =
        getWebDriver().findElement(By.xpath("//*[id('6')/x:td[2]")).getText();
    String dataViewRegionType = JMXProperties.getInstance().getProperty("region.R2.regionType");
    assertEquals(dataViewRegionType, DataViewRegionType);

    String DataViewEntryCount =
        getWebDriver().findElement(By.xpath("//*[id('6')/x:td[3]")).getText();
    String dataViewEntryCount =
        JMXProperties.getInstance().getProperty("region.R2.systemRegionEntryCount");
    assertEquals(dataViewEntryCount, DataViewEntryCount);

    String DataViewEntrySize =
        getWebDriver().findElement(By.xpath("//*[id('6')/x:td[4]")).getText();
    String dataViewEntrySize = JMXProperties.getInstance().getProperty("region.R2.entrySize");
    assertEquals(dataViewEntrySize, DataViewEntrySize);

  }


  public void loadDataBrowserpage() {
    searchByLinkAndClick(DATA_BROWSER_LABEL);
    // Thread.sleep(7000);
  }

  @Test
  public void testDataBrowserRegionName() {
    loadDataBrowserpage();
    String DataBrowserRegionName1 =
        getWebDriver().findElement(By.id(DATA_BROWSER_REGIONName1)).getText();
    String databrowserRegionNametemp1 = JMXProperties.getInstance().getProperty("region.R1.name");
    String databrowserRegionName1 = databrowserRegionNametemp1.replaceAll("[/]", "");
    assertEquals(databrowserRegionName1, DataBrowserRegionName1);

    String DataBrowserRegionName2 =
        getWebDriver().findElement(By.id(DATA_BROWSER_REGIONName2)).getText();
    String databrowserRegionNametemp2 = JMXProperties.getInstance().getProperty("region.R2.name");
    String databrowserRegionName2 = databrowserRegionNametemp2.replaceAll("[/]", "");
    assertEquals(databrowserRegionName2, DataBrowserRegionName2);

    String DataBrowserRegionName3 =
        getWebDriver().findElement(By.id(DATA_BROWSER_REGIONName3)).getText();
    String databrowserRegionNametemp3 = JMXProperties.getInstance().getProperty("region.R3.name");
    String databrowserRegionName3 = databrowserRegionNametemp3.replaceAll("[/]", "");
    assertEquals(databrowserRegionName3, DataBrowserRegionName3);

  }

  @Test
  public void testDataBrowserRegionMembersVerificaition() {
    loadDataBrowserpage();
    searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);
    String DataBrowserMember1Name1 =
        getWebDriver().findElement(By.xpath("//label[@for='Member0']")).getText();
    String DataBrowserMember1Name2 =
        getWebDriver().findElement(By.xpath("//label[@for='Member1']")).getText();
    String DataBrowserMember1Name3 =
        getWebDriver().findElement(By.xpath("//label[@for='Member2']")).getText();
    String databrowserMember1Names = JMXProperties.getInstance().getProperty("region.R1.members");

    String databrowserMember1Names1 = databrowserMember1Names.substring(0, 2);
    assertEquals(databrowserMember1Names1, DataBrowserMember1Name1);

    String databrowserMember1Names2 = databrowserMember1Names.substring(3, 5);
    assertEquals(databrowserMember1Names2, DataBrowserMember1Name2);

    String databrowserMember1Names3 = databrowserMember1Names.substring(6, 8);
    assertEquals(databrowserMember1Names3, DataBrowserMember1Name3);
    searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);

    searchByIdAndClick(DATA_BROWSER_REGION2_CHECKBOX);
    String DataBrowserMember2Name1 =
        getWebDriver().findElement(By.xpath("//label[@for='Member0']")).getText();
    String DataBrowserMember2Name2 =
        getWebDriver().findElement(By.xpath("//label[@for='Member1']")).getText();
    String databrowserMember2Names = JMXProperties.getInstance().getProperty("region.R2.members");

    String databrowserMember2Names1 = databrowserMember2Names.substring(0, 2);
    assertEquals(databrowserMember2Names1, DataBrowserMember2Name1);

    String databrowserMember2Names2 = databrowserMember2Names.substring(3, 5);
    assertEquals(databrowserMember2Names2, DataBrowserMember2Name2);
    searchByIdAndClick(DATA_BROWSER_REGION2_CHECKBOX);

    searchByIdAndClick(DATA_BROWSER_REGION3_CHECKBOX);
    String DataBrowserMember3Name1 =
        getWebDriver().findElement(By.xpath("//label[@for='Member0']")).getText();
    String DataBrowserMember3Name2 =
        getWebDriver().findElement(By.xpath("//label[@for='Member1']")).getText();
    String databrowserMember3Names = JMXProperties.getInstance().getProperty("region.R3.members");

    String databrowserMember3Names1 = databrowserMember3Names.substring(0, 2);
    assertEquals(databrowserMember3Names1, DataBrowserMember3Name1);

    String databrowserMember3Names2 = databrowserMember3Names.substring(3, 5);
    assertEquals(databrowserMember3Names2, DataBrowserMember3Name2);
    searchByIdAndClick(DATA_BROWSER_REGION3_CHECKBOX);
  }

  @Test
  public void testDataBrowserColocatedRegions() {
    loadDataBrowserpage();
    String databrowserMemberNames1 = JMXProperties.getInstance().getProperty("region.R1.members");
    String databrowserMemberNames2 = JMXProperties.getInstance().getProperty("region.R2.members");
    String databrowserMemberNames3 = JMXProperties.getInstance().getProperty("region.R3.members");

    if ((databrowserMemberNames1.matches(databrowserMemberNames2 + "(.*)"))) {
      if ((databrowserMemberNames1.matches(databrowserMemberNames3 + "(.*)"))) {
        if ((databrowserMemberNames2.matches(databrowserMemberNames3 + "(.*)"))) {
          System.out.println("R1, R2 and R3 are colocated regions");
        }
      }
    }
    searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);
    searchByLinkAndClick(DATA_BROWSER_COLOCATED_REGION);
    String DataBrowserColocatedRegion1 =
        getWebDriver().findElement(By.id(DATA_BROWSER_COLOCATED_REGION_NAME1)).getText();
    String DataBrowserColocatedRegion2 =
        getWebDriver().findElement(By.id(DATA_BROWSER_COLOCATED_REGION_NAME2)).getText();
    String DataBrowserColocatedRegion3 =
        getWebDriver().findElement(By.id(DATA_BROWSER_COLOCATED_REGION_NAME3)).getText();

    String databrowserColocatedRegiontemp1 =
        JMXProperties.getInstance().getProperty("region.R1.name");
    String databrowserColocatedRegion1 = databrowserColocatedRegiontemp1.replaceAll("[/]", "");

    String databrowserColocatedRegiontemp2 =
        JMXProperties.getInstance().getProperty("region.R2.name");
    String databrowserColocatedRegion2 = databrowserColocatedRegiontemp2.replaceAll("[/]", "");

    String databrowserColocatedRegiontemp3 =
        JMXProperties.getInstance().getProperty("region.R3.name");
    String databrowserColocatedRegion3 = databrowserColocatedRegiontemp3.replaceAll("[/]", "");

    assertEquals(databrowserColocatedRegion1, DataBrowserColocatedRegion1);
    assertEquals(databrowserColocatedRegion2, DataBrowserColocatedRegion2);
    assertEquals(databrowserColocatedRegion3, DataBrowserColocatedRegion3);

  }

  public void testTreeMapPopUpData(String S1, String gridIcon) {
    for (int i = 1; i <= 3; i++) {
      searchByLinkAndClick(CLUSTER_VIEW_LABEL);
      if (gridIcon.equals(SERVER_GROUP_GRID_ID)) {
        WebElement ServerGroupRadio =
            getWebDriver().findElement(By.xpath("//label[@for='radio-servergroups']"));
        ServerGroupRadio.click();
      }
      if (gridIcon.equals(REDUNDANCY_GRID_ID)) {
        WebElement ServerGroupRadio =
            getWebDriver().findElement(By.xpath("//label[@for='radio-redundancyzones']"));
        ServerGroupRadio.click();
      }
      searchByIdAndClick(gridIcon);
      WebElement TreeMapMember =
          getWebDriver().findElement(By.xpath("//div[@id='" + S1 + "M" + (i) + "']/div"));
      Actions builder = new Actions(getWebDriver());
      builder.clickAndHold(TreeMapMember).perform();
      int j = 1;
      String CPUUsageM1temp = getWebDriver()
          .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div/div[2]/div")).getText();
      String CPUUsageM1 = CPUUsageM1temp.replaceAll("[%]", "");
      String cpuUsageM1 = JMXProperties.getInstance().getProperty("member.M" + (i) + ".cpuUsage");
      assertEquals(cpuUsageM1, CPUUsageM1);

      String MemoryUsageM1temp = getWebDriver()
          .findElement(
              By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 1) + "]/div[2]/div"))
          .getText();
      String MemoryUsageM1 = MemoryUsageM1temp.replaceAll("MB", "");
      String memoryUsageM1 =
          JMXProperties.getInstance().getProperty("member.M" + (i) + ".UsedMemory");
      assertEquals(memoryUsageM1, MemoryUsageM1);

      String LoadAvgM1 = getWebDriver()
          .findElement(
              By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 2) + "]/div[2]/div"))
          .getText();
      String loadAvgM1 = JMXProperties.getInstance().getProperty("member.M" + (i) + ".loadAverage");
      assertEquals(TWO_PLACE_DECIMAL_FORMAT.format(Double.valueOf(loadAvgM1)), LoadAvgM1);

      String ThreadsM1 = getWebDriver()
          .findElement(
              By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 3) + "]/div[2]/div"))
          .getText();
      String threadsM1 = JMXProperties.getInstance().getProperty("member.M" + (i) + ".numThreads");
      assertEquals(threadsM1, ThreadsM1);

      String SocketsM1 = getWebDriver()
          .findElement(
              By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 4) + "]/div[2]/div"))
          .getText();
      String socketsM1 =
          JMXProperties.getInstance().getProperty("member.M" + (i) + ".totalFileDescriptorOpen");
      assertEquals(socketsM1, SocketsM1);
      builder.moveToElement(TreeMapMember).release().perform();
    }
  }

  @Test
  public void testTopologyPopUpData() {
    testTreeMapPopUpData("", CLUSTER_VIEW_GRID_ID);
  }

  @Test
  public void testServerGroupTreeMapPopUpData() {
    testTreeMapPopUpData("SG1(!)", SERVER_GROUP_GRID_ID);
  }

  @Test
  public void testDataViewTreeMapPopUpData() {
    searchByLinkAndClick(CLUSTER_VIEW_LABEL);
    searchByLinkAndClick(DATA_DROPDOWN_ID);
    WebElement TreeMapMember = getWebDriver().findElement(By.id("GraphTreeMapClusterData-canvas"));
    Actions builder = new Actions(getWebDriver());
    builder.clickAndHold(TreeMapMember).perform();
    String RegionType = getWebDriver()
        .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div/div[2]/div")).getText();
    String regionType = JMXProperties.getInstance().getProperty("region.R2.regionType");
    assertEquals(regionType, RegionType);

    String EntryCount = getWebDriver()
        .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[2]/div[2]/div")).getText();
    String entryCount = JMXProperties.getInstance().getProperty("region.R2.systemRegionEntryCount");
    assertEquals(entryCount, EntryCount);

    String EntrySizetemp = getWebDriver()
        .findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[3]/div[2]/div")).getText();
    float EntrySize = Float.parseFloat(EntrySizetemp);
    float entrySize =
        Float.parseFloat(JMXProperties.getInstance().getProperty("region.R2.entrySize"));
    entrySize = entrySize / 1024 / 1024;
    entrySize = Float.parseFloat(new DecimalFormat("##.####").format(entrySize));
    assertEquals(entrySize, EntrySize, 0.001);
    builder.moveToElement(TreeMapMember).release().perform();
  }

  @Test
  public void testRegionViewTreeMapPopUpData() {
    searchByLinkAndClick(CLUSTER_VIEW_LABEL);
    searchByLinkAndClick(DATA_DROPDOWN_ID);
    WebElement TreeMapMember = getWebDriver().findElement(By.id("GraphTreeMapClusterData-canvas"));
    TreeMapMember.click();
  }

  @Test
  public void userCanGetToPulseDetails() {
    getWebDriver().get(getPulseURL() + "pulseVersion");

    assertTrue(getWebDriver().getPageSource().contains("sourceRevision"));
  }

}
