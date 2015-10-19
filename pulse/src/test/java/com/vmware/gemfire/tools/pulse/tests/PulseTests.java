/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

@FixMethodOrder(MethodSorters.JVM)
public class PulseTests {
 // private final static String jmxPropertiesFile = "E:\\springsource\\springsourceWS\\Pulse-Cedar\\src\\test\\resources\\test.properties";
  //private static String path = "D:\\springsource\\springsourceWS\\Pulse-Cedar\\build-artifacts\\win\\dist\\pulse-7.5.war";
  private final static String jmxPropertiesFile = System
      .getProperty("pulse.propfile");
  private static String path = System.getProperty("pulse.war");

  private static Tomcat tomcat = null;
  private static Server server = null;
  private static String pulseURL = null;
  public static WebDriver driver;
  private static final String userName = "admin";
  private static final String pasword = "admin";

  /* Constants for executing Data Browser queries */
  public static final String QUERY_TYPE_ONE = "query1";
  public static final String QUERY_TYPE_TWO = "query2";
  public static final String QUERY_TYPE_THREE = "query3";
  public static final String QUERY_TYPE_FOUR = "query4";
  public static final String QUERY_TYPE_FIVE = "query5";
  public static final String QUERY_TYPE_SIX = "query6";
  public static final String QUERY_TYPE_SEVENE = "query7";

  private static final String DATA_VIEW_LABEL = "Data View";
  private static final String CLUSTER_VIEW_MEMBERS_ID = "clusterTotalMembersText";
  private static final String CLUSTER_VIEW_SERVERS_ID = "clusterServersText";
  private static final String CLUSTER_VIEW_LOCATORS_ID = "clusterLocatorsText";
  private static final String CLUSTER_VIEW_REGIONS_ID = "clusterTotalRegionsText";
  private static final String CLUSTER_CLIENTS_ID = "clusterClientsText";
  private static final String CLUSTER_FUNCTIONS_ID = "clusterFunctions";
  private static final String CLUSTER_UNIQUECQS_ID = "clusterUniqueCQs";
  private static final String CLUSTER_SUBSCRIPTION_ID = "clusterSubscriptionsText";
  private static final String CLUSTER_MEMORY_USAGE_ID = "currentMemoryUsage";
  private static final String CLUSTER_THROUGHPUT_WRITES_ID = "currentThroughoutWrites";
  private static final String CLUSTER_GCPAUSES_ID = "currentGCPauses";
  private static final String CLUSTER_WRITEPERSEC_ID = "writePerSec";
  private static final String CLUSTER_READPERSEC_ID = "readPerSec";
  private static final String CLUSTER_QUERIESPERSEC_ID = "queriesPerSec";
  private static final String CLUSTER_PROCEDURE_ID = "clusterTxnCommittedText";
  private static final String CLUSTER_TXNCOMMITTED_ID = "clusterTxnCommittedText";
  private static final String CLUSTER_TXNROLLBACK_ID = "clusterTxnRollbackText";
  private static final String MEMBER_VIEW_MEMBERNAME_ID = "memberName";
  private static final String MEMBER_VIEW_REGION_ID = "memberRegionsCount";
  private static final String MEMBER_VIEW_THREAD_ID = "threads";
  private static final String MEMBER_VIEW_SOCKETS_ID = "sockets";
  private static final String MEMBER_VIEW_LOADAVG_ID = "loadAverage";
  private static final String MEMBER_VIEW_LISTENINGPORT_ID = "receiverListeningPort";
  private static final String MEMBER_VIEW_LINKTHROUGHPUT_ID = "receiverLinkThroughput";
  private static final String MEMBER_VIEW_AVGBATCHLATENCY_ID = "receiverAvgBatchLatency";
  private static final String MEMBER_VIEW_HEAPUSAGE_ID = "memberHeapUsageAvg";
  private static final String MEMBER_VIEW_JVMPAUSES_ID = "memberGcPausesAvg";
  private static final String MEMBER_VIEW_CPUUSAGE_ID = "memberCPUUsageValue";
  private static final String MEMBER_VIEW_READPERSEC_ID = "memberGetsPerSecValue";
  private static final String MEMBER_VIEW_WRITEPERSEC_ID = "memberPutsPerSecValue";
  private static final String MEMBER_VIEW_OFFHEAPFREESIZE_ID = "offHeapFreeSize";
  private static final String MEMBER_VIEW_OFFHEAPUSEDSIZE_ID = "offHeapUsedSize";
  private static final String MEMBER_VIEW_CLIENTS_ID = "clusterClientsText";

  private static final String REGION_NAME_LABEL = "regionName";
  private static final String REGION_PATH_LABEL = "regionPath";
  private static final String REGION_TYPE_LABEL = "regionType";
  private static final String DATA_VIEW_WRITEPERSEC = "regionWrites";
  private static final String DATA_VIEW_READPERSEC = "regionReads";
  private static final String DATA_VIEW_EMPTYNODES = "regionEmptyNodes";
  private static final String DATA_VIEW_ENTRYCOUNT = "regionEntryCount";
  private static final String REGION_PERSISTENCE_LABEL = "regionPersistence";
  private static final String DATA_VIEW_USEDMEMORY = "memoryUsed";
  private static final String DATA_VIEW_TOTALMEMORY = "totalMemory";
  
  private static final String DATA_BROWSER_LABEL = "Data Browser";
  private static final String DATA_BROWSER_REGIONName1 = "treeDemo_1_span";
  private static final String DATA_BROWSER_REGIONName2 = "treeDemo_2_span";
  private static final String DATA_BROWSER_REGIONName3 = "treeDemo_3_span";
  private static final String DATA_BROWSER_REGION1_CHECKBOX = "treeDemo_1_check";
  private static final String DATA_BROWSER_REGION2_CHECKBOX = "treeDemo_2_check";
  private static final String DATA_BROWSER_REGION3_CHECKBOX = "treeDemo_3_check";
  private static final String DATA_BROWSER_COLOCATED_REGION = "Colocated Regions";
  private static final String DATA_BROWSER_COLOCATED_REGION_NAME1 = "treeDemo_1_span";
  private static final String DATA_BROWSER_COLOCATED_REGION_NAME2 = "treeDemo_2_span";
  private static final String DATA_BROWSER_COLOCATED_REGION_NAME3 = "treeDemo_3_span";

  private static final String QUERY_STATISTICS_LABEL = "Query Statistics";
  private static final String CLUSTER_VIEW_LABEL = "Cluster View";
  private static final String CLUSTER_VIEW_GRID_ID = "default_treemap_button";
  private static final String SERVER_GROUP_GRID_ID = "servergroups_treemap_button";
  private static final String REDUNDANCY_GRID_ID = "redundancyzones_treemap_button";
  private static final String MEMBER_DROPDOWN_ID = "Members";
  private static final String DATA_DROPDOWN_ID = "Data";

  private static WebDriver initdriver = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      server = Server.createServer(9999, jmxPropertiesFile);

      String host = "localhost";// InetAddress.getLocalHost().getHostAddress();
      int port = 8080;
      String context = "/pulse";
      
      tomcat = TomcatHelper.startTomcat(host, port, context, path);
      pulseURL = "http://" + host + ":" + port + context;

      Thread.sleep(5000); // wait till tomcat settles down
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      Assert.fail("Error " + e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Error " + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Error " + e.getMessage());
    }

    driver = new FirefoxDriver();
    driver.manage().window().maximize();
    driver.manage().timeouts().implicitlyWait(15, TimeUnit.SECONDS);
    driver.get(pulseURL);
    WebElement userNameElement = driver.findElement(By.id("user_name"));
    WebElement passwordElement = driver.findElement(By.id("user_password"));
    userNameElement.sendKeys(userName);
    passwordElement.sendKeys(pasword);
    passwordElement.submit();

    Thread.sleep(3000);
    WebElement userNameOnPulsePage = (new WebDriverWait(driver, 10))
        .until(new ExpectedCondition<WebElement>() {
          @Override
          public WebElement apply(WebDriver d) {
            return d.findElement(By.id("userName"));
          }
        });
    Assert.assertNotNull(userNameOnPulsePage);
    driver.navigate().refresh();
    Thread.sleep(7000);
  }

  protected void searchByLinkAndClick(String linkText) {
    WebElement element = By.linkText(linkText).findElement(driver);
    Assert.assertNotNull(element);
    element.click();
  }

  protected void searchByIdAndClick(String id) {
    WebElement element = driver.findElement(By.id(id));
    Assert.assertNotNull(element);
    element.click();
  }

  protected void searchByClassAndClick(String Class) {
    WebElement element = driver.findElement(By.className(Class));
    Assert.assertNotNull(element);
    element.click();
  }

  protected void searchByXPathAndClick(String xpath) {
	WebElement element = driver.findElement(By.xpath(xpath));
     Assert.assertNotNull(element);
    element.click();
  }

  protected void waitForElementByClassName(final String className, int seconds) {
    WebElement linkTextOnPulsePage1 = (new WebDriverWait(driver, seconds))
        .until(new ExpectedCondition<WebElement>() {
          @Override
          public WebElement apply(WebDriver d) {
            return d.findElement(By.className(className));
          }
        });
    Assert.assertNotNull(linkTextOnPulsePage1);
  }

  protected void waitForElementById(final String id, int seconds) {
    WebElement element = (new WebDriverWait(driver, 10))
        .until(new ExpectedCondition<WebElement>() {
          @Override
          public WebElement apply(WebDriver d) {
            return d.findElement(By.id(id));
          }
        });
    Assert.assertNotNull(element);
  }
  
  protected void scrollbarVerticalDownScroll() {
    JavascriptExecutor js = (JavascriptExecutor) driver;
    js.executeScript("javascript:window.scrollBy(250,700)");
    WebElement pickerScroll = driver.findElement(By.className("jspDrag"));
    WebElement pickerScrollCorner = driver.findElement(By
        .className("jspCorner"));
    Actions builder = new Actions(driver);
    Actions movePicker = builder.dragAndDrop(pickerScroll, pickerScrollCorner); // pickerscroll
                                                                                // is
                                                                                // the
                                                                                // webelement
    movePicker.perform();
  }

  protected void scrollbarHorizontalRightScroll() {
    JavascriptExecutor js = (JavascriptExecutor) driver;
    js.executeScript("javascript:window.scrollBy(250,700)");
    WebElement pickerScroll = driver
        .findElement(By
            .xpath("//div[@id='gview_queryStatisticsList']/div[3]/div/div[3]/div[2]/div"));
    WebElement pickerScrollCorner = driver.findElement(By
        .className("jspCorner"));
    Actions builder = new Actions(driver);
    Actions movePicker = builder.dragAndDrop(pickerScroll, pickerScrollCorner); // pickerscroll
                                                                                // is
                                                                                // the
                                                                                // webelement
    movePicker.perform();
  }

  
  
  @Test
  public void testClusterLocatorCount() throws IOException {
	searchByXPathAndClick(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
    String clusterLocators = driver
        .findElement(By.id(CLUSTER_VIEW_LOCATORS_ID)).getText();
   
    String totallocators = JMXProperties.getInstance().getProperty("server.S1.locatorCount");  
    Assert.assertEquals(totallocators, clusterLocators);
  }

 @Test
  public void testClusterRegionCount() {
    String clusterRegions = driver.findElement(By.id(CLUSTER_VIEW_REGIONS_ID))
        .getText();
    String totalregions = JMXProperties.getInstance().getProperty(
        "server.S1.totalRegionCount");
    Assert.assertEquals(totalregions, clusterRegions);
  }

 @Test
  public void testClusterMemberCount() {
    String clusterMembers = driver.findElement(By.id(CLUSTER_VIEW_MEMBERS_ID))
        .getText();
    String totalMembers = JMXProperties.getInstance().getProperty(
        "server.S1.memberCount");
    Assert.assertEquals(totalMembers, clusterMembers);
  }

 @Test
  public void testClusterNumClient() {
    String clusterClients = driver.findElement(By.id(CLUSTER_CLIENTS_ID))
        .getText();
    String totalclients = JMXProperties.getInstance().getProperty(
        "server.S1.numClients");
    Assert.assertEquals(totalclients, clusterClients);
  }

  @Ignore("For Gemfire XD")
  @Test
  public void testClusterNumProcedures() {
    String clusterProcedures = driver.findElement(By.id(CLUSTER_PROCEDURE_ID)).getText();
    String totalprocedures = JMXProperties.getInstance().getProperty(
        "gemfirexd.C1.ProcedureCallsInProgress");
    Assert.assertEquals(totalprocedures, clusterProcedures);
  }

  @Ignore("For Gemfire XD")
  @Test
  public void testClusterTxnCommitted() {
    String clusterTxnCommitted = driver.findElement(
        By.id(CLUSTER_TXNCOMMITTED_ID)).getText();
    String totaltxnCommitted = JMXProperties.getInstance().getProperty(
        "server.S1.TransactionCommitted");
    Assert.assertEquals(totaltxnCommitted, clusterTxnCommitted);
  }

  @Ignore("For Gemfire XD")
  @Test
  public void testClusterTxnRollback() {
    String clusterTxnRollBack = driver.findElement(
        By.id(CLUSTER_TXNROLLBACK_ID)).getText();
    String totaltxnRollback = JMXProperties.getInstance().getProperty(
        "server.S1.TransactionRolledBack");
    Assert.assertEquals(totaltxnRollback, clusterTxnRollBack);
  }

  @Test
  public void testClusterNumRunningFunction() {
    String clusterFunctions = driver.findElement(By.id(CLUSTER_FUNCTIONS_ID))
        .getText();
    String totalfunctions = JMXProperties.getInstance().getProperty(
        "server.S1.numRunningFunctions");
    Assert.assertEquals(totalfunctions, clusterFunctions);
  }

@Test
  public void testClusterRegisteredCQCount() {
    String clusterUniqueCQs = driver.findElement(By.id(CLUSTER_UNIQUECQS_ID))
        .getText();
    String totaluniqueCQs = JMXProperties.getInstance().getProperty(
        "server.S1.registeredCQCount");
    Assert.assertEquals(totaluniqueCQs, clusterUniqueCQs);
  }

 @Test
  public void testClusterNumSubscriptions() {
    String clusterSubscriptions = driver.findElement(
        By.id(CLUSTER_SUBSCRIPTION_ID)).getText();
    String totalSubscriptions = JMXProperties.getInstance().getProperty(
        "server.S1.numSubscriptions");
    Assert.assertEquals(totalSubscriptions, clusterSubscriptions);
  }

 @Test
  public void testClusterJVMPausesWidget() {
    String clusterJVMPauses = driver.findElement(By.id(CLUSTER_GCPAUSES_ID))
        .getText();
    String totalgcpauses = JMXProperties.getInstance().getProperty(
        "server.S1.jvmPauses");
    Assert.assertEquals(totalgcpauses, clusterJVMPauses);
  }

  @Test
  public void testClusterAverageWritesWidget() {
    String clusterWritePerSec = driver.findElement(
        By.id(CLUSTER_WRITEPERSEC_ID)).getText();
    String totalwritepersec = JMXProperties.getInstance().getProperty(
        "server.S1.averageWrites");
    Assert.assertEquals(totalwritepersec, clusterWritePerSec);
  }

  @Test
  public void testClusterAverageReadsWidget() {
    String clusterReadPerSec = driver.findElement(By.id(CLUSTER_READPERSEC_ID))
        .getText();
    String totalreadpersec = JMXProperties.getInstance().getProperty(
        "server.S1.averageReads");
    Assert.assertEquals(totalreadpersec, clusterReadPerSec);
  }

  @Test
  public void testClusterQuerRequestRateWidget() {
    String clusterQueriesPerSec = driver.findElement(
        By.id(CLUSTER_QUERIESPERSEC_ID)).getText();
    String totalqueriespersec = JMXProperties.getInstance().getProperty(
        "server.S1.queryRequestRate");
    Assert.assertEquals(totalqueriespersec, clusterQueriesPerSec);
  }
  
  @Test
  public void testClusterGridViewMemberID() throws InterruptedException {
	  
	 searchByIdAndClick("default_grid_button");	
	 List<WebElement> elements = driver.findElements(By.xpath("//table[@id='memberList']/tbody/tr")); //gives me 11 rows
	 
	 for(int memberCount = 1; memberCount<elements.size(); memberCount++){		  
		  String memberId = driver.findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (memberCount + 1) + "]/td")).getText();		  
		  String propertMemeberId= JMXProperties.getInstance().getProperty("member.M" + memberCount + ".id");		  
		  Assert.assertEquals(memberId, propertMemeberId);
	  }	 
  }

  @Test
  public void testClusterGridViewMemberName() {
	  searchByIdAndClick("default_grid_button"); 
	  List<WebElement> elements = driver.findElements(By.xpath("//table[@id='memberList']/tbody/tr"));  	  
	  for (int memberNameCount = 1; memberNameCount < elements.size(); memberNameCount++) {
		  String gridMemberName = driver.findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (memberNameCount + 1) + "]/td[2]")).getText();
		  String memberName = JMXProperties.getInstance().getProperty("member.M" + memberNameCount + ".member");
		  Assert.assertEquals(gridMemberName, memberName);
    }
  }
  

  @Test
  public void testClusterGridViewMemberHost() {
	  searchByIdAndClick("default_grid_button"); 
	  List<WebElement> elements = driver.findElements(By.xpath("//table[@id='memberList']/tbody/tr")); 	  
    for (int memberHostCount = 1; memberHostCount < elements.size(); memberHostCount++) {
      String MemberHost = driver.findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (memberHostCount + 1) + "]/td[3]")).getText();     
      String gridMemberHost = JMXProperties.getInstance().getProperty("member.M" + memberHostCount + ".host");
      Assert.assertEquals(gridMemberHost, MemberHost);
    }
  }

  @Test
  public void testClusterGridViewHeapUsage() {
	searchByIdAndClick("default_grid_button"); 
    for (int i = 1; i <= 3; i++) {
      Float HeapUsage = Float.parseFloat(driver
          .findElement(
              By.xpath("//table[@id='memberList']/tbody/tr[" + (i + 1) + "]/td[4]")).getText());
      Float gridHeapUsagestring = Float.parseFloat(JMXProperties.getInstance()
          .getProperty("member.M" + i + ".UsedMemory"));
     Assert.assertEquals(gridHeapUsagestring, HeapUsage);
    }
  }
   
  @Test
  public void testClusterGridViewCPUUsage() {
	searchByIdAndClick("default_grid_button"); 
    for (int i = 1; i <= 3; i++) {
      String CPUUsage = driver.findElement(By.xpath("//table[@id='memberList']/tbody/tr[" + (i + 1) + "]/td[5]")).getText();
      String gridCPUUsage = JMXProperties.getInstance().getProperty("member.M" + i + ".cpuUsage");
      gridCPUUsage = gridCPUUsage.trim();
      Assert.assertEquals(gridCPUUsage, CPUUsage);
    }
  }


  public void testRgraphWidget() throws InterruptedException {
    searchByIdAndClick("default_rgraph_button");
    Thread.sleep(7000);
    searchByIdAndClick("h1");
    Thread.sleep(500);
    searchByIdAndClick("M1");
    Thread.sleep(7000);
  }

  @Test  // region count in properties file is 2 and UI is 1
  public void testMemberTotalRegionCount() throws InterruptedException{
	testRgraphWidget();
    String RegionCount = driver.findElement(By.id(MEMBER_VIEW_REGION_ID)).getText();  
    String memberRegionCount = JMXProperties.getInstance().getProperty("member.M1.totalRegionCount");   
    Assert.assertEquals(memberRegionCount, RegionCount);
  }

  @Test
  public void testMemberNumThread()throws InterruptedException {
    String ThreadCount = driver.findElement(By.id(MEMBER_VIEW_THREAD_ID)).getText();    
    String memberThreadCount = JMXProperties.getInstance().getProperty("member.M1.numThreads");   
    Assert.assertEquals(memberThreadCount, ThreadCount);
  }

  @Test
  public void testMemberTotalFileDescriptorOpen() throws InterruptedException {
    String SocketCount = driver.findElement(By.id(MEMBER_VIEW_SOCKETS_ID))
        .getText();
    String memberSocketCount = JMXProperties.getInstance().getProperty(
        "member.M1.totalFileDescriptorOpen");
    Assert.assertEquals(memberSocketCount, SocketCount);
  }

 @Test
  public void testMemberLoadAverage() throws InterruptedException {	
    String LoadAvg = driver.findElement(By.id(MEMBER_VIEW_LOADAVG_ID))
        .getText();
    String memberLoadAvg = JMXProperties.getInstance().getProperty(
        "member.M1.loadAverage");
    Assert.assertEquals(memberLoadAvg, LoadAvg);
  }

  @Ignore("not part of pulse-Cedar 7.5")
  @Test
  public void testOffHeapFreeSize(){	  
	  
    String OffHeapFreeSizeString = driver.findElement(
        By.id(MEMBER_VIEW_OFFHEAPFREESIZE_ID)).getText();
    String OffHeapFreeSizetemp = OffHeapFreeSizeString.replaceAll("[a-zA-Z]",
        "");
    float OffHeapFreeSize = Float.parseFloat(OffHeapFreeSizetemp);
    float memberOffHeapFreeSize = Float.parseFloat(JMXProperties.getInstance()
        .getProperty("member.M1.OffHeapFreeSize"));
    if (memberOffHeapFreeSize < 1048576) {
      memberOffHeapFreeSize = memberOffHeapFreeSize / 1024;

    } else if (memberOffHeapFreeSize < 1073741824) {
      memberOffHeapFreeSize = memberOffHeapFreeSize / 1024 / 1024;
    } else {
      memberOffHeapFreeSize = memberOffHeapFreeSize / 1024 / 1024 / 1024;
    }
    memberOffHeapFreeSize = Float.parseFloat(new DecimalFormat("##.##")
        .format(memberOffHeapFreeSize));
    Assert.assertEquals(memberOffHeapFreeSize, OffHeapFreeSize); 
 
  }

  @Ignore("not part of pulse-Cedar 7.5")
  @Test
  public void testOffHeapUsedSize() throws InterruptedException {
	 
    String OffHeapUsedSizeString = driver.findElement(
        By.id(MEMBER_VIEW_OFFHEAPUSEDSIZE_ID)).getText();
    String OffHeapUsedSizetemp = OffHeapUsedSizeString.replaceAll("[a-zA-Z]",
        "");
    float OffHeapUsedSize = Float.parseFloat(OffHeapUsedSizetemp);
    float memberOffHeapUsedSize = Float.parseFloat(JMXProperties.getInstance()
        .getProperty("member.M1.OffHeapUsedSize"));
    if (memberOffHeapUsedSize < 1048576) {
      memberOffHeapUsedSize = memberOffHeapUsedSize / 1024;

    } else if (memberOffHeapUsedSize < 1073741824) {
      memberOffHeapUsedSize = memberOffHeapUsedSize / 1024 / 1024;
    } else {
      memberOffHeapUsedSize = memberOffHeapUsedSize / 1024 / 1024 / 1024;
    }
    memberOffHeapUsedSize = Float.parseFloat(new DecimalFormat("##.##")
        .format(memberOffHeapUsedSize));
    Assert.assertEquals(memberOffHeapUsedSize, OffHeapUsedSize);
  }
   @Ignore("For Gemfire XD")
   @Test  // conflict between UI and properties file
  public void testMemberClients() {  
    String Clients = driver.findElement(By.id(MEMBER_VIEW_CLIENTS_ID))
        .getText();
    
    String memberClientsString = JMXProperties.getInstance().getProperty(
        "gemfirexdmember.M1.NetworkServerClientConnectionStats");
    String[] memberClients = memberClientsString.split(",");
    Assert.assertEquals(memberClients[3], Clients);
  }

  @Test
  public void testMemberJVMPauses(){
   
    String JVMPauses = driver.findElement(By.id(MEMBER_VIEW_JVMPAUSES_ID))
        .getText();
    String memberGcPausesAvg = JMXProperties.getInstance().getProperty(
        "member.M1.JVMPauses");
    Assert.assertEquals(memberGcPausesAvg, JVMPauses);
  }

  @Test
  public void testMemberCPUUsage() {  
    String CPUUsagevalue = driver.findElement(By.id(MEMBER_VIEW_CPUUSAGE_ID))
        .getText();
    String memberCPUUsage = JMXProperties.getInstance().getProperty(
        "member.M1.cpuUsage");
    Assert.assertEquals(memberCPUUsage, CPUUsagevalue);
  }

  @Test  // difference between UI and properties file
  public void testMemberAverageReads() {	  
    float ReadPerSec = Float.parseFloat(driver.findElement(By.id(MEMBER_VIEW_READPERSEC_ID)).getText());    
    float memberReadPerSec = Float.parseFloat(JMXProperties.getInstance().getProperty("member.M1.averageReads"));
    memberReadPerSec = Float.parseFloat(new DecimalFormat("##.##")
    .format(memberReadPerSec));
    Assert.assertEquals(memberReadPerSec, ReadPerSec);
  }

 @Test
  public void testMemberAverageWrites() throws InterruptedException {  
    String WritePerSec = driver.findElement(By.id(MEMBER_VIEW_WRITEPERSEC_ID))
        .getText();
    String memberWritePerSec = JMXProperties.getInstance().getProperty(
        "member.M1.averageWrites");
    Assert.assertEquals(memberWritePerSec, WritePerSec);
  }
 

 @Test   //'Name' and 'type' is displayed blank on UI
  public void testMemberGridViewData(){	 
    searchByIdAndClick("btngridIcon");
    
    // get the number of rows on the grid
    List<WebElement> noOfRows = driver.findElements(By.xpath("//table[@id='memberRegionsList']/tbody/tr"));    
    String MemberRegionName = driver.findElement(By.xpath("//table[@id='memberRegionsList']/tbody/tr[2]/td[1]")).getText();
    String memberRegionName = JMXProperties.getInstance().getProperty("region.R2.name");
    Assert.assertEquals(memberRegionName, MemberRegionName);

    String MemberRegionType = driver.findElement(By.xpath("//table[@id='memberRegionsList']/tbody/tr[2]/td[2]")).getText();
    String memberRegionType = JMXProperties.getInstance().getProperty("region.R2.regionType");
    Assert.assertEquals(memberRegionType, MemberRegionType);
    
    String MemberRegionEntryCount = driver.findElement(By.xpath("//table[@id='memberRegionsList']/tbody/tr[2]/td[3]")).getText();
    String memberRegionEntryCount = JMXProperties.getInstance().getProperty("region.R2.systemRegionEntryCount");    
    Assert.assertEquals(memberRegionEntryCount, MemberRegionEntryCount);
  }

@Test
  public void testDropDownList() throws InterruptedException {
	searchByIdAndClick("memberName");
    searchByLinkAndClick("M3");
    searchByIdAndClick("memberName");
    searchByLinkAndClick("M2");
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewRegionName() throws InterruptedException {
    searchByLinkAndClick(DATA_VIEW_LABEL);
    Thread.sleep(7000);
    searchByIdAndClick("btngridIcon");
    String regionName = driver.findElement(By.id(REGION_NAME_LABEL)).getText();
    String dataviewregionname = JMXProperties.getInstance().getProperty("region.R1.name");
    Assert.assertEquals(dataviewregionname, regionName);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewRegionPath() {
    String regionPath = driver.findElement(By.id(REGION_PATH_LABEL)).getText();
    String dataviewregionpath = JMXProperties.getInstance().getProperty(
        "region.R1.fullPath");
    Assert.assertEquals(dataviewregionpath, regionPath);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewRegionType() {
    String regionType = driver.findElement(By.id(REGION_TYPE_LABEL)).getText();
    String dataviewregiontype = JMXProperties.getInstance().getProperty(
        "region.R1.regionType");
    Assert.assertEquals(dataviewregiontype, regionType);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewEmptyNodes() {
    String regionEmptyNodes = driver.findElement(By.id(DATA_VIEW_EMPTYNODES))
        .getText();
    String dataviewEmptyNodes = JMXProperties.getInstance().getProperty(
        "region.R1.emptyNodes");
    Assert.assertEquals(dataviewEmptyNodes, regionEmptyNodes);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewSystemRegionEntryCount() {
    String regionEntryCount = driver.findElement(By.id(DATA_VIEW_ENTRYCOUNT))
        .getText();
    String dataviewEntryCount = JMXProperties.getInstance().getProperty(
        "region.R1.systemRegionEntryCount");
    Assert.assertEquals(dataviewEntryCount, regionEntryCount);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewPersistentEnabled() {
    String regionPersistence = driver.findElement(
        By.id(REGION_PERSISTENCE_LABEL)).getText();
    String dataviewregionpersistence = JMXProperties.getInstance().getProperty(
        "region.R1.persistentEnabled");
    Assert.assertEquals(dataviewregionpersistence, regionPersistence);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewDiskWritesRate() {
    String regionWrites = driver.findElement(By.id(DATA_VIEW_WRITEPERSEC))
        .getText();
    String dataviewRegionWrites = JMXProperties.getInstance().getProperty(
        "region.R1.diskWritesRate");
    Assert.assertEquals(dataviewRegionWrites, regionWrites);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewDiskReadsRate() {
    String regionReads = driver.findElement(By.id(DATA_VIEW_READPERSEC))
        .getText();
    String dataviewRegionReads = JMXProperties.getInstance().getProperty(
        "region.R1.diskReadsRate");
    Assert.assertEquals(dataviewRegionReads, regionReads);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewDiskUsage() {
    String regionMemoryUsed = driver.findElement(By.id(DATA_VIEW_USEDMEMORY))
        .getText();
    String dataviewMemoryUsed = JMXProperties.getInstance().getProperty(
        "region.R1.diskUsage");
    Assert.assertEquals(dataviewMemoryUsed, regionMemoryUsed);
    searchByLinkAndClick(QUERY_STATISTICS_LABEL);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testDataViewGridValue() {
    String DataViewRegionName = driver.findElement(
        By.xpath("//*[id('6')/x:td[1]]")).getText();
    String dataViewRegionName = JMXProperties.getInstance().getProperty(
        "region.R1.name");
    Assert.assertEquals(dataViewRegionName, DataViewRegionName);

    String DataViewRegionType = driver.findElement(
        By.xpath("//*[id('6')/x:td[2]")).getText();
    String dataViewRegionType = JMXProperties.getInstance().getProperty(
        "region.R2.regionType");
    Assert.assertEquals(dataViewRegionType, DataViewRegionType);

    String DataViewEntryCount = driver.findElement(
        By.xpath("//*[id('6')/x:td[3]")).getText();
    String dataViewEntryCount = JMXProperties.getInstance().getProperty(
        "region.R2.systemRegionEntryCount");
    Assert.assertEquals(dataViewEntryCount, DataViewEntryCount);

    String DataViewEntrySize = driver.findElement(
        By.xpath("//*[id('6')/x:td[4]")).getText();
    String dataViewEntrySize = JMXProperties.getInstance().getProperty(
        "region.R2.entrySize");
    Assert.assertEquals(dataViewEntrySize, DataViewEntrySize);

  }
  
  
  public void loadDataBrowserpage() {
	  searchByLinkAndClick(DATA_BROWSER_LABEL);
	  //Thread.sleep(7000);
  }
  
  @Test
  public void testDataBrowserRegionName() throws InterruptedException {
	  loadDataBrowserpage();
	  String DataBrowserRegionName1 = driver.findElement(By.id(DATA_BROWSER_REGIONName1))
			  .getText();
	  String databrowserRegionNametemp1 = JMXProperties.getInstance().getProperty(
		        "region.R1.name");
	  String databrowserRegionName1 = databrowserRegionNametemp1.replaceAll("[\\/]", "");
	  Assert.assertEquals(databrowserRegionName1, DataBrowserRegionName1);
	  
	  String DataBrowserRegionName2 = driver.findElement(By.id(DATA_BROWSER_REGIONName2))
			  .getText();
	  String databrowserRegionNametemp2 = JMXProperties.getInstance().getProperty(
		        "region.R2.name");
	  String databrowserRegionName2 = databrowserRegionNametemp2.replaceAll("[\\/]", "");
	  Assert.assertEquals(databrowserRegionName2, DataBrowserRegionName2);
	  
	  String DataBrowserRegionName3 = driver.findElement(By.id(DATA_BROWSER_REGIONName3))
			  .getText();
	  String databrowserRegionNametemp3 = JMXProperties.getInstance().getProperty(
		        "region.R3.name");
	  String databrowserRegionName3 = databrowserRegionNametemp3.replaceAll("[\\/]", "");
	  Assert.assertEquals(databrowserRegionName3, DataBrowserRegionName3);
	        
  }
  
  @Test
  public void testDataBrowserRegionMembersVerificaition() throws InterruptedException {
	  loadDataBrowserpage();
	  searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);
	  String DataBrowserMember1Name1 = driver.findElement(By.xpath("//label[@for='Member0']"))
			  .getText();
	  String DataBrowserMember1Name2 = driver.findElement(By.xpath("//label[@for='Member1']"))
			  .getText();
	  String DataBrowserMember1Name3 = driver.findElement(By.xpath("//label[@for='Member2']"))
			  .getText();
	  String databrowserMember1Names = JMXProperties.getInstance().getProperty(
		        "region.R1.members");
	  
	  String databrowserMember1Names1 = databrowserMember1Names.substring(0, 2);
	  Assert.assertEquals(databrowserMember1Names1, DataBrowserMember1Name1);
	  
	  String databrowserMember1Names2 = databrowserMember1Names.substring(3, 5);
	  Assert.assertEquals(databrowserMember1Names2, DataBrowserMember1Name2);
	  
	  String databrowserMember1Names3 = databrowserMember1Names.substring(6, 8);
	  Assert.assertEquals(databrowserMember1Names3, DataBrowserMember1Name3);
	  searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);
	  
	  searchByIdAndClick(DATA_BROWSER_REGION2_CHECKBOX);
	  String DataBrowserMember2Name1 = driver.findElement(By.xpath("//label[@for='Member0']"))
			  .getText();
	  String DataBrowserMember2Name2 = driver.findElement(By.xpath("//label[@for='Member1']"))
			  .getText();
	  String databrowserMember2Names = JMXProperties.getInstance().getProperty(
		        "region.R2.members");
	  
	  String databrowserMember2Names1 = databrowserMember2Names.substring(0, 2);
	  Assert.assertEquals(databrowserMember2Names1, DataBrowserMember2Name1);
	  
	  String databrowserMember2Names2 = databrowserMember2Names.substring(3, 5);
	  Assert.assertEquals(databrowserMember2Names2, DataBrowserMember2Name2);
	  searchByIdAndClick(DATA_BROWSER_REGION2_CHECKBOX);
	  
	  searchByIdAndClick(DATA_BROWSER_REGION3_CHECKBOX);
	  String DataBrowserMember3Name1 = driver.findElement(By.xpath("//label[@for='Member0']"))
			  .getText();
	  String DataBrowserMember3Name2 = driver.findElement(By.xpath("//label[@for='Member1']"))
			  .getText();
	  String databrowserMember3Names = JMXProperties.getInstance().getProperty(
		        "region.R3.members");
	  
	  String databrowserMember3Names1 = databrowserMember3Names.substring(0, 2);
	  Assert.assertEquals(databrowserMember3Names1, DataBrowserMember3Name1);
	  
	  String databrowserMember3Names2 = databrowserMember3Names.substring(3, 5);
	  Assert.assertEquals(databrowserMember3Names2, DataBrowserMember3Name2);
	  searchByIdAndClick(DATA_BROWSER_REGION3_CHECKBOX);
  }
  
  @Test
  public void testDataBrowserColocatedRegions() throws InterruptedException {
	  loadDataBrowserpage();
	  String databrowserMemberNames1 = JMXProperties.getInstance().getProperty(
		        "region.R1.members");
	  String databrowserMemberNames2 = JMXProperties.getInstance().getProperty(
		        "region.R2.members");
	  String databrowserMemberNames3 = JMXProperties.getInstance().getProperty(
		        "region.R3.members");
	  
	  if((databrowserMemberNames1.matches(databrowserMemberNames2+"(.*)"))) {
		  if((databrowserMemberNames1.matches(databrowserMemberNames3+"(.*)"))) {
			  if((databrowserMemberNames2.matches(databrowserMemberNames3+"(.*)"))) {
				  System.out.println("R1, R2 and R3 are colocated regions");
			  }   
		  }
	  }
	  searchByIdAndClick(DATA_BROWSER_REGION1_CHECKBOX);
	  searchByLinkAndClick(DATA_BROWSER_COLOCATED_REGION);
	  String DataBrowserColocatedRegion1 = driver.findElement(By.id(DATA_BROWSER_COLOCATED_REGION_NAME1))
			  .getText();
	  String DataBrowserColocatedRegion2 = driver.findElement(By.id(DATA_BROWSER_COLOCATED_REGION_NAME2))
			  .getText();
	  String DataBrowserColocatedRegion3 = driver.findElement(By.id(DATA_BROWSER_COLOCATED_REGION_NAME3))
			  .getText();
	  
	  String databrowserColocatedRegiontemp1 = JMXProperties.getInstance().getProperty(
		        "region.R1.name");
	  String databrowserColocatedRegion1 = databrowserColocatedRegiontemp1.replaceAll("[\\/]", "");
	  
	  String databrowserColocatedRegiontemp2 = JMXProperties.getInstance().getProperty(
		        "region.R2.name");
	  String databrowserColocatedRegion2 = databrowserColocatedRegiontemp2.replaceAll("[\\/]", "");
	  
	  String databrowserColocatedRegiontemp3 = JMXProperties.getInstance().getProperty(
		        "region.R3.name");
	  String databrowserColocatedRegion3 = databrowserColocatedRegiontemp3.replaceAll("[\\/]", "");
	  
	  Assert.assertEquals(databrowserColocatedRegion1, DataBrowserColocatedRegion1);
	  Assert.assertEquals(databrowserColocatedRegion2, DataBrowserColocatedRegion2);
	  Assert.assertEquals(databrowserColocatedRegion3, DataBrowserColocatedRegion3);
	  
  }

  @Ignore("Bad Test") // clusterDetails element not found on Data Browser page. No assertions in test
  @Test
  public void testDataBrowserQueryValidation() throws IOException, InterruptedException {
	  loadDataBrowserpage();
	  WebElement textArea = driver.findElement(By.id("dataBrowserQueryText"));
	  textArea.sendKeys("query1");
	  WebElement executeButton = driver.findElement(By.id("btnExecuteQuery"));
	  executeButton.click();
	  String QueryResultHeader1 = driver.findElement(By.xpath("//div[@id='clusterDetails']/div/div/span[@class='n-title']")).getText();
	  double count = 0,countBuffer=0,countLine=0;
	  String lineNumber = "";
	  String filePath = "E:\\springsource\\springsourceWS\\Pulse-Cedar\\src\\main\\resources\\testQueryResultSmall.txt";
	  BufferedReader br;
	  String line = "";
	  br = new BufferedReader(new FileReader(filePath));
	  while((line = br.readLine()) != null)
	  {
		  countLine++;
          //System.out.println(line);
          String[] words = line.split(" ");

          for (String word : words) {
            if (word.equals(QueryResultHeader1)) {
              count++;
              countBuffer++;
            }
          }
	  }  
  }
  
 public void testTreeMapPopUpData(String S1, String gridIcon) {
	  for (int i = 1; i <=3; i++) {
		  searchByLinkAndClick(CLUSTER_VIEW_LABEL);
		  if (gridIcon.equals(SERVER_GROUP_GRID_ID)) {
			  WebElement ServerGroupRadio = driver.findElement(By.xpath("//label[@for='radio-servergroups']"));
			  ServerGroupRadio.click();
		  }
		  if (gridIcon.equals(REDUNDANCY_GRID_ID)) {
			  WebElement ServerGroupRadio = driver.findElement(By.xpath("//label[@for='radio-redundancyzones']"));
			  ServerGroupRadio.click();
		  }
		  searchByIdAndClick(gridIcon);
		  WebElement TreeMapMember = driver.findElement(By.xpath("//div[@id='" + S1 + "M"+ (i) + "']/div"));
		  Actions builder = new Actions(driver);
		  builder.clickAndHold(TreeMapMember).perform();
		  int j = 1;
		  String CPUUsageM1temp = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div/div[2]/div"))
				  .getText();
		  String CPUUsageM1 = CPUUsageM1temp.replaceAll("[\\%]", "");
		  String cpuUsageM1 = JMXProperties.getInstance().getProperty(
			        "member.M" + (i) + ".cpuUsage");
		  Assert.assertEquals(cpuUsageM1, CPUUsageM1);
			  
		  String MemoryUsageM1temp = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 1) + "]/div[2]/div"))
				  .getText();
		  String MemoryUsageM1 = MemoryUsageM1temp.replaceAll("MB", "");
		  String memoryUsageM1 = JMXProperties.getInstance().getProperty(
				  "member.M" + (i) + ".UsedMemory");
		  Assert.assertEquals(memoryUsageM1, MemoryUsageM1);
		  
		  String LoadAvgM1 = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 2) + "]/div[2]/div"))
				  .getText();
		  String loadAvgM1 = JMXProperties.getInstance().getProperty(
				  "member.M" + (i) + ".loadAverage");
		  Assert.assertEquals(loadAvgM1, LoadAvgM1);
		  
		  
		  String ThreadsM1 = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 3) + "]/div[2]/div"))
				  .getText();
		  String threadsM1 = JMXProperties.getInstance().getProperty(
				  "member.M" + (i) + ".numThreads");
		  Assert.assertEquals(threadsM1, ThreadsM1);
		  
		  String SocketsM1 = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[" + (j + 4) + "]/div[2]/div"))
				  .getText();
		  String socketsM1 = JMXProperties.getInstance().getProperty(
				  "member.M" + (i) + ".totalFileDescriptorOpen");
		  Assert.assertEquals(socketsM1, SocketsM1);
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
	  WebElement TreeMapMember = driver.findElement(By.id("GraphTreeMapClusterData-canvas"));
	  Actions builder = new Actions(driver);
	  builder.clickAndHold(TreeMapMember).perform();
	  String RegionType = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div/div[2]/div"))
			  .getText();
	  String regionType = JMXProperties.getInstance().getProperty(
			  "region.R2.regionType");
	  Assert.assertEquals(regionType, RegionType);
	  
	  String EntryCount = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[2]/div[2]/div"))
			  .getText();
	  String entryCount = JMXProperties.getInstance().getProperty(
			  "region.R2.systemRegionEntryCount");
	  Assert.assertEquals(entryCount, EntryCount);
	  
	  String EntrySizetemp = driver.findElement(By.xpath("//div[@id='_tooltip']/div/div/div[2]/div[3]/div[2]/div"))
			  .getText();
	  float EntrySize = Float.parseFloat(EntrySizetemp);
	  float entrySize = Float.parseFloat(JMXProperties.getInstance().getProperty(
			  "region.R2.entrySize"));
	  entrySize = entrySize / 1024 / 1024;
	  entrySize = Float.parseFloat(new DecimalFormat("##.####")
      .format(entrySize));
	  Assert.assertEquals(entrySize, EntrySize);  
	  builder.moveToElement(TreeMapMember).release().perform();
  }
  
  @Test
  public void testRegionViewTreeMapPopUpData() {
	  searchByLinkAndClick(CLUSTER_VIEW_LABEL);
	  searchByLinkAndClick(DATA_DROPDOWN_ID);
	  WebElement TreeMapMember = driver.findElement(By.id("GraphTreeMapClusterData-canvas"));
	  TreeMapMember.click();
  }
  
  
  
  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void loadQueryStatisticspage() throws InterruptedException {
    searchByLinkAndClick(QUERY_STATISTICS_LABEL);
    Thread.sleep(7000);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testQueryDefinitionValidation() {

    String QueryDefinition1 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[2]/td[1]"))
        .getText();
    String queryDefinition1 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q12.queryDefinition");
    Assert.assertEquals(queryDefinition1, QueryDefinition1);

    String QueryDefinition2 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[3]/td[1]"))
        .getText();
    String queryDefinition2 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q14.queryDefinition");
    Assert.assertEquals(queryDefinition2, QueryDefinition2);

    String QueryDefinition3 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[4]/td[1]"))
        .getText();
    String queryDefinition3 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q20.queryDefinition");
    Assert.assertEquals(queryDefinition3, QueryDefinition3);

    String QueryDefinition4 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[5]/td[1]"))
        .getText();
    String queryDefinition4 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q24.queryDefinition");
    Assert.assertEquals(queryDefinition4, QueryDefinition4);

    String QueryDefinition5 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[6]/td[1]"))
        .getText();
    String queryDefinition5 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q11.queryDefinition");
    Assert.assertEquals(queryDefinition5, QueryDefinition5);

    String QueryDefinition6 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[7]/td[1]"))
        .getText();
    String queryDefinition6 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q18.queryDefinition");
    Assert.assertEquals(queryDefinition6, QueryDefinition6);

    String QueryDefinition7 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[8]/td[1]"))
        .getText();
    String queryDefinition7 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q15.queryDefinition");
    Assert.assertEquals(queryDefinition7, QueryDefinition7);

    String QueryDefinition8 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[9]/td[1]"))
        .getText();
    String queryDefinition8 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q23.queryDefinition");
    Assert.assertEquals(queryDefinition8, QueryDefinition8);

    String QueryDefinition9 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[10]/td[1]"))
        .getText();
    String queryDefinition9 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q10.queryDefinition");
    Assert.assertEquals(queryDefinition9, QueryDefinition9);

    String QueryDefinition10 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[11]/td[1]"))
        .getText();
    String queryDefinition10 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q21.queryDefinition");
    Assert.assertEquals(queryDefinition10, QueryDefinition10);

    String QueryDefinition11 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[12]/td[1]"))
        .getText();
    String queryDefinition11 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q16.queryDefinition");
    Assert.assertEquals(queryDefinition11, QueryDefinition11);

    String QueryDefinition12 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[13]/td[1]"))
        .getText();
    String queryDefinition12 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q17.queryDefinition");
    Assert.assertEquals(queryDefinition12, QueryDefinition12);

    String QueryDefinition13 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[14]/td[1]"))
        .getText();
    String queryDefinition13 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q13.queryDefinition");
    Assert.assertEquals(queryDefinition13, QueryDefinition13);

    String QueryDefinition14 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[15]/td[1]"))
        .getText();
    String queryDefinition14 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q19.queryDefinition");
    Assert.assertEquals(queryDefinition14, QueryDefinition14);

    String QueryDefinition15 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[16]/td[1]"))
        .getText();
    String queryDefinition15 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q25.queryDefinition");
    Assert.assertEquals(queryDefinition15, QueryDefinition15);

    String QueryDefinition16 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[17]/td[1]"))
        .getText();
    String queryDefinition16 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q22.queryDefinition");
    Assert.assertEquals(queryDefinition16, QueryDefinition16);

    String QueryDefinition17 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[18]/td[1]"))
        .getText();
    String queryDefinition17 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q4.queryDefinition");
    Assert.assertEquals(queryDefinition17, QueryDefinition17);

    String QueryDefinition18 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[19]/td[1]"))
        .getText();
    String queryDefinition18 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q5.queryDefinition");
    Assert.assertEquals(queryDefinition18, QueryDefinition18);

    String QueryDefinition19 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[20]/td[1]"))
        .getText();
    String queryDefinition19 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q2.queryDefinition");
    Assert.assertEquals(queryDefinition19, QueryDefinition19);

    String QueryDefinition20 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[21]/td[1]"))
        .getText();
    String queryDefinition20 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q7.queryDefinition");
    Assert.assertEquals(queryDefinition20, QueryDefinition20);

    String QueryDefinition21 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[22]/td[1]"))
        .getText();
    String queryDefinition21 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q8.queryDefinition");
    Assert.assertEquals(queryDefinition21, QueryDefinition21);

    String QueryDefinition22 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[23]/td[1]"))
        .getText();
    String queryDefinition22 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q9.queryDefinition");
    Assert.assertEquals(queryDefinition22, QueryDefinition22);

    String QueryDefinition23 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[24]/td[1]"))
        .getText();
    String queryDefinition23 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q1.queryDefinition");
    Assert.assertEquals(queryDefinition23, QueryDefinition23);

    String QueryDefinition24 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[25]/td[1]"))
        .getText();
    String queryDefinition24 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q3.queryDefinition");
    Assert.assertEquals(queryDefinition24, QueryDefinition24);

    scrollbarVerticalDownScroll();

    String QueryDefinition25 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[26]/td[1]"))
        .getText();
    String queryDefinition25 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q6.queryDefinition");
    Assert.assertEquals(queryDefinition25, QueryDefinition25);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testNumExecution() throws InterruptedException {

    String QueryNumExecution1 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[2]/td[2]"))
        .getText();
    String queryNumExecution1 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q12.numExecution");
    Assert.assertEquals(queryNumExecution1, QueryNumExecution1);

    String QueryNumExecution2 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[3]/td[2]"))
        .getText();
    String queryNumExecution2 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q14.numExecution");
    Assert.assertEquals(queryNumExecution2, QueryNumExecution2);

    String QueryNumExecution3 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[4]/td[2]"))
        .getText();
    String queryNumExecution3 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q20.numExecution");
    Assert.assertEquals(queryNumExecution3, QueryNumExecution3);

    String QueryNumExecution4 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[5]/td[2]"))
        .getText();
    String queryNumExecution4 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q24.numExecution");
    Assert.assertEquals(queryNumExecution4, QueryNumExecution4);

    String QueryNumExecution5 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[6]/td[2]"))
        .getText();
    String queryNumExecution5 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q11.numExecution");
    Assert.assertEquals(queryNumExecution5, QueryNumExecution5);

    String QueryNumExecution6 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[7]/td[2]"))
        .getText();
    String queryNumExecution6 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q18.numExecution");
    Assert.assertEquals(queryNumExecution6, QueryNumExecution6);

    String QueryNumExecution7 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[8]/td[2]"))
        .getText();
    String queryNumExecution7 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q15.numExecution");
    Assert.assertEquals(queryNumExecution7, QueryNumExecution7);

    String QueryNumExecution8 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[9]/td[2]"))
        .getText();
    String queryNumExecution8 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q23.numExecution");
    Assert.assertEquals(queryNumExecution8, QueryNumExecution8);

    String QueryNumExecution9 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[10]/td[2]"))
        .getText();
    String queryNumExecution9 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q10.numExecution");
    Assert.assertEquals(queryNumExecution9, QueryNumExecution9);

    String QueryNumExecution10 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[11]/td[2]"))
        .getText();
    String queryNumExecution10 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q21.numExecution");
    Assert.assertEquals(queryNumExecution10, QueryNumExecution10);

    String QueryNumExecution11 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[12]/td[2]"))
        .getText();
    String queryNumExecution11 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q16.numExecution");
    Assert.assertEquals(queryNumExecution11, QueryNumExecution11);

    String QueryNumExecution12 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[13]/td[2]"))
        .getText();
    String queryNumExecution12 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q17.numExecution");
    Assert.assertEquals(queryNumExecution12, QueryNumExecution12);

    String QueryNumExecution13 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[14]/td[2]"))
        .getText();
    String queryNumExecution13 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q13.numExecution");
    Assert.assertEquals(queryNumExecution13, QueryNumExecution13);

    String QueryNumExecution14 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[15]/td[2]"))
        .getText();
    String queryNumExecution14 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q19.numExecution");
    Assert.assertEquals(queryNumExecution14, QueryNumExecution14);

    String QueryNumExecution15 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[16]/td[2]"))
        .getText();
    String queryNumExecution15 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q25.numExecution");
    Assert.assertEquals(queryNumExecution15, QueryNumExecution15);

    String QueryNumExecution16 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[17]/td[2]"))
        .getText();
    String queryNumExecution16 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q22.numExecution");
    Assert.assertEquals(queryNumExecution16, QueryNumExecution16);

    String QueryNumExecution17 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[18]/td[2]"))
        .getText();
    String queryNumExecution17 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q4.numExecution");
    Assert.assertEquals(queryNumExecution17, QueryNumExecution17);

    String QueryNumExecution18 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[19]/td[2]"))
        .getText();
    String queryNumExecution18 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q5.numExecution");
    Assert.assertEquals(queryNumExecution18, QueryNumExecution18);

    String QueryNumExecution19 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[20]/td[2]"))
        .getText();
    String queryNumExecution19 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q2.numExecution");
    Assert.assertEquals(queryNumExecution19, QueryNumExecution19);

    String QueryNumExecution20 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[21]/td[2]"))
        .getText();
    String queryNumExecution20 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q7.numExecution");
    Assert.assertEquals(queryNumExecution20, QueryNumExecution20);

    String QueryNumExecution21 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[22]/td[2]"))
        .getText();
    String queryNumExecution21 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q8.numExecution");
    Assert.assertEquals(queryNumExecution21, QueryNumExecution21);

    String QueryNumExecution22 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[23]/td[2]"))
        .getText();
    String queryNumExecution22 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q9.numExecution");
    Assert.assertEquals(queryNumExecution22, QueryNumExecution22);

    String QueryNumExecution23 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[24]/td[2]"))
        .getText();
    String queryNumExecution23 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q1.numExecution");
    Assert.assertEquals(queryNumExecution23, QueryNumExecution23);

    String QueryNumExecution24 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[25]/td[2]"))
        .getText();
    String queryNumExecution24 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q3.numExecution");
    Assert.assertEquals(queryNumExecution24, QueryNumExecution24);

    scrollbarVerticalDownScroll();

    String QueryNumExecution25 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[26]/td[2]"))
        .getText();
    String queryNumExecution25 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q6.numExecution");
    Assert.assertEquals(queryNumExecution25, QueryNumExecution25);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testTotalExecutionTime() {

    String QueryTotalExecutionTime1 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[2]/td[3]"))
        .getText();
    String queryTotalExecutionTime1 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q12.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime1, QueryTotalExecutionTime1);

    String QueryTotalExecutionTime2 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[3]/td[3]"))
        .getText();
    String queryTotalExecutionTime2 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q14.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime2, QueryTotalExecutionTime2);

    String QueryTotalExecutionTime3 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[4]/td[3]"))
        .getText();
    String queryTotalExecutionTime3 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q20.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime3, QueryTotalExecutionTime3);

    String QueryTotalExecutionTime4 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[5]/td[3]"))
        .getText();
    String queryTotalExecutionTime4 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q24.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime4, QueryTotalExecutionTime4);

    String QueryTotalExecutionTime5 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[6]/td[3]"))
        .getText();
    String queryTotalExecutionTime5 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q11.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime5, QueryTotalExecutionTime5);

    String QueryTotalExecutionTime6 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[7]/td[3]"))
        .getText();
    String queryTotalExecutionTime6 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q18.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime6, QueryTotalExecutionTime6);

    String QueryTotalExecutionTime7 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[8]/td[3]"))
        .getText();
    String queryTotalExecutionTime7 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q15.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime7, QueryTotalExecutionTime7);

    String QueryTotalExecutionTime8 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[9]/td[3]"))
        .getText();
    String queryTotalExecutionTime8 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q23.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime8, QueryTotalExecutionTime8);

    String QueryTotalExecutionTime9 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[10]/td[3]"))
        .getText();
    String queryTotalExecutionTime9 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q10.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime9, QueryTotalExecutionTime9);

    String QueryTotalExecutionTime10 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[11]/td[3]"))
        .getText();
    String queryTotalExecutionTime10 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q21.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime10, QueryTotalExecutionTime10);

    String QueryTotalExecutionTime11 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[12]/td[3]"))
        .getText();
    String queryTotalExecutionTime11 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q16.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime11, QueryTotalExecutionTime11);

    String QueryTotalExecutionTime12 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[13]/td[3]"))
        .getText();
    String queryTotalExecutionTime12 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q17.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime12, QueryTotalExecutionTime12);

    String QueryTotalExecutionTime13 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[14]/td[3]"))
        .getText();
    String queryTotalExecutionTime13 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q13.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime13, QueryTotalExecutionTime13);

    String QueryTotalExecutionTime14 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[15]/td[3]"))
        .getText();
    String queryTotalExecutionTime14 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q19.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime14, QueryTotalExecutionTime14);

    String QueryTotalExecutionTime15 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[16]/td[3]"))
        .getText();
    String queryTotalExecutionTime15 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q25.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime15, QueryTotalExecutionTime15);

    String QueryTotalExecutionTime16 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[17]/td[3]"))
        .getText();
    String queryTotalExecutionTime16 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q22.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime16, QueryTotalExecutionTime16);

    String QueryTotalExecutionTime17 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[18]/td[3]"))
        .getText();
    String queryTotalExecutionTime17 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q4.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime17, QueryTotalExecutionTime17);

    String QueryTotalExecutionTime18 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[19]/td[3]"))
        .getText();
    String queryTotalExecutionTime18 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q5.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime18, QueryTotalExecutionTime18);

    String QueryTotalExecutionTime19 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[20]/td[3]"))
        .getText();
    String queryTotalExecutionTime19 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q2.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime19, QueryTotalExecutionTime19);

    String QueryTotalExecutionTime20 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[21]/td[3]"))
        .getText();
    String queryTotalExecutionTime20 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q7.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime20, QueryTotalExecutionTime20);

    String QueryTotalExecutionTime21 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[22]/td[3]"))
        .getText();
    String queryTotalExecutionTime21 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q8.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime21, QueryTotalExecutionTime21);

    String QueryTotalExecutionTime22 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[23]/td[3]"))
        .getText();
    String queryTotalExecutionTime22 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q9.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime22, QueryTotalExecutionTime22);

    String QueryTotalExecutionTime23 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[24]/td[3]"))
        .getText();
    String queryTotalExecutionTime23 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q1.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime23, QueryTotalExecutionTime23);

    String QueryTotalExecutionTime24 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[25]/td[3]"))
        .getText();
    String queryTotalExecutionTime24 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q3.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime24, QueryTotalExecutionTime24);

    scrollbarVerticalDownScroll();

    String QueryTotalExecutionTime25 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[26]/td[3]"))
        .getText();
    String queryTotalExecutionTime25 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q6.totalExecutionTime");
    Assert.assertEquals(queryTotalExecutionTime25, QueryTotalExecutionTime25);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testNumExecutionsInProgress() {

    String QueryNumExecutionsInProgress1 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[2]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress1 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q12.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress1,
        QueryNumExecutionsInProgress1);

    String QueryNumExecutionsInProgress2 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[3]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress2 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q14.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress2,
        QueryNumExecutionsInProgress2);

    String QueryNumExecutionsInProgress3 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[4]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress3 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q20.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress3,
        QueryNumExecutionsInProgress3);

    String QueryNumExecutionsInProgress4 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[5]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress4 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q24.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress4,
        QueryNumExecutionsInProgress4);

    String QueryNumExecutionsInProgress5 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[6]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress5 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q11.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress5,
        QueryNumExecutionsInProgress5);

    String QueryNumExecutionsInProgress6 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[7]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress6 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q18.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress6,
        QueryNumExecutionsInProgress6);

    String QueryNumExecutionsInProgress7 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[8]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress7 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q15.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress7,
        QueryNumExecutionsInProgress7);

    String QueryNumExecutionsInProgress8 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[9]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress8 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q23.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress8,
        QueryNumExecutionsInProgress8);

    String QueryNumExecutionsInProgress9 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[10]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress9 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q10.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress9,
        QueryNumExecutionsInProgress9);

    String QueryNumExecutionsInProgress10 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[11]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress10 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q21.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress10,
        QueryNumExecutionsInProgress10);

    String QueryNumExecutionsInProgress11 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[12]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress11 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q16.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress11,
        QueryNumExecutionsInProgress11);

    String QueryNumExecutionsInProgress12 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[13]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress12 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q17.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress12,
        QueryNumExecutionsInProgress12);

    String QueryNumExecutionsInProgress13 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[14]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress13 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q13.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress13,
        QueryNumExecutionsInProgress13);

    String QueryNumExecutionsInProgress14 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[15]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress14 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q19.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress14,
        QueryNumExecutionsInProgress14);

    String QueryNumExecutionsInProgress15 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[16]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress15 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q25.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress15,
        QueryNumExecutionsInProgress15);

    String QueryNumExecutionsInProgress16 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[17]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress16 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q22.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress16,
        QueryNumExecutionsInProgress16);

    String QueryNumExecutionsInProgress17 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[18]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress17 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q4.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress17,
        QueryNumExecutionsInProgress17);

    String QueryNumExecutionsInProgress18 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[19]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress18 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q5.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress18,
        QueryNumExecutionsInProgress18);

    String QueryNumExecutionsInProgress19 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[20]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress19 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q2.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress19,
        QueryNumExecutionsInProgress19);

    String QueryNumExecutionsInProgress20 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[21]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress20 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q7.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress20,
        QueryNumExecutionsInProgress20);

    String QueryNumExecutionsInProgress21 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[22]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress21 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q8.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress21,
        QueryNumExecutionsInProgress21);

    String QueryNumExecutionsInProgress22 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[23]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress22 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q9.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress22,
        QueryNumExecutionsInProgress22);

    String QueryNumExecutionsInProgress23 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[24]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress23 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q1.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress23,
        QueryNumExecutionsInProgress23);

    String QueryNumExecutionsInProgress24 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[25]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress24 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q3.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress24,
        QueryNumExecutionsInProgress24);

    scrollbarVerticalDownScroll();

    String QueryNumExecutionsInProgress25 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[26]/td[4]"))
        .getText();
    String queryNumExecutionsInProgress25 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q6.numExecutionsInProgress");
    Assert.assertEquals(queryNumExecutionsInProgress25,
        QueryNumExecutionsInProgress25);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testNumTimesCompiled() {

    String QueryNumTimesCompiled1 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[2]/td[5]"))
        .getText();
    String queryNumTimesCompiled1 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q12.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled1, QueryNumTimesCompiled1);

    String QueryNumTimesCompiled2 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[3]/td[5]"))
        .getText();
    String queryNumTimesCompiled2 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q14.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled2, QueryNumTimesCompiled2);

    String QueryNumTimesCompiled3 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[4]/td[5]"))
        .getText();
    String queryNumTimesCompiled3 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q20.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled3, QueryNumTimesCompiled3);

    String QueryNumTimesCompiled4 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[5]/td[5]"))
        .getText();
    String queryNumTimesCompiled4 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q24.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled4, QueryNumTimesCompiled4);

    String QueryNumTimesCompiled5 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[6]/td[5]"))
        .getText();
    String queryNumTimesCompiled5 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q11.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled5, QueryNumTimesCompiled5);

    String QueryNumTimesCompiled6 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[7]/td[5]"))
        .getText();
    String queryNumTimesCompiled6 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q18.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled6, QueryNumTimesCompiled6);

    String QueryNumTimesCompiled7 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[8]/td[5]"))
        .getText();
    String queryNumTimesCompiled7 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q15.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled7, QueryNumTimesCompiled7);

    String QueryNumTimesCompiled8 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[9]/td[5]"))
        .getText();
    String queryNumTimesCompiled8 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q23.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled8, QueryNumTimesCompiled8);

    String QueryNumTimesCompiled9 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[10]/td[5]"))
        .getText();
    String queryNumTimesCompiled9 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q10.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled9, QueryNumTimesCompiled9);

    String QueryNumTimesCompiled10 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[11]/td[5]"))
        .getText();
    String queryNumTimesCompiled10 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q21.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled10, QueryNumTimesCompiled10);

    String QueryNumTimesCompiled11 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[12]/td[5]"))
        .getText();
    String queryNumTimesCompiled11 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q16.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled11, QueryNumTimesCompiled11);

    String QueryNumTimesCompiled12 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[13]/td[5]"))
        .getText();
    String queryNumTimesCompiled12 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q17.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled12, QueryNumTimesCompiled12);

    String QueryNumTimesCompiled13 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[14]/td[5]"))
        .getText();
    String queryNumTimesCompiled13 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q13.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled13, QueryNumTimesCompiled13);

    String QueryNumTimesCompiled14 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[15]/td[5]"))
        .getText();
    String queryNumTimesCompiled14 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q19.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled14, QueryNumTimesCompiled14);

    String QueryNumTimesCompiled15 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[16]/td[5]"))
        .getText();
    String queryNumTimesCompiled15 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q25.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled15, QueryNumTimesCompiled15);

    String QueryNumTimesCompiled16 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[17]/td[5]"))
        .getText();
    String queryNumTimesCompiled16 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q22.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled16, QueryNumTimesCompiled16);

    String QueryNumTimesCompiled17 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[18]/td[5]"))
        .getText();
    String queryNumTimesCompiled17 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q4.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled17, QueryNumTimesCompiled17);

    String QueryNumTimesCompiled18 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[19]/td[5]"))
        .getText();
    String queryNumTimesCompiled18 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q5.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled18, QueryNumTimesCompiled18);

    String QueryNumTimesCompiled19 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[20]/td[5]"))
        .getText();
    String queryNumTimesCompiled19 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q2.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled19, QueryNumTimesCompiled19);

    String QueryNumTimesCompiled20 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[21]/td[5]"))
        .getText();
    String queryNumTimesCompiled20 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q7.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled20, QueryNumTimesCompiled20);

    String QueryNumTimesCompiled21 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[22]/td[5]"))
        .getText();
    String queryNumTimesCompiled21 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q8.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled21, QueryNumTimesCompiled21);

    String QueryNumTimesCompiled22 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[23]/td[5]"))
        .getText();
    String queryNumTimesCompiled22 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q9.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled22, QueryNumTimesCompiled22);

    String QueryNumTimesCompiled23 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[24]/td[5]"))
        .getText();
    String queryNumTimesCompiled23 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q1.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled23, QueryNumTimesCompiled23);

    String QueryNumTimesCompiled24 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[25]/td[5]"))
        .getText();
    String queryNumTimesCompiled24 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q3.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled24, QueryNumTimesCompiled24);

    scrollbarVerticalDownScroll();

    String QueryNumTimesCompiled25 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[26]/td[5]"))
        .getText();
    String queryNumTimesCompiled25 = JMXProperties.getInstance().getProperty(
        "aggregatestatement.Q6.numTimesCompiled");
    Assert.assertEquals(queryNumTimesCompiled25, QueryNumTimesCompiled25);
  }

  @Ignore("Not part of Pulse-Cedar 7.5 release")
  @Test
  public void testNumTimesGlobalIndexLookup() {

    String QueryNumTimesGlobalIndexLookup1 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[2]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup1 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q12.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup1,
        QueryNumTimesGlobalIndexLookup1);

    String QueryNumTimesGlobalIndexLookup2 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[3]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup2 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q14.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup2,
        QueryNumTimesGlobalIndexLookup2);

    String QueryNumTimesGlobalIndexLookup3 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[4]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup3 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q20.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup3,
        QueryNumTimesGlobalIndexLookup3);

    String QueryNumTimesGlobalIndexLookup4 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[5]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup4 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q24.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup4,
        QueryNumTimesGlobalIndexLookup4);

    String QueryNumTimesGlobalIndexLookup5 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[6]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup5 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q11.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup5,
        QueryNumTimesGlobalIndexLookup5);

    String QueryNumTimesGlobalIndexLookup6 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[7]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup6 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q18.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup6,
        QueryNumTimesGlobalIndexLookup6);

    String QueryNumTimesGlobalIndexLookup7 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[8]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup7 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q15.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup7,
        QueryNumTimesGlobalIndexLookup7);

    String QueryNumTimesGlobalIndexLookup8 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[9]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup8 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q23.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup8,
        QueryNumTimesGlobalIndexLookup8);

    String QueryNumTimesGlobalIndexLookup9 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[10]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup9 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q10.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup9,
        QueryNumTimesGlobalIndexLookup9);

    String QueryNumTimesGlobalIndexLookup10 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[11]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup10 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q21.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup10,
        QueryNumTimesGlobalIndexLookup10);

    String QueryNumTimesGlobalIndexLookup11 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[12]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup11 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q16.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup11,
        QueryNumTimesGlobalIndexLookup11);

    String QueryNumTimesGlobalIndexLookup12 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[13]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup12 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q17.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup12,
        QueryNumTimesGlobalIndexLookup12);

    String QueryNumTimesGlobalIndexLookup13 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[14]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup13 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q13.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup13,
        QueryNumTimesGlobalIndexLookup13);

    String QueryNumTimesGlobalIndexLookup14 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[15]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup14 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q19.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup14,
        QueryNumTimesGlobalIndexLookup14);

    String QueryNumTimesGlobalIndexLookup15 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[16]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup15 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q25.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup15,
        QueryNumTimesGlobalIndexLookup15);

    String QueryNumTimesGlobalIndexLookup16 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[17]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup16 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q22.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup16,
        QueryNumTimesGlobalIndexLookup16);

    String QueryNumTimesGlobalIndexLookup17 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[18]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup17 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q4.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup17,
        QueryNumTimesGlobalIndexLookup17);

    String QueryNumTimesGlobalIndexLookup18 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[19]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup18 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q5.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup18,
        QueryNumTimesGlobalIndexLookup18);

    String QueryNumTimesGlobalIndexLookup19 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[20]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup19 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q2.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup19,
        QueryNumTimesGlobalIndexLookup19);

    String QueryNumTimesGlobalIndexLookup20 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[21]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup20 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q7.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup20,
        QueryNumTimesGlobalIndexLookup20);

    String QueryNumTimesGlobalIndexLookup21 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[22]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup21 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q8.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup21,
        QueryNumTimesGlobalIndexLookup21);

    String QueryNumTimesGlobalIndexLookup22 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[23]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup22 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q9.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup22,
        QueryNumTimesGlobalIndexLookup22);

    String QueryNumTimesGlobalIndexLookup23 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[24]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup23 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q1.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup23,
        QueryNumTimesGlobalIndexLookup23);

    String QueryNumTimesGlobalIndexLookup24 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[25]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup24 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q3.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup24,
        QueryNumTimesGlobalIndexLookup24);

    scrollbarVerticalDownScroll();

    String QueryNumTimesGlobalIndexLookup25 = driver.findElement(
        By.xpath("//table[@id='queryStatisticsList']/tbody/tr[26]/td[6]"))
        .getText();
    String queryNumTimesGlobalIndexLookup25 = JMXProperties.getInstance()
        .getProperty("aggregatestatement.Q6.numTimesGlobalIndexLookup");
    Assert.assertEquals(queryNumTimesGlobalIndexLookup25,
        QueryNumTimesGlobalIndexLookup25);
  }
  
  @Ignore("WIP")
  @Test
  public void testNumberOfRegions() throws InterruptedException{
	  
		driver.findElement(By.xpath("//a[text()='Data Browser']")).click();
		
		 Thread.sleep(1000);
		 List<WebElement> regionList = driver.findElements(By.xpath("//ul[@id='treeDemo']/li"));		 
		 String regions = JMXProperties.getInstance().getProperty("regions");
		 String []regionName = regions.split(" ");
		 for (String string : regionName) {
			System.out.println("Region name: " + string);
		}
		 //JMXProperties.getInstance().getProperty("region.R1.regionType");
		int i=1; 
		for (WebElement webElement : regionList) {
			//webElement.getAttribute(arg0)
			System.out.println(webElement.findElement(By.id("treeDemo_" + i + "_span")).getText());			
			i++;
		}
		
		driver.findElement(By.id("treeDemo_1_check")).click();		
		
		List<WebElement> memeberList = driver.findElements(By.xpath("//ul[@id='membersList']/li"));
		System.out.println("Memeber List: "+memeberList.size());
		int j=0;
		for (WebElement webElement : memeberList) {
			System.out.println(webElement.findElement(By.id("Member"+ j)).getAttribute("value"));
			j++;			
		}  
  }
  
  
  
  
  
  
  
  
  @Ignore("WIP")
  @Test
  public void testDataBrowser(){
	  
	  driver.findElement(By.linkText("Data Browser")).click();
	 // WebElement dataBrowserLabel = driver.findElement(By.xpath(""));
	  WebDriverWait wait = new WebDriverWait(driver, 20);
	  wait.until(ExpectedConditions.visibilityOf(driver.findElement(By.xpath("//label[text()='Data Browser']"))));
	  
	
	// Verify all elements must be displayed on data browser screen 
	  Assert.assertTrue(driver.findElement(By.xpath("//a[text()='Data Regions']")).isDisplayed());	
	  Assert.assertTrue(driver.findElement(By.id("linkColocatedRegions")).isDisplayed());	  
	  Assert.assertTrue(driver.findElement(By.linkText("All Regions")).isDisplayed());
	  
	  Assert.assertTrue(driver.findElement(By.xpath("//a[text()='Region Members']")).isDisplayed());
	  
	  Assert.assertTrue(driver.findElement(By.xpath("//a[text()='Queries']")).isDisplayed());
	  Assert.assertTrue(driver.findElement(By.xpath("//label[text()='Query Editor']")).isDisplayed());
	  Assert.assertTrue(driver.findElement(By.xpath("//label[text()='Result']")).isDisplayed());
	  Assert.assertTrue(driver.findElement(By.xpath("//input[@value='Export Result']")).isDisplayed());
	  Assert.assertTrue(driver.findElement(By.id("btnExecuteQuery")).isDisplayed());
	  Assert.assertTrue(driver.findElement(By.xpath("//input[@value='Clear']")).isDisplayed());
	  Assert.assertTrue(driver.findElement(By.id("dataBrowserQueryText")).isDisplayed());
	  
	  Assert.assertTrue(driver.findElement(By.id("historyIcon")).isDisplayed());
	  
	  //Acutal query execution
	  
	  driver.findElement(By.id("dataBrowserQueryText")).sendKeys("Query1");
	  
	  
	  
	 
	  // Assert data regions are displayed 
	  Assert.assertTrue(driver.findElement(By.id("treeDemo_1")).isDisplayed());
	
	  
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
	  //Thread.sleep(200000000);
    // driver.close();
    /*try {
      if (tomcat != null) {
        tomcat.stop();
        tomcat.destroy();
      }

      System.out.println("Tomcat Stopped");

      if (server != null) {
        server.stop();
      }

      System.out.println("Server Stopped");
    } catch (LifecycleException e) {
      e.printStackTrace();
    }
*/
  }

}
