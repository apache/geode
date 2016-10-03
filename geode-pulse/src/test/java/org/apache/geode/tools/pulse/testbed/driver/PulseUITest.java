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
 */
package org.apache.geode.tools.pulse.testbed.driver;

import static org.apache.geode.tools.pulse.tests.PulseAbstractTest.*;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.internal.JettyHelper;
import org.apache.geode.test.junit.categories.UITest;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Locator;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Peer;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Region;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Server;
import org.apache.geode.tools.pulse.testbed.TestBed;

@Ignore("TODO: test is ignored")
@Category(UITest.class)
public class PulseUITest {

  private static WebDriver driver;
  private static TestBed testBed;
  private static String pulseURL;
  private static String path;
  private static org.eclipse.jetty.server.Server jetty = null;

  private static final String userName = "admin";
  private static final String pasword = "admin";
  
  private static final String DATA_VIEW_LABEL = "Data View";
  private static final String CLUSTER_VIEW_MEMBERS_ID = "clusterTotalMembersText";
  private static final String CLUSTER_VIEW_SERVERS_ID = "clusterServersText";
  private static final String CLUSTER_VIEW_LOCATORS_ID = "clusterLocatorsText";
  private static final String CLUSTER_VIEW_REGIONS_ID = "clusterTotalRegionsText";

  @BeforeClass
  public static void setUpJetty() throws Exception {
    String host = InetAddress.getLocalHost().getHostAddress();
    int port = 8080;
    String context = "/pulse";
    path = getPulseWarPath();
    //System.setProperty("pulse.propMockDataUpdaterClass", "org.apache.geode.tools.pulse.testbed.PropMockDataUpdater");

    jetty = JettyHelper.initJetty(host, port, SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.WEB));
    JettyHelper.addWebApplication(jetty, context, getPulseWarPath());
    jetty.start();

    pulseURL = "http://" + host + ":" + port + context;
    Thread.sleep(1000); //wait till tomcat settles down
    driver = new FirefoxDriver();
    driver.manage().window().maximize();//required to make all elements visible

    Thread.sleep(5000); //wait till pulse starts polling threads...
    testBed = new TestBed();
    loginToPulse(driver, userName, pasword);
  }

  @AfterClass
  public static void stopJetty() throws Exception {
    jetty.stop();
  }

  @After
  public void closeSession() {
    driver.close();
  }

  private static void loginToPulse(WebDriver driver, String userName,String password){
    driver.get(pulseURL);    
    WebElement userNameElement = driver.findElement(By.id("user_name"));
    WebElement passwordElement = driver.findElement(By.id("user_password"));
    userNameElement.sendKeys(userName);
    passwordElement.sendKeys(password);
    passwordElement.submit();
    WebElement userNameOnPulsePage = (new WebDriverWait(driver, 10))
        .until(new ExpectedCondition<WebElement>() {
          @Override
          public WebElement apply(WebDriver d) {
            return d.findElement(By.id("userName"));
          }
        });
    assertNotNull(userNameOnPulsePage);
  }
  
  
  private void searchByLinkAndClick(String linkText){
    WebElement  dataViewButton= By.linkText(linkText).findElement(driver);
    assertNotNull(dataViewButton);   
    dataViewButton.click();
  }
  
  private void searchByIdAndClick(String id){
    WebElement  element = driver.findElement(By.id(id));
    assertNotNull(element);
    element.click();    
  }
  
  private void searchByXPathAndClick(String xpath){    
    WebElement  element = driver.findElement(By.xpath(xpath));
    assertNotNull(element);
    element.click();    
  }
  
  private void waitForElementByClassName(final String className, int seconds){
    WebElement linkTextOnPulsePage1 = (new WebDriverWait(driver, seconds))
    .until(new ExpectedCondition<WebElement>() {
      @Override
      public WebElement apply(WebDriver d) {
        return d.findElement(By.className(className));
      }
    });
    assertNotNull(linkTextOnPulsePage1);
  }
  
  private void waitForElementById(final String id, int seconds){
    WebElement element = (new WebDriverWait(driver, 10))
    .until(new ExpectedCondition<WebElement>() {
      @Override
      public WebElement apply(WebDriver d) {
        return d.findElement(By.id(id));
      }
    });
    assertNotNull(element);
  }
  
  @Test
  public void testClusterViewTopRibbon() {
    List<Server> servers = testBed.getRootDs().getServers();
    List<Locator> locators = testBed.getRootDs().getLocators();
    List<Peer> peers = testBed.getRootDs().getPeers();
    List<Region> regions = testBed.getRootDs().getRegions();
    int totalMembers = servers.size() + locators.size() + peers.size();
    int clusterMembers = Integer.parseInt(driver.findElement(
        By.id(CLUSTER_VIEW_MEMBERS_ID)).getText());
    int clusterServers = Integer.parseInt(driver.findElement(
        By.id(CLUSTER_VIEW_SERVERS_ID)).getText());
    int clusterLocators = Integer.parseInt(driver.findElement(
        By.id(CLUSTER_VIEW_LOCATORS_ID)).getText());
    int clusterRegions = Integer.parseInt(driver.findElement(
        By.id(CLUSTER_VIEW_REGIONS_ID)).getText());
    assertEquals(totalMembers, clusterMembers);
    assertEquals(servers.size(), clusterServers);
    assertEquals(locators.size(), clusterLocators);
    assertEquals(regions.size(), clusterRegions);
  }  


  @Test
  public void testDataViewRegionProperties() {
    searchByLinkAndClick(DATA_VIEW_LABEL);
    waitForElementByClassName("pointDetailsPadding",10);    
    searchByIdAndClick("btngridIcon");
    
    for(int i=1;i<testBed.getRootDs().getRegions().size();i++){
      searchByIdAndClick(""+i);
      String regionName1 = driver.findElement(By.id("regionName")).getText();
      @SuppressWarnings("rawtypes")
      List regionMemberscount1 = testBed.getRootDs().getRegion(regionName1)
          .getMembers();
      int regionEntCount1 = testBed.getRootDs().getRegion(regionName1)
          .getEntryCount();
      int regionMembers1 = Integer.parseInt(driver.findElement(
          By.id("regionMembers")).getText());
      int regionEntryCount1 = Integer.parseInt(driver.findElement(
          By.id("regionEntryCount")).getText());
      assertEquals(regionMemberscount1.size(), regionMembers1);
      assertEquals(regionEntCount1, regionEntryCount1);
    }
  }

  
  @Test
  public void testMemberViewRegions() {
    
    searchByLinkAndClick(DATA_VIEW_LABEL);
    waitForElementByClassName("pointDetailsPadding",10);    
    searchByXPathAndClick("//div[@title='peer1']");    
    waitForElementById("memberRegionsCount",10);    
    
    List<Server> servers = testBed.getRootDs().getServers();
    List<Locator> locators = testBed.getRootDs().getLocators();
    List<Peer> peers = testBed.getRootDs().getPeers();    

    String prevSelectedMember = "peer1";
    
    for (Peer p : peers) {
      String peer = p.getName();
      System.out.println("Checking regions mapping for member " + peer);
      WebElement comboBox = driver.findElement(By.linkText(prevSelectedMember));
      comboBox.click();                 
      WebElement comboList = driver.findElement(By.id("clusterMembersContainer"));     
      WebElement selectedMember = comboList.findElement(By.linkText(peer));
      selectedMember.click();
      timeout();
      String peername = driver.findElement(By.id("memberName")).getText();      
      List<Region> peerRegionscount = testBed.getRootDs().getRegions(peer);
      int peerRegions = Integer.parseInt(driver.findElement(
          By.id("memberRegionsCount")).getText());
      assertEquals(peerRegionscount.size(), peerRegions);
      prevSelectedMember = peername;
    }
    
    for (Server s : servers) {
      String server = s.getName();
      System.out.println("Checking regions mapping for server " + server);
      WebElement comboBox = driver.findElement(By.linkText(prevSelectedMember));
      comboBox.click();                 
      WebElement comboList = driver.findElement(By.id("clusterMembersContainer"));     
      WebElement selectedMember = comboList.findElement(By.linkText(server));
      selectedMember.click();
      timeout();
      String peername = driver.findElement(By.id("memberName")).getText();      
      List<Region> serverRegionscount = testBed.getRootDs().getRegions(server);
      int serverRegions = Integer.parseInt(driver.findElement(
          By.id("memberRegionsCount")).getText());
      assertEquals(serverRegionscount.size(), serverRegions);
      prevSelectedMember = peername;            
    }
    /*
    for (Locator l : locators) {      
      String locator = l.getName();
      System.out.println("Checking regions mapping for locator " + locator);
      WebElement comboBox = driver.findElement(By.linkText(prevSelectedMember));
      comboBox.click();                 
      WebElement comboList = driver.findElement(By.id("clusterMembersContainer"));     
      WebElement selectedMember = comboList.findElement(By.linkText(locator));
      selectedMember.click();
      timeout();
      String peername = driver.findElement(By.id("memberName")).getText();      
      List<Region> locatorRegionscount = testBed.getRootDs().getRegions(locator);
      int locatorRegions = Integer.parseInt(driver.findElement(
          By.id("memberRegionsCount")).getText());
      assertIndexDetailsEquals(locatorRegionscount.size(), locatorRegions);
      prevSelectedMember = peername;
    }*/
  }

  public void timeout() {
    WebElement memberNameOnPulsePage = (new WebDriverWait(driver, 10))
        .until(new ExpectedCondition<WebElement>() {
          @Override
          public WebElement apply(WebDriver d) {
            return d.findElement(By.id("memberName"));
          }
        });
    assertNotNull(memberNameOnPulsePage);    
  }  
}
