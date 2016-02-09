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
package com.gemstone.gemfire.internal.cache.wan.misc;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.IncompatibleSystemException;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;

public class WanAutoDiscoveryDUnitTest extends WANTestBase {

  
  public WanAutoDiscoveryDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);

  }
  
  /**
   * Test to validate that sender can not be started without locator started.
   * else GemFireConfigException will be thrown.
   */
  public void test_GatewaySender_Started_Before_Locator() {
    try {
      int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
      vm0.invoke(WANTestBase.class, "createCache", new Object[]{port});      
      vm0.invoke(WANTestBase.class, "createSender", new Object[]{"ln",2,false,100,10,false,false, null, false});
      fail("Expected GemFireConfigException but not thrown");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GemFireConfigException)) {
        Assert.fail("Expected GemFireConfigException but received :", e);
      }
    }
  }
  
  /**
   * Test to validate that all locators in one DS should have same name. Though
   * this test passes, it causes other below tests to fail. In this test, VM1 is
   * throwing IncompatibleSystemException after startInitLocator. I think, after
   * throwing this exception, locator is not stopped properly and hence other
   * tests are failing.
   * 
   * @throws Exception
   */
  public void __test_AllLocatorsinDSShouldHaveDistributedSystemId() throws Exception {
    try {
      Integer lnLocPort1 = (Integer)vm0.invoke(
          WANTestBase.class, "createFirstLocatorWithDSId",
          new Object[] {1});

      Integer lnLocPort2 = (Integer)vm1.invoke(
          WANTestBase.class, "createSecondLocator", new Object[] { 2,
              lnLocPort1 });
      fail("Expected IncompatibleSystemException but not thrown");
    }
    catch (Exception e) {
      if (!(e.getCause()instanceof IncompatibleSystemException)) {
        Assert.fail("Expected IncompatibleSystemException but received :", e);
      }
    }
  }
  
  /**
   * Test to validate that multiple locators added on LN site and multiple
   * locators on Ny site recognizes each other
   * @throws Exception 
   */
  public void test_NY_Recognises_ALL_LN_Locators() throws Exception {
    ArrayList<Integer> locatorPorts = new ArrayList<Integer>();
    Map<Integer, ArrayList<Integer>> dsVsPort = new HashMap<Integer, ArrayList<Integer>>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = (Integer)vm0.invoke(
        WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] {1});
    locatorPorts.add(lnLocPort1);

    Integer lnLocPort2 = (Integer)vm1.invoke(WANTestBase.class,
        "createSecondLocator", new Object[] { 1, lnLocPort1 });
    locatorPorts.add(lnLocPort2);
    
    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnLocPort1 });
    locatorPorts.add(nyLocPort1);
    
    Integer nyLocPort2 = (Integer)vm3.invoke(
        WANTestBase.class, "createSecondRemoteLocator", new Object[] {
            2, nyLocPort1, lnLocPort1});
    locatorPorts.add(nyLocPort2);
    
    vm0.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm3.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
  }
 
  /**
   * Test to validate that TK site's locator is recognized by LN and NY. Test to
   * validate that HK site's locator is recognized by LN , NY, TK.
   */
  public void test_NY_Recognises_TK_AND_HK_Through_LN_Locator() {

    Map<Integer, ArrayList<Integer>> dsVsPort = new HashMap<Integer, ArrayList<Integer>>();

    ArrayList<Integer> locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(1, locatorPorts);
    
    Integer lnLocPort1 = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    locatorPorts.add(lnLocPort1);

    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnLocPort1 });
    locatorPorts.add(nyLocPort1);

    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(3, locatorPorts);
    Integer tkLocPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnLocPort1 });
    locatorPorts.add(tkLocPort);

    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(4, locatorPorts);
    Integer hkLocPort = (Integer)vm3.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 4, lnLocPort1 });
    locatorPorts.add(hkLocPort);

    vm0.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm3.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
  }

  public void test_TK_Recognises_LN_AND_NY() {

    Map<Integer, ArrayList<Integer>> dsVsPort = new HashMap<Integer, ArrayList<Integer>>();

    ArrayList<Integer> locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(1, locatorPorts);
    
    Integer lnLocPort1 = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    locatorPorts.add(lnLocPort1);

    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnLocPort1 });
    locatorPorts.add(nyLocPort1);

    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(3, locatorPorts);
    Integer tkLocPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, nyLocPort1 });
    locatorPorts.add(tkLocPort);


    vm0.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
  }
  
  public void test_NY_Recognises_TK_AND_HK_Simeltenously() {
    Map<Integer, ArrayList<Integer>> dsVsPort = new HashMap<Integer, ArrayList<Integer>>();

    ArrayList<Integer> locatorPortsln = new ArrayList<Integer>();
    dsVsPort.put(1, locatorPortsln);
    Integer lnLocPort1 = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    locatorPortsln.add(lnLocPort1);

    ArrayList<Integer> locatorPortsny = new ArrayList<Integer>();
    dsVsPort.put(2, locatorPortsny);
    Integer nyLocPort1 = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnLocPort1 });
    locatorPortsny.add(nyLocPort1);

    int AsyncInvocationArrSize = 4;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];

    ArrayList<Integer> locatorPortstk = new ArrayList<Integer>();
    dsVsPort.put(3, locatorPortstk);
    async[0] = vm2.invokeAsync(WANTestBase.class, "createFirstRemoteLocator",
        new Object[] { 3, lnLocPort1 });

    ArrayList<Integer> locatorPortshk = new ArrayList<Integer>();
    dsVsPort.put(4, locatorPortshk);
    async[1] = vm3.invokeAsync(
        WANTestBase.class, "createFirstRemoteLocator", new Object[] {4, nyLocPort1});

    ArrayList<Integer> locatorPortsln2 = new ArrayList<Integer>();
    async[2] = vm4.invokeAsync(WANTestBase.class,
        "createSecondLocator", new Object[] { 1, lnLocPort1 });
    
    ArrayList<Integer> locatorPortsny2 = new ArrayList<Integer>();
    async[3] = vm5.invokeAsync(WANTestBase.class,
        "createSecondLocator", new Object[] { 2, nyLocPort1 });

    
    try {
      async[0].join();
      async[1].join();
      async[2].join();
      async[3].join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    locatorPortstk.add((Integer)async[0].getReturnValue());
    locatorPortshk.add((Integer)async[1].getReturnValue());
    locatorPortsln.add((Integer)async[2].getReturnValue());
    locatorPortsny.add((Integer)async[3].getReturnValue());
    
    vm0.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm3.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
  }
  
  
  public void test_LN_Sender_recogises_ALL_NY_Locators() {
    
    Integer lnLocPort1 = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    
    Integer lnLocPort2 = (Integer)vm5.invoke(WANTestBase.class,
        "createSecondLocator", new Object[] { 1, lnLocPort1 });
    
    vm2.invoke(WANTestBase.class, "createCache", new Object[]{lnLocPort1, lnLocPort2});
    
    vm2.invoke(WANTestBase.class, "createSender",
        new Object[] {"ln",2,false,100,10,false,false, null, true});
    
    Integer nyLocPort1 = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnLocPort1 });
    
    vm2.invoke(WANTestBase.class, "startSender",new Object[]{"ln"});

    //Since to fix Bug#46289, we have moved call to initProxy in getConnection which will be called only when batch is getting dispatched.
    //So for locator discovery callback to work, its now expected that atleast try to send a batch so that proxy will be initialized
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
  
     vm2.invoke(WANTestBase.class, "doPuts",
      new Object[] { getTestMethodName() + "_RR", 10});

    Integer nyLocPort2 = (Integer)vm3
        .invoke(WANTestBase.class, "createSecondRemoteLocator", new Object[] {
            2, nyLocPort1, lnLocPort1 });

    InetSocketAddress locatorToWaitFor = new InetSocketAddress("localhost",
        nyLocPort2);

    vm2.invoke(WANTestBase.class, "checkLocatorsinSender",
        new Object[] {"ln", locatorToWaitFor });

    Integer nyLocPort3 = (Integer)vm4
        .invoke(WANTestBase.class, "createSecondRemoteLocator", new Object[] {
            2, nyLocPort1, lnLocPort1 });
    
    locatorToWaitFor = new InetSocketAddress("localhost", nyLocPort3);

    vm2.invoke(WANTestBase.class, "checkLocatorsinSender",
        new Object[] {"ln", locatorToWaitFor });

  }
  
  
  public void test_RingTopology() {

    final Set<String> site1LocatorsPort = new HashSet<String>();
    int site1Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site1LocatorsPort.add("localhost["+site1Port1+"]");
   
    final Set<String> site2LocatorsPort = new HashSet<String>();
    int site2Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site2LocatorsPort.add("localhost["+site2Port1+"]");
   
    final Set<String> site3LocatorsPort = new HashSet<String>();
    int site3Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site3LocatorsPort.add("localhost["+site3Port1+"]");
   
    final Set<String> site4LocatorsPort = new HashSet<String>();
    int site4Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site4LocatorsPort.add("localhost["+site4Port1+"]");
   
    Map<Integer, Set<String>> dsVsPort = new HashMap<Integer, Set<String>>();
    dsVsPort.put(1, site1LocatorsPort);
    dsVsPort.put(2, site2LocatorsPort);
    dsVsPort.put(3, site3LocatorsPort);
    dsVsPort.put(4, site4LocatorsPort);
   
    int AsyncInvocationArrSize = 9;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
   
    async[0] = vm0.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 1, site1Port1, site1LocatorsPort, site2LocatorsPort});
   
    async[1] = vm1.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 2, site2Port1, site2LocatorsPort, site3LocatorsPort});
   
    async[2] = vm2.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 3, site3Port1, site3LocatorsPort, site4LocatorsPort});
   
    async[3] = vm3.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 4, site4Port1, site4LocatorsPort, site1LocatorsPort});
   
   // pause(5000);
    try {
      async[0].join();
      async[1].join();
      async[2].join();
      async[3].join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Could not join async operations");
    }
   
    vm0.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm3.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
  }
  
  public void ___test_3Sites3Locators() {
    final Set<String> site1LocatorsPort = new HashSet<String>();
    int site1Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site1LocatorsPort.add("localhost["+site1Port1+"]");
    int site1Port2 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site1LocatorsPort.add("localhost["+site1Port2+"]");
    int site1Port3 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site1LocatorsPort.add("localhost["+site1Port3+"]");
    
    final Set<String> site2LocatorsPort = new HashSet<String>();
    int site2Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site2LocatorsPort.add("localhost["+site2Port1+"]");
    int site2Port2 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site2LocatorsPort.add("localhost["+site2Port2+"]");
    int site2Port3 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site2LocatorsPort.add("localhost["+site2Port3+"]");
    
    final Set<String> site3LocatorsPort = new HashSet<String>();
    int site3Port1 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site3LocatorsPort.add("localhost["+site3Port1+"]");
    final int site3Port2 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site3LocatorsPort.add("localhost["+site3Port2+"]");
    int site3Port3 = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    site3LocatorsPort.add("localhost["+site3Port3+"]");

    Map<Integer, Set<String>> dsVsPort = new HashMap<Integer, Set<String>>();
    dsVsPort.put(1, site1LocatorsPort);
    dsVsPort.put(2, site2LocatorsPort);
    dsVsPort.put(3, site3LocatorsPort);
    
    int AsyncInvocationArrSize = 9;
    AsyncInvocation[] async = new AsyncInvocation[AsyncInvocationArrSize];
    
    async[0] = vm0.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 1, site1Port1, site1LocatorsPort, site2LocatorsPort});
    
    async[8] = vm0.invokeAsync(WANTestBase.class,
        "checkAllSiteMetaDataFor3Sites", new Object[] {dsVsPort});
    
    async[1] = vm1.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 1, site1Port2, site1LocatorsPort, site2LocatorsPort});
    async[2] = vm2.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 1, site1Port3, site1LocatorsPort, site2LocatorsPort});
    
    async[3] = vm3.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 2, site2Port1, site2LocatorsPort, site3LocatorsPort});
    async[4] = vm4.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 2, site2Port2, site2LocatorsPort, site3LocatorsPort});
    async[5] = vm5.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 2, site2Port3, site2LocatorsPort, site3LocatorsPort});
    
    async[6] = vm6.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 3, site3Port1, site3LocatorsPort, site1LocatorsPort});
    async[7] = vm7.invokeAsync(WANTestBase.class,
        "createLocator", new Object[] { 3, site3Port2, site3LocatorsPort, site1LocatorsPort});
    
    WANTestBase.createLocator(3, site3Port3, site3LocatorsPort, site1LocatorsPort);
    long startTime = System.currentTimeMillis();
    
    try {
      async[0].join();
      async[1].join();
      async[2].join();
      async[3].join();
      async[4].join();
      async[5].join();
      async[6].join();
      async[7].join();
      async[8].join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Could not join async operations");
    }
    Long endTime = null;
    try {
      endTime = (Long)async[8].getResult();
    }
    catch (Throwable e) {
      e.printStackTrace();
     Assert.fail("Could not get end time", e);
    }
    
    LogWriterUtils.getLogWriter().info("Time taken for all 9 locators discovery in 3 sites: " + (endTime.longValue() - startTime));
    
    vm0.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm3.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm4.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm5.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm6.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    vm7.invoke(WANTestBase.class, "checkAllSiteMetaDataFor3Sites",
        new Object[] { dsVsPort });
    WANTestBase.checkAllSiteMetaDataFor3Sites(dsVsPort);
  }
  
  
  public void test_LN_Peer_Locators_Exchange_Information() {
    ArrayList<Integer> locatorPorts = new ArrayList<Integer>();
    Map<Integer, ArrayList<Integer>> dsVsPort = new HashMap<Integer, ArrayList<Integer>>();
    dsVsPort.put(1, locatorPorts);

    Integer lnLocPort1 = (Integer)vm0.invoke(
        WANTestBase.class, "createFirstPeerLocator",
        new Object[] {1});
    locatorPorts.add(lnLocPort1);

    Integer lnLocPort2 = (Integer)vm1.invoke(WANTestBase.class,
        "createSecondPeerLocator", new Object[] { 1, lnLocPort1 });
    locatorPorts.add(lnLocPort2);
    
    vm0.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
  }
  
  public void test_LN_NY_TK_5_PeerLocators_1_ServerLocator() {
    Map<Integer, ArrayList<Integer>> dsVsPort = new HashMap<Integer, ArrayList<Integer>>();
    
    
    ArrayList<Integer> locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(1, locatorPorts);
    Integer lnLocPort1 = (Integer)vm0.invoke(
        WANTestBase.class, "createFirstPeerLocator",
        new Object[] {1});
    locatorPorts.add(lnLocPort1);
    Integer lnLocPort2 = (Integer)vm1.invoke(WANTestBase.class,
        "createSecondPeerLocator", new Object[] { 1, lnLocPort1 });
    locatorPorts.add(lnLocPort2);
    
    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(2, locatorPorts);
    Integer nyLocPort1 = (Integer)vm2.invoke(
        WANTestBase.class, "createFirstRemotePeerLocator",
        new Object[] {2, lnLocPort1});
    locatorPorts.add(nyLocPort1);
    Integer nyLocPort2 = (Integer)vm3.invoke(WANTestBase.class,
        "createSecondRemotePeerLocator", new Object[] { 2, nyLocPort1, lnLocPort2});
    locatorPorts.add(nyLocPort2);
    
    locatorPorts = new ArrayList<Integer>();
    dsVsPort.put(3, locatorPorts);
    Integer tkLocPort1 = (Integer)vm4.invoke(
        WANTestBase.class, "createFirstRemotePeerLocator",
        new Object[] {3, nyLocPort1});
    locatorPorts.add(tkLocPort1);
    Integer tkLocPort2 = (Integer)vm5.invoke(WANTestBase.class,
        "createSecondRemotePeerLocator", new Object[] { 3, tkLocPort1, nyLocPort1});
    locatorPorts.add(tkLocPort2);
    Integer tkLocPort3 = (Integer)vm6.invoke(WANTestBase.class,
        "createSecondRemoteLocator", new Object[] { 3, tkLocPort1, nyLocPort2});
    locatorPorts.add(tkLocPort3);
    
   // pause(5000);
    
    vm0.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm1.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm2.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm3.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm4.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm5.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
    vm6.invoke(WANTestBase.class, "checkAllSiteMetaData",
        new Object[] { dsVsPort });
        
  }
  
}
