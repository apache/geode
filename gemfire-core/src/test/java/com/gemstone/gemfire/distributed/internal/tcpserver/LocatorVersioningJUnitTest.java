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
package com.gemstone.gemfire.distributed.internal.tcpserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.LocatorImpl;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.org.jgroups.stack.GossipClient;
import com.gemstone.org.jgroups.stack.IpAddress;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * @author shobhit
 *
 */
@Category(IntegrationTest.class)
public class LocatorVersioningJUnitTest {

  
  @Test
  public void testLocatorStateFileBackwardCompatibility() throws IOException, InterruptedException {
  
    Locator locator = null;
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File tmpFile = new File("locator-" + port + ".log");

    int currFileVersion = LocatorImpl.FILE_FORMAT_VERSION;
    Map fileVersionMap = LocatorImpl.getFileVersionMapForTestOnly();
    assertEquals(2, fileVersionMap.size());

    // Create old FILE_FORMAT_VERSION and map it with old Version.ordinal.
    int newGossipVersion = currFileVersion-2;
    try {
      LocatorImpl.FILE_FORMAT_VERSION = newGossipVersion;
      fileVersionMap.put(newGossipVersion, Integer.valueOf(Version.GFE_71.ordinal()));
      TcpServer.isTesting = true;
  
      final Properties props = new Properties();
      props.setProperty("mcast-port", "0");
//      props.setProperty("log-level", "fine");
      props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
      locator = Locator.startLocatorAndDS(port, tmpFile, props);
      Assert.assertEquals(locator, Locator.getLocators().iterator().next());
      Thread.sleep(1000);
      
      final GossipClient client = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port),  500);
      client.register("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), 5000, false);
  
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          try {
            Vector members = client.getMembers("mygroup1", 
                new IpAddress(InetAddress.getLocalHost(), 55), true, 5000);
            System.out.println("received response of " + members + " ("+members.size()+" entries)");
            return members.size() == 1;
          }
          catch (Exception e) {
            e.printStackTrace();
            fail("unexpected exception");
          }
          return false; // NOTREACHED
        }
        public String description() {
          return null;
        }
      };
      
      DistributedTestCase.waitForCriterion(ev, 1000, 200, true);
      Vector members = client.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
      Assert.assertEquals(1, members.size());
      Assert.assertEquals(new IpAddress(InetAddress.getLocalHost(), 55), members.get(0));
  
  
      // Stop and restart locator
      locator.stop();
      locator = Locator.startLocatorAndDS(port, tmpFile, props);
      
      // Now restart should recover fine from old version state file.
      final GossipClient client2 = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port),  500);
      members = client2.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
      Assert.assertEquals(1, members.size());
      Assert.assertEquals(new IpAddress(InetAddress.getLocalHost(), 55), members.get(0));
    } finally {
      fileVersionMap.remove(newGossipVersion);
      LocatorImpl.FILE_FORMAT_VERSION = currFileVersion;
      TcpServer.isTesting = false;
      if (locator != null) {
        locator.stop();
      }
    }
  }

  @Test
  public void testLocatorStateFileBackwardCompatibilityWithGF701() throws IOException, InterruptedException {

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File tmpFile = new File("locator-" + port + ".log");
    
    int currFileVersion = LocatorImpl.FILE_FORMAT_VERSION;
    Map fileVersionMap = LocatorImpl.getFileVersionMapForTestOnly();
    assertEquals(2, fileVersionMap.size());
    
    // Create old FILE_FORMAT_VERSION and map it with old Version.ordinal.
    LocatorImpl.FILE_FORMAT_VERSION = currFileVersion-1;
    TcpServer.isTesting = true;

    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
//    props.setProperty("log-level", "fine");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    Locator loc = Locator.getLocator().startLocatorAndDS(port, tmpFile, props);
    Assert.assertEquals(loc, Locator.getLocators().iterator().next());
    Thread.sleep(1000);
    
    final GossipClient client = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port),  500);
    client.register("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), 5000, false);

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          Vector members = client.getMembers("mygroup1", 
              new IpAddress(InetAddress.getLocalHost(), 55), true, 5000);
          return members.size() == 1;
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("unexpected exception");
        }
        return false; // NOTREACHED
      }
      public String description() {
        return null;
      }
    };
    
    DistributedTestCase.waitForCriterion(ev, 1000, 200, true);
    Vector members = client.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
    Assert.assertEquals(1, members.size());
    Assert.assertEquals(new IpAddress(InetAddress.getLocalHost(), 55), members.get(0));


    // Stop and restart locator
    loc.stop();
    loc = Locator.startLocatorAndDS(port, tmpFile, props);

    // Change LocatorImpl back to latest File format version.
    LocatorImpl.FILE_FORMAT_VERSION = currFileVersion;

    // Now restart should recover fine from old version state file.
    final GossipClient client2 = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port),  500);
    members = client2.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), true, 5000);
    Assert.assertEquals(1, members.size());
    Assert.assertEquals(new IpAddress(InetAddress.getLocalHost(), 55), members.get(0));
    TcpServer.isTesting = false;
    loc.stop();

    // Check if log file has any IOException. As IOException doesn't affect main
    // locator thread.
    FileReader fr = new FileReader(tmpFile);
    BufferedReader br = new BufferedReader(fr);
    
    try {
      
      String line = br.readLine();
      boolean found = false;
      while (line != null) {
        if (line.contains("IOException")) {
          found = true;
        }
        line = br.readLine();
      }
      
      if (found) {
        fail("IOException is thrown in locator, most probably because of not being able to read" +
        		" previoous state from locator state file. Locator log file is: "
            + tmpFile);
      }
    } finally {
      br.close();
      fr.close();
    }
  }
}
