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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Vector;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * This tests the rolling upgrade for locators with
 * different GOSSIPVERSION.
 *
 * @author shobhit
 *
 */
@Category(DistributedTest.class)
@Ignore("Test was disabled by renaming to DisabledTest")
public class TcpServerBackwardCompatDUnitTest extends DistributedTestCase {

  /**
   * @param name
   */
  public TcpServerBackwardCompatDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    invokeInEveryVM(new CacheSerializableRunnable("Set TcpServer.isTesting true") {
      
      @Override
      public void run2() throws CacheException {
        TcpServer.isTesting = true;
      }
    });
  }

  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new CacheSerializableRunnable("Set TcpServer.isTesting true") {
      
      @Override
      public void run2() throws CacheException {
        TcpServer.isTesting = false;
      }
    });
    super.tearDown2();
  }

  /**
   * This test starts two locators with current GOSSIPVERSION
   * and then shuts down one of them and restart it with new
   * GOSSIPVERSION and verifies that it has recoverd the system
   * View. Then we upgrade next locator.
   */
  public void testGossipVersionBackwardCompatibility() {
    Host host = Host.getHost(0);
    final VM locator0 = host.getVM(0);
    final VM locator1 = host.getVM(1);
    final VM locatorRestart0 = host.getVM(2);
    final VM member = host.getVM(3);

    // Create properties for locator0
    final int port0 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final File logFile0 = new File(getUniqueName() + "-locator" + port0 + ".log");
    
    // Create properties for locator1
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    while (port == port0) {
      port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    }
    final int port1 = port;

    final File logFile1 = new File(getUniqueName() + "-locator" + port1 + ".log");
    
    final String locators = host.getHostName() + "[" + port0 + "]," +
                            host.getHostName() + "[" + port1 + "]";
    
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    // Start locator0 with props.
    //props.setProperty(DistributionConfig.START_LOCATOR_NAME, host.getHostName() + "["+port0+"]");
    locator0.invoke(new CacheSerializableRunnable("Starting first locator on port " + port0) {
      
      @Override
      public void run2() throws CacheException {
        try {
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION-100, Version.CURRENT_ORDINAL);
          
          Locator.startLocatorAndDS(port0, logFile0, props);
        } catch (IOException e) {
          fail("Locator1 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
        }
      }
    });

    // Start a new member to add it to discovery set of locator0.
    member.invoke(new CacheSerializableRunnable("Start a member") {
      
      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
        TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION-100, Version.CURRENT_ORDINAL);
        InternalDistributedSystem.connect(props);
      }
    });

    // Start locator1 with props.
    //props.setProperty(DistributionConfig.START_LOCATOR_NAME, host.getHostName() + "["+port1+"]");
    locator1.invoke(new CacheSerializableRunnable("Starting second locator on port " + port1) {

      @Override
      public void run2() throws CacheException {
        try {
          TcpServer.TESTVERSION -= 100;
          TcpServer.OLDTESTVERSION -= 100;
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION, Version.CURRENT_ORDINAL);
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.OLDTESTVERSION, Version.GFE_57.ordinal());
          assertEquals("Gossip Version and Test version are not same", TcpServer.GOSSIPVERSION, TcpServer.TESTVERSION);
          assertEquals("Previous Gossip Version and Test version are not same", TcpServer.OLDGOSSIPVERSION, TcpServer.OLDTESTVERSION);

          Locator.startLocatorAndDS(port1, logFile1, props);

          // Start a gossip client to connect to first locator "locator0".
          fail("this test must be fixed to work with the jgroups replacement");
          // TODO
//          final GossipClient client = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port1),  500);
//          client.register("mygroup1", new IpAddress(InetAddress.getLocalHost(), port1), 5000, false);

          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              try {
                // TODO
//                Vector members = client.getMembers("mygroup1", 
//                    new IpAddress(InetAddress.getLocalHost(), port0), true, 5000);
//                return members.size() == 2;
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
          fail("this test must be fixed to work with the jgroups replacement");
          // TODO
//          Vector members = client.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), port0), true, 5000);
//          Assert.assertEquals(2, members.size());
//          Assert.assertTrue(members.contains(new IpAddress(InetAddress.getLocalHost(), port0)));
//          Assert.assertTrue(members.contains(new IpAddress(InetAddress.getLocalHost(), port1)));

        } catch (IOException e) {
          fail("Locator1 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
        }
      }
    });

    // Stop first locator currently running in locator0 VM.
    locator0.invoke(new CacheSerializableRunnable("Stopping first locator") {
      
      @Override
      public void run2() throws CacheException {
        Locator.getLocator().stop();
        disconnectFromDS();
      }
    });
    
    // Restart first locator in new VM.
    //props.setProperty(DistributionConfig.START_LOCATOR_NAME, host.getHostName() + "["+port0+"]");
    locatorRestart0.invoke(new CacheSerializableRunnable("Restarting first locator on port " + port0) {
      
      @Override
      public void run2() throws CacheException {
        try {
          TcpServer.TESTVERSION -= 100;
          TcpServer.OLDTESTVERSION -= 100;
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.TESTVERSION, Version.CURRENT_ORDINAL);
          TcpServer.getGossipVersionMapForTestOnly().put(TcpServer.OLDTESTVERSION, Version.GFE_57.ordinal());
          assertEquals("Gossip Version and Test version are not same", TcpServer.GOSSIPVERSION, TcpServer.TESTVERSION);
          assertEquals("Previous Gossip Version and Test version are not same", TcpServer.OLDGOSSIPVERSION, TcpServer.OLDTESTVERSION);

          Locator.startLocatorAndDS(port0, logFile0, props);

          // A new gossip client with new GOSSIPVERSION must be able
          // to connect with new locator on port1, remote locator.
          // Reuse locator0 VM.
          fail("this test must be fixed to work with the jgroups replacement");
          // TODO
//          final GossipClient client2 = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port1),  500);
//          Vector<IpAddress> members = client2.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), port1), true, 5000);
//          Assert.assertEquals(2, members.size());
          // As they are coming from other locator, their pid is of other locator process.
//          getLogWriter().info(members.get(0) + " " + members.get(1));

          // TODO
//          for (IpAddress ipAddr : members) {
//            int port = ipAddr.getPort();
//            String hostname = ipAddr.getIpAddress().getHostAddress();
//            int pid = ipAddr.getProcessId();
//            Assert.assertTrue(" " + ipAddr, port == port0 || port == port1);
//            Assert.assertTrue(" " + ipAddr, hostname.equals(InetAddress.getLocalHost().getHostAddress()));
//            Assert.assertTrue(" " + ipAddr, pid == locator1.getPid());
//          }

        } catch (IOException e) {
          fail("Locator0 start failed with Gossip Version: " + TcpServer.GOSSIPVERSION + "!", e);
        }
      }
    });
  }
}
