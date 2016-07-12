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
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.MembershipManagerHelper;
import com.gemstone.gemfire.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.test.dunit.*;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

/**
 * Tests the functionality of the {@link DistributedSystem} class.
 *
 * @see InternalDistributedSystemJUnitTest
 *
 */
@Category(DistributedTest.class)
public class DistributedSystemDUnitTest extends JUnit4DistributedTestCase {

  public void postSetUp() throws Exception {
    disconnectAllFromDS();
  }
  
  /**
   * ensure that waitForMemberDeparture correctly flushes the serial message queue for
   * the given member
   */
  @Test
  public void testWaitForDeparture() throws Exception {
    disconnectAllFromDS();
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Properties p = getDistributedSystemProperties();
    p.put(LOCATORS, "");
    p.put(START_LOCATOR, "localhost[" + locatorPort + "]");
    p.put(DISABLE_TCP, "true");
    InternalDistributedSystem ds = (InternalDistributedSystem)DistributedSystem.connect(p);
    try {
      // construct a member ID that will represent a departed member
      InternalDistributedMember mbr = new InternalDistributedMember("localhost", 12345, "", "",
          DistributionManager.NORMAL_DM_TYPE, null, null);
      final DistributionManager mgr = (DistributionManager)ds.getDistributionManager();
      // schedule a message in order to create a queue for the fake member
      final FakeMessage msg = new FakeMessage(null);
      mgr.getExecutor(DistributionManager.SERIAL_EXECUTOR, mbr).execute(new SizeableRunnable(100) {
        public void run() {
          msg.doAction(mgr, false);
        }
        public String toString() {
          return "Processing fake message";
        }
      });
      try {
        assertTrue("expected the serial queue to be flushed", mgr.getMembershipManager().waitForDeparture(mbr));
      } catch (InterruptedException e) {
        fail("interrupted");
      } catch (TimeoutException e) {
        fail("timed out - increase this test's member-timeout setting");
      }
    } finally {
      ds.disconnect();
    }
  }

  private static class FakeMessage extends SerialDistributionMessage {
    volatile boolean[] blocked;
    volatile boolean processed;
    
    FakeMessage(boolean[] blocked) {
      this.blocked = blocked;
    }
    public void doAction(DistributionManager dm, boolean block) {
      processed = true;
      if (block) {
        synchronized(blocked) {
          blocked[0] = true;
          blocked.notify();
          try {
            blocked.wait(60000);
          } catch (InterruptedException e) {}
        }
      }
    }
    public int getDSFID() {
      return 0; // never serialized
    }
    protected void process(DistributionManager dm) {
      // this is never called
    }
    public String toString() {
      return "FakeMessage(blocking="+(blocked!=null)+")";
    }
  }
  
  /**
   * Tests that we can get a DistributedSystem with the same
   * configuration twice.
   */
  @Test
  public void testGetSameSystemTwice() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    // set a flow-control property for the test (bug 37562)
    config.setProperty(MCAST_FLOW_CONTROL, "3000000,0.20,3000");
    
    DistributedSystem system1 = DistributedSystem.connect(config);
    DistributedSystem system2 = DistributedSystem.connect(config);
    assertSame(system1, system2);
    system1.disconnect();
  }

  /**
   * Tests that getting a <code>DistributedSystem</code> with a
   * different configuration after one has already been obtained
   * throws an exception.
   */
  @Test
  public void testGetDifferentSystem() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_FLOW_CONTROL, "3000000,0.20,3000");


    DistributedSystem system1 = DistributedSystem.connect(config);
    config.setProperty(MCAST_ADDRESS, "224.0.0.1");
    try {
      DistributedSystem.connect(config);
      if (System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "mcast-address") == null) {
        fail("Should have thrown an IllegalStateException");
      }
    }
    catch (IllegalStateException ex) {
      // pass...
    }
    finally {
      system1.disconnect();
    }
  }

  /**
   * Tests getting a system with a different configuration after
   * another system has been closed.
   */
  @Test
  public void testGetDifferentSystemAfterClose() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    DistributedSystem system1 = DistributedSystem.connect(config);
    system1.disconnect();
    int time = DistributionConfig.DEFAULT_ACK_WAIT_THRESHOLD + 17;
    config.put(ACK_WAIT_THRESHOLD,
               String.valueOf(time));
    DistributedSystem system2 = DistributedSystem.connect(config);
    system2.disconnect();
  }


  @Test
  public void testGetProperties() {
    Properties config = new Properties();
    // a loner is all this test needs
    int unusedPort = 0;
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    DistributedSystem system1 = DistributedSystem.connect(config);
    
    assertTrue(config != system1.getProperties());
    assertEquals(unusedPort, Integer.parseInt(system1.getProperties().getProperty(MCAST_PORT)));
    
    system1.disconnect();
    
    assertTrue(config != system1.getProperties());
    assertEquals(unusedPort, Integer.parseInt(system1.getProperties().getProperty(MCAST_PORT)));
  }


  @Test
  public void testIsolatedDistributedSystem() throws Exception {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    getSystem(config);
    try {
      // make sure isolated distributed system can still create a cache and region
      Cache cache = CacheFactory.create(getSystem());
      Region r = cache.createRegion(getUniqueName(), new AttributesFactory().create());
      r.put("test", "value");
      assertEquals("value", r.get("test"));
    } finally {
      getSystem().disconnect();
    }
  }


  /** test the ability to set the port used to listen for tcp/ip connections */
  @Test
  public void testSpecificTcpPort() throws Exception {
    Properties config = new Properties();
    int tcpPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    config.put(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    config.setProperty(TCP_PORT, String.valueOf(tcpPort));
    InternalDistributedSystem system = getSystem(config);
    DistributionManager dm = (DistributionManager)system.getDistributionManager();
    GMSMembershipManager mgr = (GMSMembershipManager)dm.getMembershipManager();
    int actualPort = mgr.getDirectChannelPort();
    system.disconnect();
    assertEquals(tcpPort, actualPort);
  }

  /** test that loopback cannot be used as a bind address when a locator w/o a bind address is being used */
  @Test
  public void testLoopbackNotAllowed() throws Exception {
	  // DISABLED for bug #49926
    InetAddress loopback = null;
    for (Enumeration<NetworkInterface> it = NetworkInterface.getNetworkInterfaces(); it.hasMoreElements(); ) {
      NetworkInterface nif = it.nextElement();
      for (Enumeration<InetAddress> ait = nif.getInetAddresses(); ait.hasMoreElements(); ) {
        InetAddress a = ait.nextElement();
        Class theClass = SocketCreator.getLocalHost() instanceof Inet4Address? Inet4Address.class : Inet6Address.class;
        if (a.isLoopbackAddress() && (a.getClass().isAssignableFrom(theClass))) {
          loopback = a;
          break;
        }
      }
    }
    if (loopback != null) {
      Properties config = new Properties();
      config.put(MCAST_PORT, "0");
      String locators = InetAddress.getLocalHost().getHostName()+"["+DistributedTestUtils.getDUnitLocatorPort()+"]";
      config.put(LOCATORS, locators);
      config.setProperty(BIND_ADDRESS, loopback.getHostAddress());
      LogWriterUtils.getLogWriter().info("attempting to connect with " + loopback +" and locators=" + locators);
      try {
        InternalDistributedSystem system = getSystem(config);
        system.disconnect();
        fail("expected a configuration exception disallowing use of loopback address");
      } catch (GemFireConfigException e) {
        // expected
      } catch (DistributionException e) {
        // expected
      }
    }
  }

  @Test
  public void testUDPPortRange() throws Exception {
    Properties config = new Properties();
    int unicastPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    config.put(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    // Minimum 3 ports required in range for UDP, FD_SOCK and TcpConduit.
    config.setProperty(MEMBERSHIP_PORT_RANGE,
        ""+unicastPort+"-"+(unicastPort+2));
    InternalDistributedSystem system = getSystem(config);
    DistributionManager dm = (DistributionManager)system.getDistributionManager();
    InternalDistributedMember idm = dm.getDistributionManagerId();
    system.disconnect();
    assertTrue(unicastPort <= idm.getPort() && idm.getPort() <= unicastPort+2);
    assertTrue(unicastPort <= idm.getPort() && idm.getDirectChannelPort() <= unicastPort+2);
  }
  
  /***
   * this will return starting port, from it "range" of port will available
   * @param range
   * @return
   */
  private int getPortRange(int range) {
    int port = DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0] + 10000;
    int startPort = port;
    int found = 0;
    while (port <= DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1]) {
      if (AvailablePort.isPortAvailable(port, AvailablePort.SOCKET)) {
        found++;
        if (found == range) {
          break;
        }
        port++;
      } else {
        port++;
        startPort = port;
        found = 0;
      }
    }
    if (port > DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1]) {
      fail("Unable to find port range " + range);
    }
    return startPort;
  }

  @Test
  public void testMembershipPortRangeWithExactThreeValues() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    int portRange = 3;
    int portStartRange = getPortRange(portRange);
    int portEndRange = portStartRange + portRange - 1;
    config.setProperty(MEMBERSHIP_PORT_RANGE, "" + (portStartRange) + "-" + (portEndRange));
    InternalDistributedSystem system = getSystem(config);
    Cache cache = CacheFactory.create(system);
    cache.addCacheServer();
    DistributionManager dm = (DistributionManager) system.getDistributionManager();
    InternalDistributedMember idm = dm.getDistributionManagerId();
    GMSMembershipManager manager = (GMSMembershipManager)MembershipManagerHelper.getMembershipManager(system);
    JGroupsMessenger messenger = (JGroupsMessenger)manager.getServices().getMessenger();
    String jgConfig = messenger.getJGroupsStackConfig();
    system.disconnect();
    assertTrue("expected to find port_range=\"2\" in " + jgConfig, jgConfig.contains("port_range=\"2\""));
    assertTrue(idm.getPort() <= portEndRange);
    assertTrue(idm.getPort() >= portStartRange);
    assertTrue(idm.getDirectChannelPort() <= portEndRange);
    assertTrue(idm.getDirectChannelPort() >= portStartRange);
  }

  @Test
  public void testConflictingUDPPort() throws Exception {
    final Properties config = new Properties();
    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.MULTICAST);
    final int[] socketPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int unicastPort = socketPorts[0];
    config.setProperty(MCAST_PORT, String.valueOf(mcastPort));
    config.setProperty(START_LOCATOR, "localhost[" + socketPorts[1] + "]");
    config.setProperty(MEMBERSHIP_PORT_RANGE,
        ""+unicastPort+"-"+(unicastPort+2));
    InternalDistributedSystem system = (InternalDistributedSystem)DistributedSystem.connect(config);
    try {
      DistributionManager dm = (DistributionManager)system.getDistributionManager();
      InternalDistributedMember idm = dm.getDistributionManagerId();
      VM vm = Host.getHost(0).getVM(1);
      vm.invoke(new CacheSerializableRunnable("start conflicting system") {
        public void run2() {
          try {
            String locators = (String) config.remove(START_LOCATOR);
            config.put(LOCATORS, locators);
            DistributedSystem system = DistributedSystem.connect(config);
            system.disconnect();
          } catch (GemFireConfigException e) {
            return; // 
          }
          catch (RMIException e) {
            if (e.getCause() instanceof SystemConnectException) {
              //GEODE-1198: for this test, the membership-port-range has only 3 ports available.
              //If in some rare cases, one of the ports is used by others, it will get this 
              //exception. So just ignore it. Since adding one more port will also fail the test.
              return;
            }
          }
          fail("expected a GemFireConfigException but didn't get one");
        }
      });
    } finally {
      system.disconnect();
    }
  }

  /**
   * Tests that configuring a distributed system with a cache-xml-file
   * of "" does not initialize a cache.  See bug 32254.
   *
   * @since GemFire 4.0
   */
  @Test
  public void testEmptyCacheXmlFile() throws Exception {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(CACHE_XML_FILE, "");

    DistributedSystem sys = DistributedSystem.connect(config);

    try {
      try {
        CacheFactory.getInstance(sys);
        fail("Should have thrown a CancelException");
      } 
      catch (CancelException expected) {
      }
      // now make sure we can create the cache
      CacheFactory.create(sys);

    } finally {
      sys.disconnect();
    }
  }
  

  
}
