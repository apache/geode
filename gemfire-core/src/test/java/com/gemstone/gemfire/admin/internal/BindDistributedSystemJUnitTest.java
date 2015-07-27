/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl}.
 *
 * @author    Kirk Lund
 * @created   August 30, 2004
 * @since     3.5
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class BindDistributedSystemJUnitTest {

  private final static int RETRY_ATTEMPTS = 3;
  private final static int RETRY_SLEEP = 100;

  private DistributedSystem system;

  @After
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }
    this.system = null;
  }
  
//  public void testBindToAddressNull() throws Exception {
//    DistributedSystemFactory.bindToAddress(null);
//     todo...
//  }
//
//  public void testBindToAddressEmpty() throws Exception {
//    DistributedSystemFactory.bindToAddress("");
//     todo...
//  }

  @Test
  public void testBindToAddressLoopback() throws Exception {
    String bindTo = "127.0.0.1";
    // make sure bindTo is the loopback... needs to be later in test...
    assertEquals(true, InetAddressUtil.isLoopback(bindTo));

    Properties props = new Properties();
    props.setProperty(DistributionConfig.BIND_ADDRESS_NAME, bindTo);
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,
        String.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)));
    this.system = com.gemstone.gemfire.distributed.DistributedSystem.connect(
        props);
        
    assertEquals(true, this.system.isConnected());

    // analyze the system's config...
    DistributionConfig distConfig =
        ((InternalDistributedSystem) this.system).getConfig();

    // create a loner distributed system and make sure it is connected...
    String locators = distConfig.getLocators();
    String mcastAddress = InetAddressUtil.toString(distConfig.getMcastAddress());
    int mcastPort = distConfig.getMcastPort();

    // Because of fix for bug 31409
    this.system.disconnect();

    checkAdminAPI(bindTo, locators, mcastAddress, mcastPort);
  }

  private void checkAdminAPI(String bindTo,
      String locators,
      String mcastAddress,
      int mcastPort) throws Exception {

    // build the config that defines the system...
    com.gemstone.gemfire.admin.DistributedSystemConfig config =
      AdminDistributedSystemFactory.defineDistributedSystem();
    config.setBindAddress(bindTo);
    config.setLocators(locators);
    config.setMcastAddress(mcastAddress);
    config.setMcastPort(mcastPort);
    config.setRemoteCommand(com.gemstone.gemfire.admin.DistributedSystemConfig.DEFAULT_REMOTE_COMMAND);
    assertNotNull(config);

    AdminDistributedSystem distSys =
        AdminDistributedSystemFactory.getDistributedSystem(config);
    assertNotNull(distSys);

    // connect to the system...
    distSys.connect();
    try {
      checkSystemIsRunning(distSys);

//      DistributionManager dm =
//          ((AdminDistributedSystemImpl) distSys).getDistributionManager();
//      checkDistributionPorts(dm, bindTo);

    }
    finally {
      distSys.disconnect();
    }
  }

  private void checkSystemIsRunning(final AdminDistributedSystem distSys) throws Exception {
//    WaitCriterion ev = new WaitCriterion() {
//      public boolean done() {
//        return distSys.isConnected() || distSys.isRunning();
//      }
//      public String description() {
//        return "distributed system is either connected or running: " + distSys;
//      }
//    };
//    DistributedTestCase.waitForCriterion(ev, 120 * 1000, 200, true);
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 120 * 1000; done = (distSys.isConnected() || distSys.isRunning())) {
        Thread.sleep(200);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("waiting for distributed system is either connected or running: " + distSys, done);
    assertTrue(distSys.isConnected());
  }

//  protected void checkDistributionPorts(DistributionManager dm,
//      String bindToAddress) throws Exception {
//    assertNotNull(dm);
//    DistributionChannel distChannel = dm.getDistributionChannel();
//
//    // now query the Channel...
//    DirectChannel dc = distChannel.getDirectChannel();
////    MembershipManager jc = distChannel.getMembershipManager();
//
//    // get Jgroups IpAddress and retrieve its ports...
//    DistributedMember ipAddress = dc.getLocalAddress();
//    InetAddress jgAddr = ipAddress.getIpAddress();
////    int mcastPort = ipAddress.getPort();
//    int directPort = ipAddress.getDirectChannelPort();
//    int[] portsToCheck = new int[]{directPort};
//
//    // check the loopback first...
//    assertEquals(InetAddress.getByName(bindToAddress), jgAddr);
//    checkInetAddress(jgAddr, portsToCheck, true);
//
//    // check all non-loopback (127.0.0.1) interfaces...
//    // note: if this fails then this machine probably lacks a NIC
//    InetAddress host = InetAddress.getLocalHost();
//    assertEquals(false, InetAddressUtil.isLoopback(host));
//    InetAddress[] hostAddrs = InetAddress.getAllByName(host.getHostName());
//    for (int i = 0; i < hostAddrs.length; i++) {
//      if (!InetAddressUtil.isLoopback(hostAddrs[i])) {
//        // make sure sure non-loopback is not listening to ports...
//        checkInetAddress(hostAddrs[i], portsToCheck, false);
//      }
//    }
//  }

  private static void checkInetAddress(InetAddress addr,
      int[] ports,
      boolean isListening) throws Exception {
    for (int i = 0; i < ports.length; i++) {
      System.out.println("addr = " + addr);
      System.out.println("ports[i] = " + ports[i]);
      System.out.println("isListening = " + isListening);
      assertEquals(isListening, isPortListening(addr, ports[i]));
    }
  }

  /**
   * Gets the portListening attribute of the BindDistributedSystemJUnitTest class
   *
   * @param addr           Description of the Parameter
   * @param port           Description of the Parameter
   * @return               The portListening value
   * @exception Exception  Description of the Exception
   */
  private static boolean isPortListening(InetAddress addr, int port) throws Exception {
    // Try to create a Socket
    Socket client = null;
    try {
      client = new Socket(addr, port);
      return true;
    }
    catch (Exception e) {
      return false;
    }
    finally {
      if (client != null) {
        try {
          client.close();
        }
        catch (Exception e) {
          /*
           *  ignore
           */
        }
      }
    }
  }

}

