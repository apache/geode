package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/** A test of 46438 - missing response to an update attributes message */
public class ConnectDisconnectDUnitTest extends CacheTestCase {
  
  static {
//    System.setProperty("DistributionManager.VERBOSE", "true");
  }


  private ExpectedException ex;

  public ConnectDisconnectDUnitTest(String name) {
    super(name);
  }
  
  
  
//  @Override
//  public void setUp() throws Exception {
//    super.setUp();
//    invokeInEveryVM(new SerializableRunnable() {
//      
//      @Override
//      public void run() {
//        JGroupMembershipManager.setDebugJGroups(true);
//        System.setProperty("p2p.simulateDiscard", "true");
//        System.setProperty("p2p.simulateDiscard.received", "0.10");
//        System.setProperty("p2p.simulateDiscard.sent", "0.10");
//        System.setProperty("gemfire.membership-port-range", "17000-17200");
//      }
//    });
//  }
  
  
//  @Override
//  public void tearDown2() throws Exception {
//    ex.remove();
//  }


  // see bugs #50785 and #46438 
  public void testManyConnectsAndDisconnects() throws Throwable {
//    invokeInEveryVM(new SerializableRunnable() {
//
//      @Override
//      public void run() {
//        Log.setLogWriterLevel("info");
//      }
//    });

// uncomment these lines to use stand-alone locators
//     int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(4);
//     setLocatorPorts(ports);

    for(int i = 0; i < 50; i++) {
      getLogWriter().info("Test run: " + i);
      runOnce();
      tearDown();
      setUp();
    }
  }
  
  
  static int LOCATOR_PORT;
  static String LOCATORS_STRING;
  
  static int[] locatorPorts;
  
  public void setLocatorPorts(int[] ports) {
    deleteLocatorStateFile(ports);
    String locators = "";
    for (int i=0; i<ports.length; i++) {
      if (i > 0) {
        locators += ",";
      }
      locators += "localhost["+ports[i]+"]";
    }
    final String locators_string = locators;
    for (int i=0; i<ports.length; i++) {
      final int port = ports[i];
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable("set locator port") {
        public void run() {
          LOCATOR_PORT = port;
          LOCATORS_STRING = locators_string;
        }
      });
    }
    locatorPorts = ports;
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    if (locatorPorts != null) {
      deleteLocatorStateFile(locatorPorts);
    }
  }
  

  /**
   * This test creates 4 vms and starts a cache in each VM. If that doesn't hang, it destroys the DS in all
   * vms and recreates the cache.
   * @throws Throwable 
   */
  public void runOnce() throws Throwable {
    
    int numVMs = 4;
    
    VM[] vms = new VM[numVMs];
    
    for(int i= 0; i < numVMs; i++) {
//      if(i == 0) {
//        vms[i] = Host.getHost(0).getVM(4);
//      } else {
        vms[i] = Host.getHost(0).getVM(i);
//      }
    }
    
    AsyncInvocation[] asyncs = new AsyncInvocation[numVMs];
    for(int i= 0; i < numVMs; i++) {
      asyncs[i] = vms[i].invokeAsync(new SerializableRunnable("Create a cache") {
        @Override
        public void run() {
//          try {
//            JGroupMembershipManager.setDebugJGroups(true);
          getCache();
//          } finally {
//            JGroupMembershipManager.setDebugJGroups(false);
//          }
        }
      });
    }
    
    
    for(int i= 0; i < numVMs; i++) {
      asyncs[i].getResult();
//      try {
//        asyncs[i].getResult(30 * 1000);
//      } catch(TimeoutException e) { 
//        getLogWriter().severe("DAN DEBUG - we have a hang");
//        dumpAllStacks();
//        fail("DAN - WE HIT THE ISSUE",e);
//        throw e;
//      }
    }
    
    disconnectAllFromDS();
  }


  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty("log-level", "info");
    props.setProperty("conserve-sockets", "false");
    if (LOCATOR_PORT > 0) {
      props.setProperty("start-locator", "localhost["+LOCATOR_PORT+"]");
      props.setProperty("locators", LOCATORS_STRING);
    }
    return props;
  }
  
  
}
