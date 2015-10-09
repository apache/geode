/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Same tests as the auto connection source test, but the
 * system is using multicast for membership discovery, and
 * the locator is only used for peer discovery.
 * @author dsmith
 *
 */
public class AutoConnectionSourceWithUDPDUnitTest extends
    AutoConnectionSourceDUnitTest {

  protected int mCastPort;

  public AutoConnectionSourceWithUDPDUnitTest(String name) {
    super(name);
  }
  
  public void testStartLocatorLater() throws InterruptedException {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    startBridgeServerInVM(vm1, null, null);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(vm0, locatorPort, "");
    
    startBridgeClientInVM(vm2, null, getServerHostName(vm0.getHost()), locatorPort);
    putAndWaitForSuccess(vm2, REGION_NAME, "key", "value");
    Assert.assertEquals("value", getInVM(vm1, "key"));
  }
  
  public void setUp() throws Exception {
    super.setUp();
    mCastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    addExpectedException("java.net.SocketException");
  }

  protected int startBridgeServerInVM(VM vm, final String[] groups, String locators,
      final String[] regions) {
    SerializableCallable connect =
      new SerializableCallable("Start bridge server") {
          public Object call() throws IOException  {
            Properties props = new Properties();
            props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mCastPort));
            props.setProperty(DistributionConfig.MCAST_ADDRESS_NAME, DistributionConfig.DEFAULT_MCAST_ADDRESS.getHostAddress());
            props.setProperty(DistributionConfig.LOCATORS_NAME, "");
            DistributedSystem ds = getSystem(props);
            Cache cache = CacheFactory.create(ds);
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setEnableBridgeConflation(true);
            factory.setDataPolicy(DataPolicy.REPLICATE);
            RegionAttributes attrs = factory.create();
            for(int i = 0; i < regions.length; i++) {
              cache.createRegion(regions[i], attrs);
            }
            CacheServer server = cache.addCacheServer();
            final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
            server.setPort(serverPort);
            server.setGroups(groups);
            server.start();
            
            remoteObjects.put(CACHE_KEY, cache);
            
            return new Integer(serverPort);
          }
        };
    Integer port = (Integer) vm.invoke(connect);
    return port.intValue();
  }

  public void startLocatorInVM(final VM vm, final int locatorPort, final String otherLocators) {
    vm.invoke(new SerializableRunnable("Create Locator") {

      final String testName= getUniqueName();
      public void run() {
        disconnectFromDS();
        Properties props = new Properties();
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mCastPort));
        props.setProperty(DistributionConfig.MCAST_ADDRESS_NAME, DistributionConfig.DEFAULT_MCAST_ADDRESS.getHostAddress());
        props.setProperty(DistributionConfig.LOCATORS_NAME, "");
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
        props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        InetAddress bindAddr = null;
        try {
          bindAddr = InetAddress.getByName(getServerHostName(vm.getHost()));
        } catch (UnknownHostException uhe) {
          fail("While resolving bind address ", uhe);
        }
        try {
          File logFile = new File(testName + "-locator" + locatorPort
              + ".log");
          Locator locator = Locator.startLocatorAndDS(locatorPort, logFile, bindAddr, props, false, true, null);
          remoteObjects.put(LOCATOR_KEY, locator);
        } catch (IOException ex) {
          fail("While starting locator on port " + locatorPort, ex);
        }
      }
    });
  }
  
  

}
