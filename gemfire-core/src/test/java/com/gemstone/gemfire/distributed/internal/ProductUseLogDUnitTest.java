/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

public class ProductUseLogDUnitTest extends DistributedTestCase {

  public ProductUseLogDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    return p;
  }
  
  public void testMembershipMonitoring() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // use a locator so we will monitor server load and record member->server mappings
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    Properties p = new Properties();
    p.put(DistributionConfig.START_LOCATOR_NAME, "localhost["+locatorPort+"],peer=false");
    p.put(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "false");
    InternalDistributedSystem system = getSystem(p);
    
    InternalLocator locator = (InternalLocator)Locator.getLocator();
    // server location is forced on for product use logging
    assertTrue(locator.isServerLocator());
    
    File logFile = new File("locator"+locatorPort+"views.log");
    // the locator should have already created this file
    assertTrue(logFile.exists());
    
    assertTrue(logFile.exists());
    vm0.invoke(new SerializableRunnable("get system") {
      public void run() {
        InternalDistributedSystem system = getSystem();
        Cache cache = CacheFactory.create(system);
        CacheServer server = cache.addCacheServer();
        try {
          server.start();
        } catch (IOException e) {
          fail("failed to start server", e);
        }
      }
    });

    // wait for the server info to be received and logged 
//    pause(2 * BridgeServerImpl.FORCE_LOAD_UPDATE_FREQUENCY * 1000);

    system.disconnect();

    String logContents = readFile(logFile);
    assertTrue("expected " + logFile + " to contain a View", logContents.contains("View"));
    assertTrue("expected " + logFile + " to contain 'server summary'", logContents.contains("server summary"));
  }
  
  private String readFile(File file) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    StringBuilder sb = new StringBuilder(10000);
    String line;
    while ( (line = reader.readLine()) != null ) {
      sb.append(line).append(File.separator);
    }
    return sb.toString();
  }

  public void tearDown2() {
    disconnectAllFromDS();
  }
}
