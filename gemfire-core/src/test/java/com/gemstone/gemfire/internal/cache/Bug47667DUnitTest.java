/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.LocatorTestBase;
import com.gemstone.gemfire.internal.AvailablePort;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

public class Bug47667DUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = 2859534245283086765L;

  public Bug47667DUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnectAllFromDS();
  }

  public void testbug47667() {
    Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);

    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locatorHost = getServerHostName(host);
    startLocatorInVM(locator, locatorPort, "");

    String locString = getLocatorString(host, locatorPort);
    startBridgeServerInVM(server1, new String[] {"R1"}, locString, new String[] {"R1"});
    startBridgeServerInVM(server2, new String[] {"R2"}, locString, new String[] {"R2"});

    client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolLocator(locatorHost, locatorPort);
        ClientCache cache = ccf.create();
        PoolManager.createFactory().addLocator(locatorHost, locatorPort).setServerGroup("R1").create("R1");
        PoolManager.createFactory().addLocator(locatorHost, locatorPort).setServerGroup("R2").create("R2");
        Region r1 = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).setPoolName("R1").create("R1");
        Region r2 = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).setPoolName("R2").create("R2");
        CacheTransactionManager mgr = cache.getCacheTransactionManager();
        mgr.begin();
        r1.put(1, "value1");
        mgr.commit();
        mgr.begin();
        r2.put(2, "value2");
        mgr.commit();
        return null;
      }
    });
  }
}
