/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;

import dunit.DistributedTestCase;
import dunit.Host;

/**
 * @author dsmith
 * @since 5.7
 *
 * Test reconnecting a durable client that is using
 * the locator to discover its servers
 */
public class DurableClientReconnectAutoDUnitTest extends
    DurableClientReconnectDUnitTest {

  public static void caseSetUp() throws Exception {
    DistributedTestCase.disconnectAllFromDS();
  }
 
  public DurableClientReconnectAutoDUnitTest(String name) {
    super(name);
  }
  
  public void testDurableReconnectSingleServerWithZeroConnPerServer() {
    //do nothing, this test doesn't make sense with the locator
  }

  public void testDurableReconnectSingleServer() throws Exception {
    //do nothing, this test doesn't make sense with the locator
  }
  
  protected PoolFactory getPoolFactory() {
    Host host = Host.getHost(0);
    PoolFactory factory = PoolManager.createFactory()
    .addLocator(getServerHostName(host), getDUnitLocatorPort());
    return factory;
  }

}
