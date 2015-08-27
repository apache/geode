/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.*;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.cache.client.*;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * Test to verify that client side region.close() should unregister the client with the server.
 * It also checks that client region queue also gets removed properly.
 *
 * @author Suyog Bhoakre
 */

public class RegionCloseDUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM client1 = null;

  private static  int PORT1 ;

  private static final String REGION_NAME = "RegionCloseDUnitTest_region";

  protected static String  clientMembershipId;

  private static Cache cache = null;

  /** constructor */
  public RegionCloseDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
	  super.setUp();
    final Host host = Host.getHost(0);
    //Server1 VM
    server1 = host.getVM(0);
    //Client 1 VM
    client1 = host.getVM(1);

    PORT1 =  ((Integer)server1.invoke(RegionCloseDUnitTest.class, "createServerCache" )).intValue();
    client1.invoke(RegionCloseDUnitTest.class, "createClientCache", new Object[] {
      getServerHostName(host), new Integer(PORT1)});

  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }


  public void testCloseRegionOnClient()
  {
    server1.invoke(RegionCloseDUnitTest.class, "VerifyClientProxyOnServerBeforeClose");
    client1.invoke(RegionCloseDUnitTest.class, "closeRegion");
   // pause(10000);
    server1.invoke(RegionCloseDUnitTest.class, "VerifyClientProxyOnServerAfterClose");
  }

  public static void createClientCache(String host, Integer port1) throws Exception
  {
    PORT1 = port1.intValue() ;
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new RegionCloseDUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory()
      .addServer(host, PORT1)
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      .setReadTimeout(2000)
      .setThreadLocalConnections(true)
      .setSocketBufferSize(1000)
      .setMinConnections(2)
      // .setRetryAttempts(2)
      // .setRetryInterval(250)
      .create("RegionCloseDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();

    cache.createRegion(REGION_NAME, attrs);
  }

  public static Integer createServerCache() throws Exception
  {
    new RegionCloseDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static void VerifyClientProxyOnServerBeforeClose()
  {
    Cache c = CacheFactory.getAnyInstance();
    assertEquals("More than one CacheServer", 1, c.getCacheServers().size());


    final CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return bs.getAcceptor().getCacheClientNotifier().getClientProxies().size() == 1;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 15 * 1000, 200, true);
    assertEquals(1, bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());

    Iterator iter = bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
    if (iter.hasNext()) {
      CacheClientProxy proxy = (CacheClientProxy)iter.next();
      clientMembershipId = proxy.getProxyID().toString();
      assertNotNull(proxy.getHARegion());
    }
  }

  public static void closeRegion()
  {
    try {
      Region r = cache.getRegion("/"+ REGION_NAME);
      assertNotNull(r);
      String poolName = r.getAttributes().getPoolName();
      assertNotNull(poolName);
      Pool pool = PoolManager.find(poolName);
      assertNotNull(pool);
      r.close();
      pool.destroy();
    }
    catch (Exception ex) {
      fail("failed while region close", ex);
    }
  }

  public static void VerifyClientProxyOnServerAfterClose()
  {
    final Cache c = CacheFactory.getAnyInstance();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return c.getCacheServers().size() == 1;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 40 * 1000, 200, true);

    final CacheServerImpl bs = (CacheServerImpl)c.getCacheServers().iterator()
        .next();
    ev = new WaitCriterion() {
      public boolean done() {
        return c.getRegion("/" + clientMembershipId) == null;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 40 * 1000, 200, true);
    
    ev = new WaitCriterion() {
      public boolean done() {
        return bs.getAcceptor().getCacheClientNotifier().getClientProxies().size() != 1;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 40 * 1000, 200, true);
    // assertNull(c.getRegion("/"+clientMembershipId));
    assertEquals(0, bs.getAcceptor().getCacheClientNotifier()
        .getClientProxies().size());
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void tearDown2() throws Exception
  {
	super.tearDown2();
    //close client
    client1.invoke(RegionCloseDUnitTest.class, "closeCache");
    //close server
    server1.invoke(RegionCloseDUnitTest.class, "closeCache");
  }

}
