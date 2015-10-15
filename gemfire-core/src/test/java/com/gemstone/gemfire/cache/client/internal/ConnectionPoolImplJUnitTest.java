/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author dsmith
 *
 */
@Category(IntegrationTest.class)
public class ConnectionPoolImplJUnitTest {
  
  private static final String expectedRedundantErrorMsg = "Could not find any server to create redundant client queue on.";
  private static final String expectedPrimaryErrorMsg = "Could not find any server to create primary client queue on.";

  private Cache cache;
  private int port; 
  
  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    cache = CacheFactory.create(DistributedSystem.connect(props));
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  }
  
  @After
  public void tearDown() {
    if (cache != null && !cache.isClosed()) {
      unsetQueueError();
      cache.close();
    }
  }
  
  private void setQueueError() {
    final String addExpectedPEM =
        "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String addExpectedREM =
        "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";
    
    cache.getLogger().info(addExpectedPEM);
    cache.getLogger().info(addExpectedREM);
    
  }
  
  private void unsetQueueError() {
    final String removeExpectedPEM =
        "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String removeExpectedREM =
        "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";
    
    cache.getLogger().info(removeExpectedPEM);
    cache.getLogger().info(removeExpectedREM);
  }

  @Test
  public void testDefaults() throws Exception {
    setQueueError();
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port);

    PoolImpl pool = (PoolImpl) cpf.create("myfriendlypool");
    
    // check defaults
    assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT, pool.getFreeConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, pool.getSocketBufferSize());
    assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, pool.getReadTimeout());
    assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, pool.getMinConnections());
    assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, pool.getMaxConnections());
    assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, pool.getRetryAttempts());
    assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, pool.getIdleTimeout());
    assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, pool.getPingInterval());
    assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS, pool.getThreadLocalConnections());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED, pool.getSubscriptionEnabled());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY, pool.getSubscriptionRedundancy());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT, pool.getSubscriptionMessageTrackingTimeout());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL, pool.getSubscriptionAckInterval());
    assertEquals(PoolFactory.DEFAULT_SERVER_GROUP, pool.getServerGroup());
    // check non default
    assertEquals("myfriendlypool", pool.getName());
    assertEquals(1, pool.getServers().size());
    assertEquals(0, pool.getLocators().size());
    {
      InetSocketAddress addr = (InetSocketAddress)pool.getServers().get(0);
      assertEquals(port, addr.getPort());
      assertEquals("localhost", addr.getHostName());
    }
    
  }
  
  @Test
  public void testProperties() throws Exception {
    int readTimeout = 234234;

    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port)
      .setReadTimeout(readTimeout)
      .setThreadLocalConnections(true);

    PoolImpl pool = (PoolImpl) cpf.create("myfriendlypool");

    // check non default
    assertEquals("myfriendlypool", pool.getName());
    assertEquals(readTimeout, pool.getReadTimeout());
    assertEquals(true, pool.getThreadLocalConnections());
    assertEquals(1, pool.getServers().size());
    assertEquals(0, pool.getLocators().size());
    {
      InetSocketAddress addr = (InetSocketAddress)pool.getServers().get(0);
      assertEquals(port, addr.getPort());
      assertEquals("localhost", addr.getHostName());
    }
  }
  
  @Test
  public void testCacheClose() throws Exception {
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addLocator("localhost", AvailablePortHelper.getRandomAvailableTCPPort());
    Pool pool1 = cpf.create("pool1");
    Pool pool2 = cpf.create("pool2");
    cache.close();
    
    assertTrue(pool1.isDestroyed());
    assertTrue(pool2.isDestroyed());
  }
  
  @Test
  public void testExecuteOp() throws Exception {
    CacheServer server1 = cache.addCacheServer();
    CacheServer server2 = cache.addCacheServer();
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = ports[0];
    int port2 = ports[1];
    server1.setPort(port1);
    server2.setPort(port2);
    
    server1.start();
    server2.start();
    
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port2);
    cpf.addServer("localhost", port1);
    PoolImpl pool = (PoolImpl) cpf.create("pool1");
    
    ServerLocation location1 = new ServerLocation("localhost", port1);
    ServerLocation location2 = new ServerLocation("localhost", port2);
    
    Op testOp = new Op() {
      int attempts = 0;

      public Object attempt(Connection cnx) throws Exception {
        if(attempts == 0) {
          attempts++;
          throw new SocketTimeoutException();
        }
        else {
          return cnx.getServer();
        }
          
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    //TODO - set retry attempts, and throw in some assertions
    //about how many times we retry
    
    ServerLocation usedServer = (ServerLocation) pool.execute(testOp);
    assertTrue("expected " + location1 + " or " + location2 + ", got " + usedServer,
        location1.equals(usedServer) || location2.equals(usedServer));
    
    testOp = new Op() {
      public Object attempt(Connection cnx) throws Exception {
          throw new SocketTimeoutException();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    try {
      usedServer = (ServerLocation) pool.execute(testOp);
      fail("Should have failed");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
  }
  
  @Test
  public void testCreatePool() throws Exception {
    CacheServer server1 = cache.addCacheServer();
    int port1 = port;
    server1.setPort(port1);
    
    server1.start();
    
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port1);
    cpf.setSubscriptionEnabled(true);
    cpf.setSubscriptionRedundancy(0);
    PoolImpl pool = (PoolImpl) cpf.create("pool1");
    
    ServerLocation location1 = new ServerLocation("localhost", port1);
    
    Op testOp = new Op() {
      public Object attempt(Connection cnx) throws Exception {
          return cnx.getServer();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    assertEquals(location1, pool.executeOnPrimary(testOp));
    assertEquals(location1, pool.executeOnQueuesAndReturnPrimaryResult(testOp));
  }
  
}
