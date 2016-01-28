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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;

import junit.framework.Assert;
import util.TestException;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.cq.CqQueryImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Class <code>DurableClientTestCase</code> tests durable client
 * functionality.
 * 
 * @author Barry Oglesby
 * 
 * @since 5.2
 */ 
public class DurableClientTestCase extends DistributedTestCase {

  protected VM server1VM;
  protected VM server2VM;
  protected VM durableClientVM;
  protected VM publisherClientVM;
  protected String regionName;
  
  protected static volatile boolean isPrimaryRecovered = false;

  public DurableClientTestCase(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    Host host = Host.getHost(0);
    this.server1VM = host.getVM(0);
    this.server2VM = host.getVM(1);
    this.durableClientVM = host.getVM(2);
    this.publisherClientVM = host.getVM(3);
    this.regionName = getName() + "_region";
    //Clients see this when the servers disconnect
    addExpectedException("Could not find any server");
    testName = getName();
    System.out.println("\n\n[setup] START TEST " + getClass().getSimpleName()+"."+ testName+"\n\n");
  }
  
  public void tearDown2() throws Exception {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  /**
   * Test that starting a durable client is correctly processed by the server.
   */
  public void testSimpleDurableClient() {    
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId)});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
        //assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_KEEP_ALIVE, proxy.getDurableKeepAlive());
      }
    });
    
    // Stop the durable client
    this.disconnectDurableClient();
    
    // Verify the durable client is present on the server for closeCache=false case.
    this.verifySimpleDurableClient();
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

    this.closeDurableClient();
    
  }
  
  /**
   * Test that starting a durable client is correctly processed by the server.
   * In this test we will set gemfire.SPECIAL_DURABLE property to true and will
   * see durableID appended by poolname or not
   */
  public void testSimpleDurableClient2() {
    final Properties jp = new Properties();
    jp.setProperty("gemfire.SPECIAL_DURABLE", "true");

    try {
      // Start a server
      int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class, "createCacheServer", new Object[] { regionName, new Boolean(true) })).intValue();

      // Start a durable client that is not kept alive on the server when it
      // stops normally
      final String durableClientId = getName() + "_client";

      this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
          new Object[] { getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId), new Boolean(false), jp });

      // Send clientReady message
      this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
        public void run2() throws CacheException {
          CacheServerTestUtil.getCache().readyForEvents();
        }
      });

      // Verify durable client on server
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        public void run2() throws CacheException {
          // Find the proxy
          checkNumberOfClientProxies(1);
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);

          // Verify that it is durable and its properties are correct
          assertTrue(proxy.isDurable());
          assertNotSame(durableClientId, proxy.getDurableId());

          /* new durable id will be like this
           * durableClientId
           * _gem_ //separartor
           * client pool name */
          String dId = durableClientId + "_gem_" + "CacheServerTestUtil";

          assertEquals(dId, proxy.getDurableId());
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
          // assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_KEEP_ALIVE, proxy.getDurableKeepAlive());
        }
      });

      // Stop the durable client
      this.disconnectDurableClient();

      // Verify the durable client is present on the server for closeCache=false case.
      this.verifySimpleDurableClient();

      // Stop the server
      this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

      this.closeDurableClient();
    } finally {
 
      this.durableClientVM.invoke(CacheServerTestUtil.class, "unsetJavaSystemProperties", 
        new Object[] {jp});
    }
    
  }
  
  public void closeDurableClient()
  {
  }
  
  public void disconnectDurableClient()
  {
    this.disconnectDurableClient(false);    
  }

  public void disconnectDurableClient(boolean keepAlive)
  {
    printClientProxyState("Before");
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache",new Object[] {new Boolean(keepAlive)});
    pause(1000);
    printClientProxyState("after");
  }

  public void printClientProxyState(String st) {
    CacheSerializableRunnable s = new CacheSerializableRunnable("Loggiog CCCP and ServerConnetcion state"){
      @Override
      public void run2() throws CacheException {
        // TODO Auto-generated method stub
        CacheServerTestUtil.getCache().getLogger().info(st + " CCP states: " + getAllClientProxyState());
        CacheServerTestUtil.getCache().getLogger().info(st + " CHM states: " + printMap(ClientHealthMonitor._instance.getConnectedClients(null)));
      }
    };
    server1VM.invoke(s);
  }
  
  private String printMap(Map m) {
    Iterator<Map.Entry> itr = m.entrySet().iterator();
    StringBuffer sb = new StringBuffer();
    while(itr.hasNext()){
      sb.append("{");
      Map.Entry entry = itr.next();
      sb.append(entry.getKey());
      sb.append(", ");
      printMapValue(entry.getValue(), sb);
      sb.append("}");
    }
    return sb.toString();    
  }
  
  private void printMapValue(Object value, StringBuffer sb) {
    if(value.getClass().isArray()) {
      
      sb.append("{");
      sb.append(Arrays.toString((Object[])value));
      sb.append("}");
    } else {
      sb.append(value);
    }
  }
  
  public void verifySimpleDurableClient()
  {
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertNull(proxy);
      }
    });
  }
  
  /**
   * Test that starting, stopping then restarting a durable client is correctly
   * processed by the server.
   */
  public void testStartStopStartDurableClient() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        //assertEquals(durableClientKeepAlive, proxy.getDurableKeepAlive());
      }
    });
    
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Re-start the durable client
    this.restartDurableClient(new Object[] {
        getClientPool(getServerHostName(durableClientVM.getHost()),serverPort, true),
        regionName,
        getClientDistributedSystemProperties(durableClientId,
            durableClientTimeout) });
    
    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });
        
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  /**
   * Test that starting, stopping then restarting a durable client is correctly
   * processed by the server.
   * This is a test of bug 39630
   */
  public void test39630() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)});

//    // Send clientReady message
//    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
//      public void run2() throws CacheException {
//        CacheServerTestUtil.getCache().readyForEvents();
//      }
//    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        //assertEquals(durableClientKeepAlive, proxy.getDurableKeepAlive());
      }
    });
    
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Verify the durable client still exists on the server, and the socket is closed
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        Assert.assertNotNull(proxy._socket);
        long end = System.currentTimeMillis() + 60000;
        
        while(!proxy._socket.isClosed()) {
          if(System.currentTimeMillis() > end) {
            break;
          }
        }
        Assert.assertTrue(proxy._socket.isClosed());
      }
    });
    
    // Re-start the durable client (this is necessary so the
    //netDown test will set the appropriate system properties.
    this.restartDurableClient(new Object[] {
        getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true),
        regionName,
        getClientDistributedSystemProperties(durableClientId,
            durableClientTimeout) });

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  public void restartDurableClient(Object[] args)
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", args);

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });
  }
  
  /**
   * Test that disconnecting a durable client for longer than the timeout
   * period is correctly processed by the server.
   */
  public void testStartStopTimeoutDurableClient() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 5; // keep the client alive for 5 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(durableClientTimeout, proxy.getDurableTimeout());
      }
    });
    
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });
    
    // Pause to let the client timeout. This time is 2.5 seconds longer than
    // the durableClientTimeout set above. It should be long enough for the 
    // client to timeout and get cleaned up. There could be a race here,
    // though.
    // no need for the explicit pause since checkNumberOfClientProxies
    // will wait up to 15 seconds
    //pause(7500);
    
    // Verify it no longer exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
        CacheClientProxy proxy = getClientProxy();
        assertNull(proxy);
      }
    });
    
    this.restartDurableClient(new Object[] {
        getClientPool(getServerHostName(Host.getHost(0)), serverPort, true),
        regionName,
        getClientDistributedSystemProperties(durableClientId,
            durableClientTimeout) });

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  public void testDurableClientPrimaryUpdate() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 120; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

    // Publish some entries
    final int numberOfEntries = 1;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
    
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    //Make sure the proxy is actually paused, not dispatching
    this.server1VM.invoke(new CacheSerializableRunnable("Wait for paused") {
      public void run2() throws CacheException {
        WaitCriterion  wc = new WaitCriterion() {
          public boolean done() {
            CacheClientProxy proxy = getClientProxy();
            return proxy != null && proxy.isPaused();
          }
          public String description() {
            return "Proxy was not paused: " + getClientProxy();
          }
        };
        //If we wait too long, the durable queue will be gone, because
        //the timeout is 120 seconds.
        DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      }
    });

    // Publish some more entries
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
        
    // Verify the durable client's queue contains the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        WaitCriterion  wc = new WaitCriterion() {
          String excuse;
          public boolean done() {
            CacheClientProxy proxy = getClientProxy();
            if (proxy == null) {
              excuse = "No CacheClientProxy";
              return false;
            }
            // Verify the queue size
            int sz = proxy.getQueueSize();
            if (numberOfEntries != sz) {
              excuse = "expected = " + numberOfEntries + ", actual = " + sz;
              return false;
            }
            return true;
          }
          public String description() {
            return excuse;
          }
        };
        //If we wait too long, the durable queue will be gone, because
        //the timeout is 120 seconds.
        DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
      }
    });

    // Verify that disconnected client does not receive any events.
    this.verifyListenerUpdatesDisconnected(numberOfEntries);
    
    // Re-start the durable client
    this.restartDurableClient(new Object[] {
        getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId), Boolean.TRUE });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });
        
    // Verify the durable client received the updates held for it on the server
    this.verifyListenerUpdates(numberOfEntries, numberOfEntries);
    
    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the durable client VM
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Test that a durable client correctly receives updates after it reconnects.
   */
  public void testStartStopStartDurableClientUpdate() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

    // Publish some entries
    final int numberOfEntries = 1;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);
    
    // ARB: Wait for queue ack to arrive at server.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
    
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        final CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return proxy.isPaused();
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 1000, 200, true);
        assertTrue(proxy.isPaused());
      }
    });

    // Publish some more entries
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish more entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
    
    // Verify the durable client's queue contains the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify the queue size
        assertEquals(numberOfEntries, proxy.getQueueSize());
      }
    });

    // Verify that disconnected client does not receive any events.
    this.verifyListenerUpdatesDisconnected(numberOfEntries);
    
    // Re-start the durable client
    this.restartDurableClient(new Object[] {
        getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName,
        getClientDistributedSystemProperties(durableClientId), Boolean.TRUE });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });
        
    // Verify the durable client received the updates held for it on the server
    this.verifyListenerUpdates(numberOfEntries, numberOfEntries);
    
    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the durable client VM
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  public void verifyListenerUpdates(int numEntries)
  {
    this.verifyListenerUpdatesEntries(numEntries, 0);
  }
  
  public void verifyListenerUpdates(int numEntries, int numEntriesBeforeDisconnect)
  {
    this.verifyListenerUpdatesEntries(numEntries, 0);
  }
      
  public void verifyListenerUpdatesEntries(final int numEntries, final int numEntriesBeforeDisconnect)
  {
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener = (CacheServerTestUtil.ControlListener) region
                .getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(60 * 1000, numEntries+numEntriesBeforeDisconnect);
        assertEquals(numEntries+numEntriesBeforeDisconnect, listener.events.size());
        //CacheServerTestUtil.getCache().getLogger().info("ARB: verify updates(): numEntries = " + numEntries);
        //CacheServerTestUtil.getCache().getLogger().info("ARB: verify updates(): listener.events.size() = " + listener.events.size());
      }
    });
  }
  
  public void verifyListenerUpdatesDisconnected(int numberOfEntries)
  {
    // ARB: do nothing.
  }
  
  public void verifySimpleDurableClientMultipleServers()
  {
    // Verify the durable client is no longer on server1
    this.server1VM
        .invoke(new CacheSerializableRunnable("Verify durable client") {
          public void run2() throws CacheException {
            // Find the proxy
            checkNumberOfClientProxies(0);
            CacheClientProxy proxy = getClientProxy();
            assertNull(proxy);
          }
        });

    // Verify the durable client is no longer on server2
    this.server2VM
        .invoke(new CacheSerializableRunnable("Verify durable client") {
          public void run2() throws CacheException {
            // Find the proxy
            checkNumberOfClientProxies(0);
            CacheClientProxy proxy = getClientProxy();
            assertNull(proxy);
          }
        });
  }
  
  /**
   * Test whether a durable client reconnects properly to a server that is
   * stopped and restarted.
   */
  public void testDurableClientConnectServerStopStart() {
    // Start a server
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });
    
    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });

    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Re-start the server
    this.server1VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] {regionName, new Boolean(true),
            new Integer(serverPort)});
    
    // Pause 10 seconds to allow client to reconnect to server
    // no need for the explicit pause since checkNumberOfClientProxies
    // will wait up to 15 seconds
    //pause(10000);
    
    // Verify durable client on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });
    
    // Start a publisher
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

    // Publish some messages
    // Publish some entries
    final int numberOfEntries = 10;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener = (CacheServerTestUtil.ControlListener) region
                .getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries);
        assertEquals(numberOfEntries, listener.events.size());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  @Ignore("This test is failing inconsistently, see bug 51258")
  public void DISABLED_testDurableNonHAFailover() throws InterruptedException
  {
      durableFailover(0);
      durableFailoverAfterReconnect(0);   
  }

  @Ignore("This test is failing inconsistently, see bug 51258")
  public void DISABLED_testDurableHAFailover() throws InterruptedException
  {
    //Clients see this when the servers disconnect
    addExpectedException("Could not find any server");
    durableFailover(1);
    durableFailoverAfterReconnect(1);
  }
  
  /**
   * Test a durable client with 2 servers where the client fails over from one to another server
   * with a publisher/feeder performing operations and the client verifying updates received.
   * Redundancy level is set to 1 for this test case.
   */
  public void durableFailover(int redundancyLevel) throws InterruptedException {
    
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int server1Port = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int server2Port = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
        
    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Start a durable client
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(getServerHostName(Host.getHost(0)), server1Port, server2Port, true); 
    }
    else {
      clientPool = getClientPool(getServerHostName(Host.getHost(0)), server1Port, server2Port, true, 0);
    }
    
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });
    
    // Re-start server2
    this.server2VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] { regionName, new Boolean(true),
            new Integer(server2Port)});
        
    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(publisherClientVM.getHost()), server1Port, server2Port, false), regionName});

    // Publish some entries
    final int numberOfEntries = 1;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);

    try {
      java.lang.Thread.sleep(5000);
    }
    catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }
    
    
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Publish updates during client downtime
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    
    // Re-start the durable client that is kept alive on the server
    this.restartDurableClient(new Object[] {
        clientPool,
        regionName,
        getClientDistributedSystemProperties(durableClientId,
            durableClientTimeout), Boolean.TRUE });
    
    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });
    
    // Publish second round of updates
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries before failover") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=1; i<numberOfEntries+1; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates before failover
    this.verifyListenerUpdates(numberOfEntries+1, numberOfEntries);

    try {
      java.lang.Thread.sleep(1000);
    }
    catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }

    setPrimaryRecoveryCheck();
    
    // Stop server 1 - publisher will put 10 entries during shutdown/primary identification
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

    this.durableClientVM.invoke(new CacheSerializableRunnable("Get") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        assertNull(region.getEntry("0"));
      }
    });
    
    checkPrimaryRecovery();
    // Publish second round of updates after failover
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries after failover") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=2; i<numberOfEntries+2; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates after failover
    this.verifyListenerUpdates(numberOfEntries+2, numberOfEntries);
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  

  public void durableFailoverAfterReconnect(int redundancyLevel)
  {

    
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int server1Port = ports[0].intValue();
    final int mcastPort = ports[1].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int server2Port = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true), new Integer(mcastPort)}))
        .intValue();
    
    // Start a durable client
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    Pool clientPool;
    if (redundancyLevel == 1) {
      clientPool = getClientPool(getServerHostName(Host.getHost(0)), server1Port, server2Port, true); 
    }
    else {
      clientPool = getClientPool(getServerHostName(Host.getHost(0)), server1Port, server2Port, true, 0);
    }
    
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {clientPool, regionName,
        getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });
    
    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(getServerHostName(publisherClientVM.getHost()), server1Port, server2Port, false), regionName});

    // Publish some entries
    final int numberOfEntries = 1;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    try {
      java.lang.Thread.sleep(10000);
    }
    catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }
    
    // Verify the durable client received the updates
    this.verifyListenerUpdates(numberOfEntries);
    
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
      }
    });

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Stop server 1 - publisher will put 10 entries during shutdown/primary identification
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Publish updates during client downtime
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });

    // Re-start the durable client that is kept alive on the server
    this.restartDurableClient(new Object[] {
        clientPool,
        regionName,
        getClientDistributedSystemProperties(durableClientId,
            durableClientTimeout), Boolean.TRUE });

    // Have the durable client register interest in all keys
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });
    
    // Publish second round of updates
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries before failover") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=1; i<numberOfEntries+1; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates before failover
    if (redundancyLevel == 1) {
      this.verifyListenerUpdates(numberOfEntries+1, numberOfEntries);
    }
    else {
      this.verifyListenerUpdates(numberOfEntries, numberOfEntries);
    }
    
    this.durableClientVM.invoke(new CacheSerializableRunnable("Get") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        assertNull(region.getEntry("0"));
      }
    });
    
    // Publish second round of updates after failover
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish entries after failover") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=2; i<numberOfEntries+2; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue);
        }
      }
    });
    
    // Verify the durable client received the updates after failover
    if (redundancyLevel == 1) {
      this.verifyListenerUpdates(numberOfEntries+2, numberOfEntries);
    }
    else {
      this.verifyListenerUpdates(numberOfEntries+1, numberOfEntries);
    }
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  
  //First we will have the client wait before trying to reconnect
  //Then the drain will lock and begins to drain
  //The client will then be able to continue, and get rejected
  //Then we proceed to drain and release all locks
  //The client will then reconnect
  public class RejectClientReconnectTestHook implements CacheClientProxy.TestHook {
      CountDownLatch reconnectLatch = new CountDownLatch(1);
      CountDownLatch continueDrain = new CountDownLatch(1);
      volatile boolean clientWasRejected = false;
      CountDownLatch clientConnected = new CountDownLatch(1);
      
      public void doTestHook(String spot) {
        System.out.println("JASON " + spot);
        try {
          if (spot.equals("CLIENT_PRE_RECONNECT")) {
            if (!reconnectLatch.await(60, TimeUnit.SECONDS)) {
              throw new TestException("reonnect latch was never released.");
            }
          }
          else if (spot.equals("DRAIN_IN_PROGRESS_BEFORE_DRAIN_LOCK_CHECK")) {
            //let client try to reconnect
            reconnectLatch.countDown();
            //we wait until the client is rejected
            if (!continueDrain.await(120, TimeUnit.SECONDS)) {
              throw new TestException("Latch was never released.");
            }
          }
          else if (spot.equals("CLIENT_REJECTED_DUE_TO_CQ_BEING_DRAINED")) {
            clientWasRejected = true;
            continueDrain.countDown();
          }
          else if (spot.equals("DRAIN_COMPLETE")) {
            
          }
        }
        catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
      
      public boolean wasClientRejected() {
        return clientWasRejected;
      }
  }  

  /*
   * This hook will cause the close cq to throw an exception due to a client in the middle of activating
   * sequence -
   * server will pause before draining
   * client will begin to reconnect and then wait to continue
   * server will be unblocked, and rejected
   * client will the be unlocked after server is rejected and continue
   */
  public class CqExceptionDueToActivatingClientTestHook implements CacheClientProxy.TestHook {
      CountDownLatch unblockDrain = new CountDownLatch(1);
      CountDownLatch unblockClient = new CountDownLatch(1);
      CountDownLatch finish = new CountDownLatch(1);
      public void doTestHook(String spot) {
        if (spot.equals("PRE_DRAIN_IN_PROGRESS")) {
          try {
            //Unblock any client waiting to reconnect
            unblockClient.countDown();
            //Wait until client is reconnecting
            if (!unblockDrain.await(120, TimeUnit.SECONDS)) {
              throw new TestException("client never got far enough reconnected to unlatch lock.");
            }
          }
          catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
          }
        }
        if (spot.equals("PRE_RELEASE_DRAIN_LOCK")) {
          //Client is reconnecting but still holds the drain lock
          //let the test continue to try to close a cq
          unblockDrain.countDown();
          //wait until the server has finished attempting to close the cq
          try {
            if (!finish.await(30, TimeUnit.SECONDS) ) {
              throw new TestException("Test did not complete, server never finished attempting to close cq");
            }
          }
          catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
          }
        }
        if (spot.equals("DRAIN_COMPLETE")) {
          finish.countDown();
        }
      }
  }

  
  protected CqQuery createCq(String cqName, String cqQuery, boolean durable) throws CqException, CqExistsException {
    QueryService qs = CacheServerTestUtil.getCache().getQueryService();
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = { new CacheServerTestUtil.ControlCqListener() };
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();
    return qs.newCq(cqName, cqQuery, cqa, durable);
   
  }

  protected Pool getClientPool(String host, int serverPort, boolean establishCallbackConnection){
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, serverPort)
      .setSubscriptionEnabled(establishCallbackConnection)
      .setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl)pf).getPoolAttributes();
  }
  
  
  protected Pool getClientPool(String host, int server1Port, int server2Port, boolean establishCallbackConnection){
    return getClientPool(host, server1Port, server2Port, establishCallbackConnection, 1);
  }
  
  
  protected Properties getClientDistributedSystemProperties(String durableClientId) {
    return getClientDistributedSystemProperties(durableClientId,
        DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }
  
  protected Properties getClientDistributedSystemPropertiesNonDurable(String durableClientId) {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return properties;
  }
  
  
  protected Properties getClientDistributedSystemProperties(
      String durableClientId, int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME, durableClientId);
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, String.valueOf(durableClientTimeout));
    return properties;
  }
  
  protected static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor()
        .getCacheClientNotifier();
    
    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = (CacheClientProxy) i.next();
    }
    return proxy;
  }
  
  protected static String getAllClientProxyState() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor()
        .getCacheClientNotifier();
    
    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    StringBuffer sb = new StringBuffer();
    while(i.hasNext()) {
      sb.append(" [");
      sb.append(i.next().getState());
      sb.append(" ]");
    }
    return sb.toString();
  }
  
  protected static void checkNumberOfClientProxies(final int expected) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return expected == getNumberOfClientProxies();
      }
      public String description() {
        /*String result =  "";
        if(ClientHealthMonitor._instance != null && ClientHealthMonitor._instance.getCleanupProxyIdTable() != null)
          result = ClientHealthMonitor._instance.getCleanupProxyIdTable().toString(); 
        
        
        CacheClientProxy ccp = getClientProxy();
        
        if(ccp != null)
          result += " ccp: " + ccp.toString();*/
        return getAllClientProxyState();
      }
    };
    DistributedTestCase.waitForCriterion(ev, 50 * 1000, 200, true);
  }
  
  protected static void checkProxyIsAlive(final CacheClientProxy proxy) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return proxy.isAlive();
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 15 * 1000, 200, true);
  }
  
  protected static int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier()
        .getClientProxies().size();
  }
  
  protected static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer = (CacheServerImpl) CacheServerTestUtil
        .getCache().getCacheServers().iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }
  

  protected Pool getClientPool(String host, int server1Port, int server2Port, boolean establishCallbackConnection, int redundancyLevel){
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port)
      .addServer(host, server2Port)
      .setSubscriptionEnabled(establishCallbackConnection)
      .setSubscriptionRedundancy(redundancyLevel)
      .setSubscriptionAckInterval(1);
    return ((PoolFactoryImpl)pf).getPoolAttributes();
  }
  
  /**
   * Returns the durable client proxy's HARegionQueue region name. This method
   * is accessed via reflection on a server VM.
   * @return the durable client proxy's HARegionQueue region name
   */
  protected static String getHARegionQueueName() {
    checkNumberOfClientProxies(1);
    CacheClientProxy proxy = getClientProxy();
    assertNotNull(proxy);
    return proxy.getHARegionName();
  }
  
  public static void verifyReceivedMarkerAck(final CacheClientProxy proxy) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        GemFireCacheImpl.getInstance().getLoggerI18n().fine(
            "DurableClientDUnitTest->WaitCriterion :: done called");
        return checkForAck(proxy);
      }
      public String description() {
        return "never received marker ack";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 3 * 60 * 1000, 200/*0*/, true);
  }

  /**
   * This is an incredibly ugly test to see if we got an ack from the client.
   * the dispatched messages map is <b> static<b> map, which has
   * region queue names for keys and MapWrappers (an protected class)
   * as values.  All this is testing is to see that this queue has an entry in the dispatchedMessages
   * map, which means it got at least one periodic ack.
   * @return true if there was an ack
   */
  protected static boolean checkForAck(CacheClientProxy proxy) {
      //pause(5000);
      return HARegionQueue.isTestMarkerMessageRecieved();
  }
  
  protected static void setTestFlagToVerifyActForMarker(Boolean flag){
    HARegionQueue.setUsedByTest(flag.booleanValue());
  }
  
  public static void setBridgeObeserverForAfterPrimaryRecovered() {
    DurableClientTestCase.isPrimaryRecovered = false;
    PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG = true;
    ClientServerObserver bo = ClientServerObserverHolder
        .setInstance(new ClientServerObserverAdapter() {
          public void afterPrimaryRecovered(ServerLocation location) {
            DurableClientTestCase.isPrimaryRecovered = true;
            PoolImpl.AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG = false;
          }
        });
  }
  
  public void setPrimaryRecoveryCheck() {
    this.durableClientVM.invoke(new CacheSerializableRunnable("Set observer") {
      public void run2() {
        setBridgeObeserverForAfterPrimaryRecovered();
      }
    });
  }
  
  public void checkPrimaryRecovery() {
    this.durableClientVM.invoke(new CacheSerializableRunnable("Check observer") {
      public void run2() {
        WaitCriterion waitForPrimaryRecovery = new WaitCriterion() {
          public boolean done() {
            return DurableClientTestCase.isPrimaryRecovered;
          }

          public String description() {
            return "Did not detect primary recovery event during wait period";
          }
        };
        
        // wait for primary (and interest) recovery
        // recovery satisfier task currently uses ping interval value
        DistributedTestCase.waitForCriterion(waitForPrimaryRecovery, 30000, 1000, true);
      }
    });
  }
  
  

  protected void sendClientReady(VM vm) {
   // Send clientReady message
    vm.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });
  }
  
  protected void registerInterest(VM vm, final String regionName, final boolean durable) {
    vm.invoke(new CacheSerializableRunnable("Register interest on region : " + regionName) {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, durable);
      }
    });
  }
  
  protected void createCq(VM vm, final String cqName, final String cqQuery, final boolean durable) {
    vm.invoke(new CacheSerializableRunnable(
        "Register cq " + cqName) {
      public void run2() throws CacheException {
        
        try {
          createCq(cqName, cqQuery, durable).execute();
        }
        catch (CqExistsException e) {
          throw new CacheException(e) {};
        }
        catch (CqException e) {
          throw new CacheException(e) {};
        }        
        catch (RegionNotFoundException e) {
          throw new CacheException(e) {};
        }
        
      }
    });
  }
  
  protected void publishEntries(VM vm, final String regionName, final int numEntries) {
    vm.invoke(new CacheSerializableRunnable(
        "publish " + numEntries + " entries") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i = 0; i < numEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, new Portfolio(i));
        }
      }
    });
  }
  
  /*
   * Due to the way removal from ha region queue is implemented
   * a dummy cq or interest needs to be created and a dummy value used so that
   * none of the actual cqs will be triggered and yet an event will
   * flush the queue
   */
  protected void flushEntries(VM server, VM client, final String regionName) {
    //This wait is to make sure that all acks have been responded to...
    //We can add a stat later on the cache client proxy stats that checks 
    //ack counts
    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    registerInterest(client, regionName, false);
    server.invoke(new CacheSerializableRunnable("flush entries") {
      public void run2() throws CacheException {
        CqService service = ((InternalCache) CacheServerTestUtil.getCache()).getCqService();
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);
        region.put("LAST", "ENTRY");
      }
    });
  }
  
  
  protected void checkCqStatOnServer(VM server, final String durableClientId, final String cqName, final int expectedNumber) {
    server.invoke(new CacheSerializableRunnable(
        "Check ha queued cq stats for durable client " + durableClientId + " cq: " + cqName) {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();
        CqService cqService = ((InternalCache)CacheServerTestUtil.getCache()).getCqService();
        cqService.start();
        final CqQueryImpl cqQuery = (CqQueryImpl)cqService.getClientCqFromServer(proxyId, cqName);
       
        //Wait until we get the expected number of events or until 10 seconds are up
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return cqQuery.getVsdStats().getNumHAQueuedEvents() == expectedNumber;
          }
          public String description() {
            return "cq numHAQueuedEvents stat was expected to be " + expectedNumber + " but was instead " + cqQuery.getVsdStats().getNumHAQueuedEvents();
          }
        };
        DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
        
        assertEquals(expectedNumber, cqQuery.getVsdStats().getNumHAQueuedEvents());
      }
    });
  }
  
  /*
   * Remaining is the number of events that could still be in the queue due to timing issues with acks
   * and receiving them after remove from ha queue region has been called.
   */
  protected void checkHAQueueSize(VM server, final String durableClientId, final int expectedNumber, final int remaining) {
    server.invoke(new CacheSerializableRunnable(
        "Check ha queued size for durable client " + durableClientId) {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();
        
        //Wait until we get the expected number of events or until 10 seconds are up
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return clientProxy.getQueueSizeStat() == expectedNumber || clientProxy.getQueueSizeStat() == remaining;
          }
          public String description() {
            return "queue size stat was expected to be " + expectedNumber + " but was instead " + clientProxy.getQueueSizeStat();
          }
        };
        DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
        
        assertTrue(clientProxy.getQueueSizeStat() == expectedNumber || clientProxy.getQueueSizeStat() == remaining);
      }
    });
  }
  
  protected void checkNumDurableCqs(VM server, final String durableClientId, final int expectedNumber) {
    server.invoke(new CacheSerializableRunnable(
        "check number of durable cqs on server for durable client: " + durableClientId){
        public void run2() throws CacheException {
          try {
            final CacheClientNotifier ccnInstance = CacheClientNotifier
                .getInstance();
            final CacheClientProxy clientProxy = ccnInstance
                .getClientProxy(durableClientId);
            ClientProxyMembershipID proxyId = clientProxy.getProxyID();
            CqService cqService = ((InternalCache)CacheServerTestUtil.getCache()).getCqService();
            cqService.start();
            List<String> cqNames = cqService.getAllDurableClientCqs(proxyId);
            assertEquals(expectedNumber, cqNames.size());
          }
          catch (Exception e) {
            throw new CacheException(e){};
          }
        }
    });
  }
  
  /*
   * @param vm
   * @param cqName
   * @param numEvents
   * @param numEventsToWaitFor most times will be the same as numEvents,
            but there are times where we want to wait for an event we know is 
            not coming just to be sure an event actually isnt received
   * @param secondsToWait
   */
  protected void checkCqListenerEvents(VM vm, final String cqName, final int numEvents, final int numEventsToWaitFor, final int secondsToWait) {
    vm.invoke(new CacheSerializableRunnable("Verify events for cq: " + cqName) {
      public void run2() throws CacheException {
        QueryService qs = CacheServerTestUtil.getCache().getQueryService();
        CqQuery cq = qs.getCq(cqName);
        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlCqListener listener = (CacheServerTestUtil.ControlCqListener) cq.getCqAttributes().getCqListener();
        listener.waitWhileNotEnoughEvents(secondsToWait * 1000, numEventsToWaitFor);
        assertEquals(numEvents, listener.events.size());
      }
    });
  }
  
  protected void checkInterestEvents(VM vm, final String regionName, final int numEvents) {
    vm.invoke(new CacheSerializableRunnable("Verify interest events") {
      public void run2() throws CacheException {
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);

        CacheServerTestUtil.ControlListener clistener = (CacheServerTestUtil.ControlListener) region
            .getAttributes().getCacheListeners()[0];
        clistener.waitWhileNotEnoughEvents(30000, numEvents);
        assertEquals(numEvents, clistener.events.size());      
        }
    });
  }
  
  protected void startDurableClient(VM vm, String durableClientId, int serverPort1, String regionName, int durableTimeoutInSeconds) {
    vm.invoke(
        CacheServerTestUtil.class,
        "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()),
                serverPort1, true), regionName,
            getClientDistributedSystemProperties(durableClientId, durableTimeoutInSeconds),
            Boolean.TRUE });
  }
  
  protected void startDurableClient(VM vm, String durableClientId, int serverPort1, String regionName) {
    vm.invoke(
        CacheServerTestUtil.class,
        "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()),
                serverPort1, true), regionName,
            getClientDistributedSystemProperties(durableClientId),
            Boolean.TRUE });
  }
  
  protected void startDurableClient(VM vm, String durableClientId, int serverPort1, int serverPort2, String regionName) {
    vm.invoke(
        CacheServerTestUtil.class,
        "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(vm.getHost()),
                serverPort1, serverPort2, true), regionName,
            getClientDistributedSystemProperties(durableClientId),
            Boolean.TRUE });
  }
  
  protected void startClient(VM vm, int serverPort1, String regionName) {
    vm.invoke(
        CacheServerTestUtil.class,
        "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(vm.getHost()),
                serverPort1, false), regionName });
  }
  
  protected void startClient(VM vm, int serverPort1, int serverPort2, String regionName) {
    vm.invoke(
        CacheServerTestUtil.class,
        "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(vm.getHost()),
                serverPort1, serverPort2, false), regionName });
  }
  
  protected void verifyDurableClientOnServer(VM server, final String durableClientId) {
    server.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });
  }
  
  protected void checkPrimaryUpdater(VM vm) {
    vm.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        WaitCriterion  wc = new WaitCriterion() {
          public boolean done() {
            return CacheServerTestUtil.getPool().isPrimaryUpdaterAlive();
          }
          public String description() {
            return "No primary updater";
          }
        };
        DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
        assertTrue(CacheServerTestUtil.getPool().isPrimaryUpdaterAlive());
      }
    });
  }
  
  protected void closeCache(VM vm) {
    vm.invoke(CacheServerTestUtil.class, "closeCache");
  }
}
