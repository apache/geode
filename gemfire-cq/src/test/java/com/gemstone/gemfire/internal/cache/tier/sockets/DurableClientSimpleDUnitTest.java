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

import java.util.Iterator;

import org.junit.Ignore;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerRefusedConnectionException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.ClientServerObserver;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

public class DurableClientSimpleDUnitTest extends DurableClientTestCase {

  public DurableClientSimpleDUnitTest(String name) {
    super(name);
  }
  /**
   * Test that a durable client correctly receives updates.
   */
  public void testSimpleDurableClientUpdate() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is not kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

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
        new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

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

  /**
   * Test that a durable client VM with multiple BridgeClients correctly
   * registers on the server.
   */
  public void testMultipleBridgeClientsInSingleDurableVM() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client with 2 regions (and 2 BridgeClients) that is not
    // kept alive on the server when it stops normally
    final String durableClientId = getName() + "_client";
    final String regionName1 = regionName + "1";
    final String regionName2 = regionName + "2";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClients", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName1, regionName2, getClientDistributedSystemProperties(durableClientId)});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        assertEquals(2, PoolManager.getAll().size());
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {       
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor()
            .getCacheClientNotifier();
        
        // Iterate the CacheClientProxies
        checkNumberOfClientProxies(2);
        String firstProxyRegionName = null;
        for (Iterator i = notifier.getClientProxies().iterator(); i.hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) i.next();
          assertTrue(proxy.isDurable());
          assertEquals(durableClientId, proxy.getDurableId());
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
          
          // Verify the two HA region names aren't the same
          if (firstProxyRegionName == null) {
            firstProxyRegionName = proxy.getHARegionName();
          } else {
            assertTrue(!firstProxyRegionName.equals(proxy.getHARegionName()));
          }
        }
      }
    });
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Verify the durable client is no longer on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(0);
      }
    });
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Test that a second VM with the same durable id cannot connect to the server
   * while the first VM is connected. Also, verify that the first client is not
   * affected by the second one attempting to connect.
   */
  public void XtestMultipleVMsWithSameDurableId() {
    // Start a server
    final int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

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
        region.registerInterestRegex(".*", InterestResultPolicy.NONE);
      }
    });

    // Attempt to start another durable client VM with the same id.
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Create another durable client") {
      public void run2() throws CacheException {
        getSystem(getClientDistributedSystemProperties(durableClientId));
        PoolFactoryImpl pf = (PoolFactoryImpl)PoolManager.createFactory();
        pf.init(getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), serverPort, true));
        try {
          pf.create("uncreatablePool");
          fail("Should not have been able to create the pool");
        } catch (ServerRefusedConnectionException e) {
          // expected exception
          disconnectFromDS();
        } catch (Exception e) {
          Assert.fail("Should not have gotten here", e);
        }
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
    
    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

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
    
    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  /**
   * Test that the server correctly processes starting two durable clients.
   */
  public void testSimpleTwoDurableClients() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId)});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'publisherClientVM' as a durable client.
    VM durableClient2VM = this.publisherClientVM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClient2VM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId2)});
    
    // Send clientReady message
    durableClient2VM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor()
            .getCacheClientNotifier();
        
        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found=false, durableClient2Found=false;
        for (Iterator i = notifier.getClientProxies().iterator(); i.hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) i.next();
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
        assertTrue(durableClient2Found);
      }
    });
    
    // Stop the durable clients
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    durableClient2VM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Test that starting a durable client on multiple servers (one live and one
   * not live) is processed correctly.
   */
  @Ignore("Disabled for bug 52043")
  public void DISABLED_testDurableClientMultipleServersOneLive() {
    // Start server 1
    final int server1Port = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start server 2
    final int server2Port = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
    
    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), server1Port, server2Port, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

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

    // Verify durable client on server1
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

    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), server1Port, server2Port, false), regionName});

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
        assertEquals("Events were" + listener.events, numberOfEntries, listener.events.size());
      }
    });
    
    try {
      java.lang.Thread.sleep(10000);
    }
    catch (InterruptedException ex) {
      fail("interrupted");
    }
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
    
    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
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
    
    // Re-start the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), server1Port, server2Port, true), regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

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
      }
    });
        
    // Verify the durable client received the updates held for it on the server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Get the listener and wait for the appropriate number of events
        CacheServerTestUtil.ControlListener listener = (CacheServerTestUtil.ControlListener) region
                .getAttributes().getCacheListeners()[0];
        listener.waitWhileNotEnoughEvents(30000, numberOfEntries);
        assertEquals("Events were" + listener.events, numberOfEntries, listener.events.size());
      }
    });
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
    
  /**
   * Test that updates to two durable clients are processed correctly.
   */
  public void testTwoDurableClientsStartStopUpdate() {
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
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

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
    
    // Start another durable client that is not kept alive on the server when
    // it stops normally. Use the 'server2VM' as the second durable client.
    VM durableClient2VM = this.server2VM;
    final String durableClientId2 = getName() + "_client2";
    durableClient2VM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClient2VM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId2, durableClientTimeout), Boolean.TRUE});
    
    // Send clientReady message
    durableClient2VM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Have the durable client register interest in all keys
    durableClient2VM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      }
    });
    
    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor()
            .getCacheClientNotifier();
        
        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found=false, durableClient2Found=false;
        for (Iterator i = notifier.getClientProxies().iterator(); i.hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) i.next();
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
        assertTrue(durableClient2Found);
      }
    });
    
    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

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
    
    // Verify durable client 1 received the updates
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
    
    // Verify durable client 2 received the updates
    durableClient2VM.invoke(new CacheSerializableRunnable("Verify updates") {
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
    
    // ARB: Wait for queue ack to arrive at server.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
    
    // Stop the durable clients
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
    durableClient2VM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
    
    // Verify the durable clients still exist on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor()
            .getCacheClientNotifier();
        
        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(2);
        boolean durableClient1Found=false, durableClient2Found=false;
        for (Iterator i = notifier.getClientProxies().iterator(); i.hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) i.next();
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          if (proxy.getDurableId().equals(durableClientId2)) {
            durableClient2Found = true;
          }
          assertEquals(durableClientTimeout, proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
        assertTrue(durableClient2Found);
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
      java.lang.Thread.sleep(1000);
    }
    catch (java.lang.InterruptedException ex) {
      fail("interrupted");
    }

    // Verify the durable clients' queues contain the entries
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor()
            .getCacheClientNotifier();
        
        // Iterate the CacheClientProxies and verify the queue sizes
        checkNumberOfClientProxies(2);
//        boolean durableClient1Found=false, durableClient2Found=false;
        for (Iterator i = notifier.getClientProxies().iterator(); i.hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) i.next();
          assertEquals(numberOfEntries, proxy.getQueueSize());
        }
      }
    });

    // Re-start durable client 1
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Re-start durable client 2
    durableClient2VM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClient2VM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId2), Boolean.TRUE});

    // Send clientReady message
    durableClient2VM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client 1 received the updates held for it on the server
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
    
    // Verify durable client 2 received the updates held for it on the server
    durableClient2VM.invoke(new CacheSerializableRunnable("Verify updates") {
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
    
    // Stop durable client 1
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop durable client 2
    durableClient2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Tests whether a durable client reconnects properly to two servers.
   */
  public void testDurableClientReconnectTwoServers() {
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    
    // on test flag for periodic ack
    this.server1VM.invoke(DurableClientTestCase.class, "setTestFlagToVerifyActForMarker",
        new Object[] { new Boolean(true) });
    
    final int server1Port = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int server2Port = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
        
    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), server1Port, server2Port, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

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
        region.registerInterestRegex(".*", InterestResultPolicy.NONE,true);
      }
    });
    
    // Verify durable client on server 1
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
        verifyReceivedMarkerAck(proxy);
      }
    });

    // VJR: wait for ack to go out
    Wait.pause(5000);

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
    
    // Verify durable client on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Re-start server2
    this.server2VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] { regionName, new Boolean(true),
            new Integer(server2Port)});
        
    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), server1Port, server2Port, false), regionName});

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

    try {
      java.lang.Thread.sleep(1000);
    }
    catch (java.lang.InterruptedException ex) {
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

    // Re-start the durable client that is kept alive on the server when it stops
    // normally
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), server1Port, server2Port, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client on server 1
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

    // Verify durable client on server 2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
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

    // Verify the HA region names are the same on both servers
    String server1HARegionQueueName= (String) this.server1VM.invoke(DurableClientTestCase.class, "getHARegionQueueName");
    String server2HARegionQueueName= (String) this.server2VM.invoke(DurableClientTestCase.class, "getHARegionQueueName");
    assertEquals(server1HARegionQueueName, server2HARegionQueueName);
    
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

    // off test flag for periodic ack
    this.server1VM.invoke(DurableClientTestCase.class, "setTestFlagToVerifyActForMarker",
        new Object[] { new Boolean(false) });
    
    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  public void testReadyForEventsNotCalledImplicitly() {
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    // make the client use ClientCacheFactory so it will have a default pool
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createClientCache", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), serverPort, true), regionName, getClientDistributedSystemProperties(durableClientId)});

    // verify that readyForEvents has not yet been called on the client's default pool
    this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
      public void run2() throws CacheException {
        for (Pool p: PoolManager.getAll().values()) {
          assertEquals(false, ((PoolImpl)p).getReadyForEventsCalled());
        }
      }
    });
    
    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable clients on server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Get the CacheClientNotifier
        CacheClientNotifier notifier = getBridgeServer().getAcceptor()
            .getCacheClientNotifier();
        
        // Iterate the CacheClientProxies and verify they are correct
        checkNumberOfClientProxies(1);
        boolean durableClient1Found=false, durableClient2Found=false;
        for (Iterator i = notifier.getClientProxies().iterator(); i.hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) i.next();
          assertTrue(proxy.isDurable());
          if (proxy.getDurableId().equals(durableClientId)) {
            durableClient1Found = true;
          }
          assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
        }
        assertTrue(durableClient1Found);
      }
    });
    
    // Stop the durable clients
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  //This test method is disabled because it is failing
  //periodically and causing cruise control failures
  //See bug #47060
  public void testReadyForEventsNotCalledImplicitlyWithCacheXML() {
    try {
      setPeriodicACKObserver(durableClientVM);
      final String cqName = "cqTest";
      // Start a server
      int serverPort = (Integer) this.server1VM.invoke(CacheServerTestUtil.class, "createCacheServerFromXml", new Object[]{ DurableClientTestCase.class.getResource("durablecq-server-cache.xml")});
  
      // Start a durable client that is not kept alive on the server when it
      // stops normally
      final String durableClientId = getName() + "_client";
      
      //create client cache from xml
      this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClientFromXml", new Object[]{ DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client", durableClientId, 45, Boolean.FALSE});
  
      // verify that readyForEvents has not yet been called on all the client's pools
      this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
        public void run2() throws CacheException {
          for (Pool p: PoolManager.getAll().values()) {
            assertEquals(false, ((PoolImpl)p).getReadyForEventsCalled());
          }
        }
      });
      
      // Send clientReady message
      this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
        public void run2() throws CacheException {
          CacheServerTestUtil.getCache().readyForEvents();
        }
      });
      
      //Durable client registers durable cq on server
      this.durableClientVM.invoke(new CacheSerializableRunnable("Register Cq") {
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);
          
          // Create CQ Attributes.
          CqAttributesFactory cqAf = new CqAttributesFactory();
          
          // Initialize and set CqListener.
          CqListener[] cqListeners = { new CacheServerTestUtil.ControlCqListener() };
          cqAf.initCqListeners(cqListeners);
          CqAttributes cqa = cqAf.create();
  
          // Create cq's
          // Get the query service for the Pool
          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
  
          try { 
            CqQuery query = queryService.newCq(cqName , "Select * from /" + regionName, cqa, true);
            query.execute();
          }
          catch (CqExistsException e) {
            fail("Failed due to " + e);
          }
          catch (CqException e) {
            fail("Failed due to " + e);
          }
          catch (RegionNotFoundException e) {
            fail("Could not find specified region:" + regionName + ":" + e);
          }
        }
      });
  
      // Verify durable client on server1
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        public void run2() throws CacheException {
          // Find the proxy
          checkNumberOfClientProxies(1);
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);
  
          // Verify that it is durable
          assertTrue(proxy.isDurable());
          assertEquals(durableClientId, proxy.getDurableId());
        }
      });
      
      // Start normal publisher client
      this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
          new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});
  
      // Publish some entries
      final int numberOfEntries = 10;
      this.publisherClientVM.invoke(new CacheSerializableRunnable("publish updates") {
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
          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
          CqQuery cqQuery = queryService.getCq(cqName);
          CacheServerTestUtil.ControlCqListener cqlistener = (CacheServerTestUtil.ControlCqListener) cqQuery.getCqAttributes().getCqListener();
          cqlistener.waitWhileNotEnoughEvents(30000, numberOfEntries);
          assertEquals(numberOfEntries, cqlistener.events.size());
        }
      });
      
       try {
        Thread.sleep(10000);
      }
      catch (InterruptedException e) {
        fail("interrupted" + e);
      }
      
      // Stop the durable client
      this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
      
      // Verify the durable client still exists on the server
      this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
        public void run2() throws CacheException {
          // Find the proxy
          CacheClientProxy proxy = getClientProxy();
          assertNotNull(proxy);
        }
      });
  
      // Publish some more entries
      this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish additional updates") {
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);
  
          // Publish some entries
          for (int i=0; i<numberOfEntries; i++) {
            String keyAndValue = String.valueOf(i);
            region.put(keyAndValue, keyAndValue + "lkj");
          }
        }
      });
      
      this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
      
      // Re-start the durable client
      this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClientFromXml", new Object[]{ DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client", durableClientId, 45,  Boolean.FALSE});
  
      
      //Durable client registers durable cq on server
      this.durableClientVM.invoke(new CacheSerializableRunnable("Register cq") {
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);
  
          // Create CQ Attributes.
          CqAttributesFactory cqAf = new CqAttributesFactory();
          
          // Initialize and set CqListener.
          CqListener[] cqListeners = { new CacheServerTestUtil.ControlCqListener() };
          cqAf.initCqListeners(cqListeners);
          CqAttributes cqa = cqAf.create();
  
          // Create cq's
          // Get the query service for the Pool
          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
  
          try { 
            CqQuery query = queryService.newCq(cqName , "Select * from /" + regionName, cqa, true);
            query.execute();
          }
          catch (CqExistsException e) {
            fail("Failed due to " + e);
          }
          catch (CqException e) {
            fail("Failed due to " + e);
          }
          catch (RegionNotFoundException e) {
            fail("Could not find specified region:" + regionName + ":" + e);
          }
         
        }
      });
      
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
        }
      });
          
      // Verify the durable client received the updates held for it on the server
      this.durableClientVM.invoke(new CacheSerializableRunnable("Verify updates") {
        public void run2() throws CacheException {
          // Get the region
          Region region = CacheServerTestUtil.getCache().getRegion(regionName);
          assertNotNull(region);
  
          QueryService queryService = CacheServerTestUtil.getPool().getQueryService();
  
          CqQuery cqQuery = queryService.getCq(cqName);
          
          CacheServerTestUtil.ControlCqListener cqlistener = (CacheServerTestUtil.ControlCqListener) cqQuery.getCqAttributes().getCqListener();
          cqlistener.waitWhileNotEnoughEvents(30000, numberOfEntries);
          assertEquals(numberOfEntries, cqlistener.events.size());
        }
      });
      
      // Stop the durable client
      this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  
      // Stop the server
      this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    }finally{
      unsetPeriodicACKObserver(durableClientVM);
    }
  }
  
  private void setPeriodicACKObserver(VM vm){
    CacheSerializableRunnable cacheSerializableRunnable = new CacheSerializableRunnable("Set ClientServerObserver"){
      @Override
      public void run2() throws CacheException {
        PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = true;
        ClientServerObserver origObserver = ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
          public void beforeSendingClientAck()
          {
            LogWriterUtils.getLogWriter().info("beforeSendingClientAck invoked");
           
          }
        });
        
      }
    };
    vm.invoke(cacheSerializableRunnable);
  }
  
  private void unsetPeriodicACKObserver(VM vm){
    CacheSerializableRunnable cacheSerializableRunnable = new CacheSerializableRunnable("Unset ClientServerObserver"){
      @Override
      public void run2() throws CacheException {
        PoolImpl.BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = false;        
      }
    };
    vm.invoke(cacheSerializableRunnable);
  }
  
  public void testReadyForEventsNotCalledImplicitlyForRegisterInterestWithCacheXML() {
    final String cqName = "cqTest";
    regionName = "testReadyForEventsNotCalledImplicitlyWithCacheXML_region";
    // Start a server
    int serverPort = (Integer) this.server1VM.invoke(CacheServerTestUtil.class, "createCacheServerFromXmlN", new Object[]{ DurableClientTestCase.class.getResource("durablecq-server-cache.xml")});

    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    
    //create client cache from xml
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClientFromXmlN", new Object[]{ DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client", durableClientId, 45, Boolean.TRUE});

    // verify that readyForEvents has not yet been called on all the client's pools
    this.durableClientVM.invoke(new CacheSerializableRunnable("check readyForEvents not called") {
      public void run2() throws CacheException {
        for (Pool p: PoolManager.getAll().values()) {
          assertEquals(false, ((PoolImpl)p).getReadyForEventsCalled());
        }
      }
    });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });
    
    //Durable client registers durable cq on server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register Interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);
        
        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, true);
      }
    });

    // Verify durable client on server1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);

        // Verify that it is durable
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
      }
    });
    
    // Start normal publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(publisherClientVM.getHost()), serverPort, false), regionName});

    // Publish some entries
    final int numberOfEntries = 10;
    this.publisherClientVM.invoke(new CacheSerializableRunnable("publish updates") {
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
    try {
      Thread.sleep(10000);
    }
    catch (InterruptedException e) {
      fail("interrupted" + e);
    }
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
    
    // Verify the durable client still exists on the server
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Publish some more entries
    this.publisherClientVM.invoke(new CacheSerializableRunnable("Publish additional updates") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Publish some entries
        for (int i=0; i<numberOfEntries; i++) {
          String keyAndValue = String.valueOf(i);
          region.put(keyAndValue, keyAndValue + "lkj");
        }
      }
    });
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Re-start the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClientFromXmlN", new Object[]{ DurableClientTestCase.class.getResource("durablecq-client-cache.xml"), "client", durableClientId, 45,  Boolean.TRUE});

    
    //Durable client registers durable cq on server
    this.durableClientVM.invoke(new CacheSerializableRunnable("Register interest") {
      public void run2() throws CacheException {
        // Get the region
        Region region = CacheServerTestUtil.getCache().getRegion(regionName);
        assertNotNull(region);

        // Register interest in all keys
        region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES, true);
      }
    });
    
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
      }
    });
        
    // Verify the durable client received the updates held for it on the server
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

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Tests the ha queued events stat
   * Connects a durable client, registers durable cqs and then shuts down the durable client
   * Publisher then does puts onto the server
   * Events are queued up and the stats are checked
   * Durable client is then reconnected, events are dispatched and stats are rechecked
   */
  public void testHAQueueSizeStat() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    final String durableClientId = getName() + "_client";
  
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    //verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkHAQueueSize(server1VM, durableClientId, 10, 11);

    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
  
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    //This can cause events to linger in the queue due to a "later" ack and only cleared on
    //the next dispatch.  We need to send one more message to dispatch, that calls remove one more
    //time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    checkHAQueueSize(server1VM, durableClientId, 0, 1);
        
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  

  /**
   * Tests the ha queued events stat
   * Connects a durable client, registers durable cqs and then shuts down the durable client
   * Publisher then does puts onto the server
   * Events are queued up and the stats are checked
   * Test sleeps until durable client times out
   * Stats should now be 0
   * Durable client is then reconnected, no events should exist and stats are rechecked
   */
  public void testHAQueueSizeStatExpired() throws Exception {
    int timeoutInSeconds = 20;
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    final int mcastPort = ports[1].intValue();
    
    final String durableClientId = getName() + "_client";
  
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName, timeoutInSeconds);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    //verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkHAQueueSize(server1VM, durableClientId, 10, 11);

    //pause until timeout
    try {
      Thread.sleep((timeoutInSeconds + 2) * 1000);
    }
    catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
  
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    
    //Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    //This can cause events to linger in the queue due to a "later" ack and only cleared on
    //the next dispatch.  We need to send one more message to dispatch, that calls remove one more
    //time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    checkHAQueueSize(server1VM, durableClientId, 0, 1);

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Tests the ha queued events stat
   * Starts up two servers, shuts one down
   * Connects a durable client, registers durable cqs and then shuts down the durable client
   * Publisher then does puts onto the server
   * Events are queued up
   * Durable client is then reconnected but does not send ready for events
   * Secondary server is brought back up
   * Stats are checked
   * Durable client then reregisters cqs and sends ready for events
   */
  public void testHAQueueSizeStatForGII() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
  
    //shut down server 2
    closeCache(server2VM);
    
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
  
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
  
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    verifyDurableClientOnServer(server1VM, durableClientId);
    checkNumDurableCqs(server1VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
    
    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
    
    // Re-start server2, at this point it will be the first time server2 has connected to client
    this.server2VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] { regionName, new Boolean(true),
            new Integer(serverPort2)});
    
    // Verify durable client on server2
    verifyDurableClientOnServer(server2VM, durableClientId);
    
    //verify cqs and stats on server 2.  These events are through gii, stats should be correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkHAQueueSize(server2VM, durableClientId, 10, 11);
    
    closeCache(server1VM);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    //verify cq listeners received events
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);
    checkHAQueueSize(server2VM, durableClientId, 0, 1);
    
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the servers
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Tests the ha queued cq stat
   */
  public void testHAQueuedCqStat() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    final int mcastPort = ports[1].intValue();
    
    final String durableClientId = getName() + "_client";
  
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    //verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
  
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    

    //Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    //This can cause events to linger in the queue due to a "later" ack and only cleared on
    //the next dispatch.  We need to send one more message to dispatch, that calls remove one more
    //time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);
        
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  /**
   * @throws Exception
   */
  public void testHAQueuedCqStatOnSecondary() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
    
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
  
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
  
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    //Verify durable client on server 2
    verifyDurableClientOnServer(server2VM, durableClientId);
    
    //Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
  
    //Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
       
    //verify cq stats are correct on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);
    
    //verify cq stats are correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);
      
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
      
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Verify stats are 0 for both servers
    flushEntries(server1VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);
        
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  
  /**
   * Server 2 comes up, client connects and registers cqs, server 2 then disconnects
   * events are put into region
   * client goes away
   * server 2 comes back up and should get a gii
   * check stats
   * server 1 goes away
   * client comes back and receives all events
   * stats should still be correct
   * 
   * @throws Exception
   */
  public void testHAQueuedCqStatForGII() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
  
    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
  
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
  
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    //Verify durable client on both servers
    verifyDurableClientOnServer(server2VM, durableClientId);
    verifyDurableClientOnServer(server1VM, durableClientId);
    
    //verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);
    
    //shutdown server 2
    closeCache(server2VM);
    
    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
    
    // Re-start server2, should get events through gii
    this.server2VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] { regionName, new Boolean(true),
            new Integer(serverPort2)});
 
    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    //verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);
    
    closeCache(server1VM);

    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);

   
    // Stop the durable clients
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the servers
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  

  /**
   * Start both servers, but shut down secondary server before durable client has
   * connected.  
   * Connect durable client to primary, register cqs and then shutdown durable client
   * Publish events, reconnect durable client but do not send ready for events
   * Restart secondary and check stats to be sure cqs have correct stats due to GII
   * Shutdown primary and fail over to secondary
   * Durable Client sends ready or events and receives events
   * Recheck stats
   */
  public void testHAQueuedCqStatForGII2() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
  
    //shut down server 2
    closeCache(server2VM);
    
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
  
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
  
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    verifyDurableClientOnServer(server1VM, durableClientId);
    checkNumDurableCqs(server1VM, durableClientId, 3);

    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
    
    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
    
    // Re-start server2, at this point it will be the first time server2 has connected to client
    this.server2VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] { regionName, new Boolean(true),
            new Integer(serverPort2)});
    
    // Verify durable client on server2
    verifyDurableClientOnServer(server2VM, durableClientId);
    
    //verify cqs and stats on server 2.  These events are through gii, stats should be correct
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);
    
    closeCache(server1VM);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the servers
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  /**
   * Server 2 comes up and goes down after client connects and registers cqs
   * events are put into region
   * client goes away
   * server 2 comes back up and should get a gii
   * check stats
   * client comes back and receives all events
   * stats should still be correct
   * 
   * @throws Exception
   */
  public void testHAQueuedCqStatForGIINoFailover() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    // Start server 2
    final int serverPort2 = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
  
    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
  
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
  
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    //Verify durable client on both servers
    verifyDurableClientOnServer(server2VM, durableClientId);
    verifyDurableClientOnServer(server1VM, durableClientId);
    
    //verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);
    
    //shutdown server 2
    closeCache(server2VM);
    
    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
    
    // Re-start server2, should get events through gii
    this.server2VM.invoke(CacheServerTestUtil.class, "createCacheServer",
        new Object[] { regionName, new Boolean(true),
            new Integer(serverPort2)});
 
    // Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);

    //verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Verify stats are 0 for server2 (we failed over)
    flushEntries(server2VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);

    // Stop the durable clients
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop the servers
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * server 1 and 2 both get events
   * server 1 goes down
   * dc reconnects
   * server 2 behaves accordingly
   * @throws Exception
   */
  public void testHAQueuedCqStatForFailover() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int serverPort2 = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
  
    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
  
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
  
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    //Verify durable client on both servers
    verifyDurableClientOnServer(server2VM, durableClientId);
    verifyDurableClientOnServer(server1VM, durableClientId);
    
    //verify durable cqs on both servers
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkNumDurableCqs(server2VM, durableClientId, 3);
    
    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
    
    closeCache(server1VM);

    //verify cq stats are correct on server 2
    checkNumDurableCqs(server2VM, durableClientId, 3);
    checkCqStatOnServer(server2VM, durableClientId, "All", 10);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 5);
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, serverPort2, regionName);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
      
    //verify listeners on client
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    
    //Verify stats are 0 for both servers
    flushEntries(server2VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server2VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server2VM, durableClientId, "All", 0);
   
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  
  /**
   * Tests the ha queued cq stat
   */
  public void testHAQueuedCqStatWithTimeOut() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    int timeoutInSeconds = 20;
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    final String durableClientId = getName() + "_client";
  
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName, timeoutInSeconds);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    //verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);

    //Pause for timeout
    try {
      Thread.sleep((timeoutInSeconds + 5) * 1000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
  
    //Make sure all events are expired and are not sent
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    
    //Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    //This can cause events to linger in the queue due to a "later" ack and only cleared on
    //the next dispatch.  We need to send one more message to dispatch, that calls remove one more
    //time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);
        
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server
   * @throws Exception
   */
  public void testCloseCqAndDrainEvents() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(
        CacheServerTestUtil.class, "createCacheServer", new Object[] {
            regionName, new Boolean(true) })).intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    
    this.server1VM.invoke(new CacheSerializableRunnable(
        "Close cq for durable client") {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
        }
        catch (CqException e) {
          throw new CacheException(e){};
        }
      }
    });
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    //verify cq events for all 3 cqs
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
   

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server
   * This draining should not affect events that still have register interest
   * @throws Exception
   */
  public void testCloseAllCqsAndDrainEvents() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    final String durableClientId = getName() + "_client";
  
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    registerInterest(durableClientVM, regionName, true);
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    
    this.server1VM.invoke(new CacheSerializableRunnable(
        "Close cq for durable client") {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          ccnInstance.closeClientCq(durableClientId, "GreaterThan5");
          ccnInstance.closeClientCq(durableClientId, "LessThan5");
        }
        catch (CqException e) {
          throw new CacheException(e){};
        }
      }
    });
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    registerInterest(durableClientVM, regionName, true);
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkInterestEvents(durableClientVM, regionName, 10);
   
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server
   * This draining should remove all events due to no interest registered
   * Continues to publish afterwards to verify that stats are correct
   * @throws Exception
   */
  public void testCloseAllCqsAndDrainEventsNoInterestRegistered() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    final String durableClientId = getName() + "_client";
  
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
      
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);

    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    
    this.server1VM.invoke(new CacheSerializableRunnable(
        "Close cq for durable client") {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          ccnInstance.closeClientCq(durableClientId, "GreaterThan5");
          ccnInstance.closeClientCq(durableClientId, "LessThan5");
        }
        catch (CqException e) {
          throw new CacheException(e){};
        }
      }
    });
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    //Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    //This can cause events to linger in the queue due to a "later" ack and only cleared on
    //the next dispatch.  We need to send one more message to dispatch, that calls remove one more
    //time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    
    //the flush entry message may remain in the queue due
    //verify the queue stats are as close/correct as possible
    this.checkHAQueueSize(server1VM, durableClientId, 0, 1);
    
    
    //continue to publish and make sure we get the events
    publishEntries(publisherClientVM, regionName, 10);
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 10/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 10/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 10/*secondsToWait*/);

    //Due to the implementation of DurableHARegionQueue where remove is called after dispatch.
    //This can cause events to linger in the queue due to a "later" ack and only cleared on
    //the next dispatch.  We need to send one more message to dispatch, that calls remove one more
    //time and any remaining acks (with or without this final published events ack)
    flushEntries(server1VM, durableClientVM, regionName);
    
    //the flush entry message may remain in the queue due
    //verify the queue stats are as close/correct as possible
    this.checkHAQueueSize(server1VM, durableClientId, 0, 1);
    
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  
  /**
   * Test functionality to close the cq and drain all events from the ha queue from the server
   * Two durable clients, one will have a cq be closed, the other should be unaffected
   * @throws Exception
   */
  public void testCloseCqAndDrainEvents2Client() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int serverPort = ports[0].intValue();
    
    final String durableClientId = getName() + "_client";
    final String durableClientId2 = getName() + "_client2";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);

    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);

    // Stop the durable client
    this.disconnectDurableClient(true);
    
    startDurableClient(durableClientVM, durableClientId2, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
    
    // Verify 2nd durable client on server
    this.server1VM
        .invoke(new CacheSerializableRunnable("Verify 2nd durable client") {
          public void run2() throws CacheException {
            // Find the proxy
            checkNumberOfClientProxies(2);
          }
        });
    
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
     
    
    this.server1VM.invoke(new CacheSerializableRunnable(
        "Close cq for durable client 1") {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
        }
        catch (CqException e) {
          throw new CacheException(e){};
        }
      }
    });
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    
    //verify cq events for all 3 cqs, where ALL should have 0 entries
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    
    this.disconnectDurableClient(false);
    
    // Restart the 2nd durable client
    startDurableClient(durableClientVM, durableClientId2, serverPort, regionName);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
    //send client ready
    sendClientReady(durableClientVM);
    
    //verify cq events for all 3 cqs, where ALL should have 10 entries
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  /**
   * Tests situation where a client is trying to reconnect while a cq is being drained.
   * The client should be rejected until no cqs are currently being drained
   * @throws Exception
   */
  public void testRejectClientWhenDrainingCq() throws Exception {
    try {
      IgnoredException.addIgnoredException(LocalizedStrings.CacheClientNotifier_COULD_NOT_CONNECT_DUE_TO_CQ_BEING_DRAINED.toLocalizedString());
      IgnoredException.addIgnoredException("Could not initialize a primary queue on startup. No queue servers available.");
      
      String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
      String allQuery = "select * from /" + regionName + " p where p.ID > -1";
      String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
          
      // Start server 1
      Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
          "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
      final int serverPort = ports[0].intValue();
  
      final String durableClientId = getName() + "_client";
      this.durableClientVM.invoke(CacheServerTestUtil.class, "disableShufflingOfEndpoints");
    
      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    
      //register durable cqs
      createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
      createCq(durableClientVM, "All", allQuery, true);
      createCq(durableClientVM, "LessThan5", lessThan5Query, true);
      //send client ready
      sendClientReady(durableClientVM);
  
      verifyDurableClientOnServer(server1VM, durableClientId);
  
      // Stop the durable client
      this.disconnectDurableClient(true);
      
      // Start normal publisher client
      startClient(publisherClientVM, serverPort, regionName);
    
      // Publish some entries
      publishEntries(publisherClientVM, regionName, 10);
              
      this.server1VM.invokeAsync(new CacheSerializableRunnable(
          "Close cq for durable client") {
        public void run2() throws CacheException {
  
          //Set the Test Hook!
          //This test hook will pause during the drain process
          CacheClientProxy.testHook = new RejectClientReconnectTestHook();
  
          final CacheClientNotifier ccnInstance = CacheClientNotifier
              .getInstance();
          final CacheClientProxy clientProxy = ccnInstance
              .getClientProxy(durableClientId);
          ClientProxyMembershipID proxyId = clientProxy.getProxyID();
  
          try {
            ccnInstance.closeClientCq(durableClientId, "All");
          }
          catch (CqException e) {
            throw new CacheException(e){};
          }
        }
      });
  
      // Restart the durable client
      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
  
      this.server1VM.invoke(new CacheSerializableRunnable(
          "verify was rejected at least once") {
        public void run2() throws CacheException {
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return  CacheClientProxy.testHook != null && (((RejectClientReconnectTestHook) CacheClientProxy.testHook).wasClientRejected());
            }
            public String description() {
              return null;
            }
          };
          Wait.waitForCriterion(ev, 10 * 1000, 200, true);
          assertTrue(((RejectClientReconnectTestHook) CacheClientProxy.testHook).wasClientRejected());
        }
      });
      
      checkPrimaryUpdater(durableClientVM);
      
      // After rejection, the client will retry and eventually connect
      // Verify durable client on server2
      verifyDurableClientOnServer(server1VM, durableClientId);
    
      createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
      createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
      createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
      //send client ready
      sendClientReady(durableClientVM);
  
      checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
      checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
      checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);      
  
      // Stop the durable client
      this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  
      // Stop the publisher client
      this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  
      // Stop the server
      this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    }finally{
      this.server1VM.invoke(new CacheSerializableRunnable(
          "unset test hook") {
        public void run2() throws CacheException {
          CacheClientProxy.testHook = null;
        }
      });
    }
  }
  
  
  /**
   * Tests scenario where close cq will throw an exception due to a client
   * being reactivated
   * @throws Exception
   */
  public void testCqCloseExceptionDueToActivatingClient() throws Exception {
    try {
      String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
      String allQuery = "select * from /" + regionName + " p where p.ID > -1";
      String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
          
      // Start server 1
      Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
          "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
      final int serverPort = ports[0].intValue();
      
      final String durableClientId = getName() + "_client";
    
      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
      //register durable cqs
      createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
      createCq(durableClientVM, "All", allQuery, true);
      createCq(durableClientVM, "LessThan5", lessThan5Query, true);
      //send client ready
      sendClientReady(durableClientVM);
        
      // Verify durable client on server
      verifyDurableClientOnServer(server1VM, durableClientId);
            
      // Stop the durable client
      this.disconnectDurableClient(true);
      
      // Start normal publisher client
      startClient(publisherClientVM, serverPort, regionName);
  
      // Publish some entries
      publishEntries(publisherClientVM, regionName, 10);
          
      
      AsyncInvocation async = this.server1VM.invokeAsync(new CacheSerializableRunnable(
          "Close cq for durable client") {
        public void run2() throws CacheException {
  
          //Set the Test Hook!
          //This test hook will pause during the drain process
          CacheClientProxy.testHook = new CqExceptionDueToActivatingClientTestHook();
          
          final CacheClientNotifier ccnInstance = CacheClientNotifier
              .getInstance();
          final CacheClientProxy clientProxy = ccnInstance
              .getClientProxy(durableClientId);
          ClientProxyMembershipID proxyId = clientProxy.getProxyID();
  
          try {
            ccnInstance.closeClientCq(durableClientId, "All");
            fail("Should have thrown an exception due to activating client");
          }
          catch (CqException e) {
            String expected = LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_RESTARTING_DURABLE_CLIENT.toLocalizedString("All", proxyId.getDurableId());
            if (!e.getMessage().equals(expected)) {            
             fail("Not the expected exception, was expecting " + (LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_RESTARTING_DURABLE_CLIENT.toLocalizedString("All", proxyId.getDurableId()) + " instead of exception: " + e.getMessage()));
            }
          }
        }
      });
      
      //Restart the durable client
      startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    
      //Reregister durable cqs
      createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
      createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
      createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);
      //send client ready
      sendClientReady(durableClientVM);
      
      async.join();
      assertEquals(async.getException() != null ? async.getException().toString(): "No error" ,false,  async.exceptionOccurred());
      
      //verify cq listener events
      checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
      checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
      checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);           
  
      // Stop the durable client
      this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  
      // Stop the publisher client
      this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  
      // Stop the server
      this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    }finally {
      this.server1VM.invoke(new CacheSerializableRunnable(
          "unset test hook") {
        public void run2() throws CacheException {
          CacheClientProxy.testHook = null;
        }
      });
    }
  }
  
  /**
   * Tests situation where a client is trying to reconnect while a cq is being drained
   * @throws Exception
   */
  public void testCqCloseExceptionDueToActiveConnection() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(
        CacheServerTestUtil.class, "createCacheServer", new Object[] {
            regionName, new Boolean(true) })).intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    sendClientReady(durableClientVM);

    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);

    verifyDurableClientOnServer(server1VM, durableClientId);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);
  
    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);

    //Attempt to close a cq even though the client is running
    this.server1VM.invoke(new CacheSerializableRunnable(
        "Close cq for durable client") {
      public void run2() throws CacheException {
        
        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();

        try {
          ccnInstance.closeClientCq(durableClientId, "All");
          fail("expected a cq exception.  We have an active client proxy, the close cq command should have failed");
        }
        catch (CqException e) {
          //expected exception;
          String expected = LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_ACTIVE_DURABLE_CLIENT.toLocalizedString("All", proxyId.getDurableId());
          if (!e.getMessage().equals(expected)) {
            fail("Not the expected exception, was expecting " + (LocalizedStrings.CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_ACTIVE_DURABLE_CLIENT.toLocalizedString("All", proxyId.getDurableId()) + " instead of exception: " + e.getMessage()));
          }
        }
      }
    });
    
    //verify cq events for all 3 cqs
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 4 /*numEventsExpected*/, 4/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 5 /*numEventsExpected*/, 5/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 10 /*numEventsExpected*/, 10/*numEventsToWaitFor*/, 15/*secondsToWait*/);
    

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  
  /**
   * Test functionality to close the durable client 
   * and drain all events from the ha queue from the server
   * @throws Exception
   */
  public void testCloseCacheProxy() throws Exception {
    String greaterThan5Query = "select * from /" + regionName + " p where p.ID > 5";
    String allQuery = "select * from /" + regionName + " p where p.ID > -1";
    String lessThan5Query = "select * from /" + regionName + " p where p.ID < 5";
        
    // Start a server
    int serverPort = ((Integer) this.server1VM.invoke(
        CacheServerTestUtil.class, "createCacheServer", new Object[] {
            regionName, new Boolean(true) })).intValue();

    // Start a durable client that is kept alive on the server when it stops
    // normally
    final String durableClientId = getName() + "_client";
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    //register durable cqs
    createCq(durableClientVM, "GreaterThan5", greaterThan5Query, true);
    createCq(durableClientVM, "All", allQuery, true);
    createCq(durableClientVM, "LessThan5", lessThan5Query, true);
    //send client ready
    sendClientReady(durableClientVM);
    
    // Verify durable client on server
    verifyDurableClientOnServer(server1VM, durableClientId);
          
    // Stop the durable client
    this.disconnectDurableClient(true);
    
    // Start normal publisher client
    startClient(publisherClientVM, serverPort, regionName);

    // Publish some entries
    publishEntries(publisherClientVM, regionName, 10);
   
    //verify cq stats are correct
    checkNumDurableCqs(server1VM, durableClientId, 3);
    checkCqStatOnServer(server1VM, durableClientId, "All", 10);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 4);
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 5);
    
    //drop client proxy
    this.server1VM.invoke(new CacheSerializableRunnable(
        "Close client proxy on server for client" + durableClientId) {
      public void run2() throws CacheException {

        final CacheClientNotifier ccnInstance = CacheClientNotifier
            .getInstance();
        final CacheClientProxy clientProxy = ccnInstance
            .getClientProxy(durableClientId);
        ClientProxyMembershipID proxyId = clientProxy.getProxyID();
        
        ccnInstance.closeDurableClientProxy(durableClientId);
      }
    });
    
    
    //Restart the durable client
    startDurableClient(durableClientVM, durableClientId, serverPort, regionName);
    
    //check that cqs are no longer registered
    checkNumDurableCqs(server1VM, durableClientId, 0);
    
    //Reregister durable cqs
    createCq(durableClientVM, "GreaterThan5", "select * from /" + regionName + " p where p.ID > 5", true);
    createCq(durableClientVM, "All", "select * from /" + regionName + " p where p.ID > -1", true);
    createCq(durableClientVM, "LessThan5", "select * from /" + regionName + " p where p.ID < 5", true);

   //Before sending client ready, lets make sure the stats already reflect 0 queued events
    checkCqStatOnServer(server1VM, durableClientId, "LessThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "GreaterThan5", 0);
    checkCqStatOnServer(server1VM, durableClientId, "All", 0);
    
    //send client ready
    sendClientReady(durableClientVM);
    
    //verify cq events for all 3 cqs are 0 events
    checkCqListenerEvents(durableClientVM, "GreaterThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "LessThan5", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);
    checkCqListenerEvents(durableClientVM, "All", 0 /*numEventsExpected*/, 1/*numEventsToWaitFor*/, 5/*secondsToWait*/);

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the publisher client
    this.publisherClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop the server
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
  }  
  
  /**
   * Test that starting a durable client on multiple servers is processed
   * correctly.
   */
  public void testSimpleDurableClientMultipleServers() {
    // Start server 1
    Integer[] ports = ((Integer[]) this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServerReturnPorts", new Object[] {regionName, new Boolean(true)}));
    final int server1Port = ports[0].intValue();
    
    // Start server 2 using the same mcast port as server 1
    final int server2Port = ((Integer) this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] {regionName, new Boolean(true)}))
        .intValue();
    
    // Start a durable client connected to both servers that is kept alive when
    // it stops normally
    final int durableClientTimeout = 60; // keep the client alive for 60 seconds
    //final boolean durableClientKeepAlive = true; // keep the client alive when it stops normally
    final String durableClientId = getName() + "_client";
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), server1Port, server2Port, true), regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client on server 1
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
    
    // Verify durable client on server 2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
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
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache", new Object[] {new Boolean(true)});
    
    // Verify the durable client is still on server 1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });
    
    // Verify the durable client is still on server 2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Start up the client again. This time initialize it so that it is not kept
    // alive on the servers when it stops normally.
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient", 
        new Object[] {getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), server1Port, server2Port, true), regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE});

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Verify durable client on server1
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
    
    // Verify durable client on server2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
        
        checkProxyIsAlive(proxy);
        
        // Verify that it is durable and its properties are correct
        assertTrue(proxy.isDurable());
        assertEquals(durableClientId, proxy.getDurableId());
        assertEquals(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT, proxy.getDurableTimeout());
      }
    });

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
    
    this.verifySimpleDurableClientMultipleServers();

    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    
    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");
  }

  
  
}
