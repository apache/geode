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

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author Deepkumar Varma
 * 
 * We have 2 servers and One client which registers some keys with durable
 * interest and some without it. We maintain queues on only One server as
 * redundancy level is one. Following 2 tests have the two TestCase scenarios
 * 
 * There are two Tests First Test does the follows : // Step 1: Starting the
 * servers // Step 2: Bring Up the Client // Step 3: Client registers Interests //
 * Step 4: Update Values on the Server for Keys // Step 5: Verify Updates on the
 * Client // Step 6: Close Cache of the DurableClient // Step 7: Update the
 * Values // Step 8: Re-start the Client // Step 9: Verify Updates on the Client //
 * Step 10 : Stop all VMs
 * 
 * For Test 2 the steps are as follows // Step 1: Starting the servers // Step
 * 2: Bring Up the Client // Step 3: Client registers Interests // Step 4:
 * Update Values on the Server for Keys // Step 5: Verify Updates on the Client //
 * Step 6: Close Cache of the DurableClient // Step 7: Update the Values // Step
 * 8: Re-start the Client // Step 9: Send Client Ready Message // Step 10:
 * Register all Keys (K1, K2 as Non-Durable. K3, K4 as Durable) // Step 11:
 * Unregister Some Keys (Here K1, K3) // Step 12: Modify values on the server
 * for all the Keys // Step 13: Check the values for the ones not unregistered
 * and the Unregistered Keys' Values should be null
 * 
 */
public class DurableRegistrationDUnitTest extends DistributedTestCase {

  private VM server1VM;

  private VM server2VM;

  private VM durableClientVM;

  private String regionName;

  private int PORT1;

  private int PORT2;

  private static final String K1 = "KEY_STONE1";

  private static final String K2 = "KEY_STONE2";

  private static final String K3 = "KEY_STONE3";

  private static final String K4 = "KEY_STONE4";

  public DurableRegistrationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    this.server1VM = host.getVM(0);
    this.server2VM = host.getVM(1);
    this.durableClientVM = host.getVM(2);
    regionName = DurableRegistrationDUnitTest.class.getName() + "_region";
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  public void testSimpleDurableClient() {

    // Step 1: Starting the servers
    PORT1 = ((Integer)this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
            })).intValue();
    PORT2 = ((Integer)this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true),
            })).intValue();

    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";

    final int durableClientTimeout = 600; // keep the client alive for 600
    // seconds
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 0),
            regionName,
            getClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Step 3: Client registers Interests
    // KEY_STONE1, KEY_STONE2 are registered as durableKeys & KEY_STONE3,
    // KEY_STONE4 as non-durableKeys

    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K2, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K3, new Boolean(true) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K4, new Boolean(true) });

    // Step 4: Update Values on the Server for KEY_STONE1, KEY_STONE2,
    // KEY_STONE3, KEY_STONE4

    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K1, "Value1" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K2, "Value2" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K3, "Value3" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K4, "Value4" });

    pause(1000);
    // Step 5: Verify Updates on the Client

    assertEquals("Value1", this.server2VM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));
    assertEquals("Value1", this.server1VM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));

    assertEquals("Value1", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));
    assertEquals("Value2", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K2 }));
    assertEquals("Value3", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K3 }));
    assertEquals("Value4", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K4 }));

    // Step 6: Close Cache of the DurableClient
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "closeCache");
    // pause(5000);
    // Step 7: Update KEY_STONE1,KEY_STONE2,KEY_STONE3,KEY_STONE4 on the
    // Server say with values PingPong1, PingPong2, PingPong3, PingPong4
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K1, "PingPong1" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K2, "PingPong2" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K3, "PingPong3" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K4, "PingPong4" });

    // Step 8: Re-start the Client
    this.durableClientVM
        .invoke(CacheServerTestUtil.class, "createCacheClient",
            new Object[] { getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 0),
                regionName,
                getClientDistributedSystemProperties(durableClientId),
                Boolean.TRUE });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    pause(5000);

    assertNull(this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "getValue", new Object[] { K1 }));
    assertNull(this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "getValue", new Object[] { K2 }));

    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });

    pause(5000);
    assertNull(this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "getValue", new Object[] { K1 }));
    assertNull(this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "getValue", new Object[] { K2 }));

    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K1, "PingPong_updated_1" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K2, "PingPong_updated_2" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K3, "PingPong_updated_3" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K4, "PingPong_updated_4" });

    pause(5000);

    // Step 9: Verify Updates on the Client
    assertEquals("PingPong_updated_1", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));
    assertNull(this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "getValue", new Object[] { K2 }));
    assertEquals("PingPong_updated_3", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K3 }));
    assertEquals("PingPong_updated_4", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K4 }));

    // Step 10 : Stop all VMs
    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

  }

  public void testSimpleDurableClientWithRegistration() {
    // Step 1: Starting the servers
    PORT1 = ((Integer)this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
            })).intValue();
    PORT2 = ((Integer)this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
            })).intValue();

    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final String durableClientId = getName() + "_client";
    // keep the client alive for 600 seconds
    final int durableClientTimeout = 600;
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 0),
            regionName,
            getClientDistributedSystemProperties(durableClientId,
                durableClientTimeout) });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Step 3: Client registers Interests
    // KEY_STONE1, KEY_STONE2 are registered as durableKeys & KEY_STONE3,
    // KEY_STONE4 as non-durableKeys

    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K2, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K3, new Boolean(true) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K4, new Boolean(true) });

    // Step 4: Update Values on the Server for KEY_STONE1, KEY_STONE2,
    // KEY_STONE3, KEY_STONE4

    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K1, "Value1" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K2, "Value2" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K3, "Value3" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K4, "Value4" });

    pause(1000);
    // Step 5: Verify Updates on the Client

    assertEquals("Value1", this.server2VM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));
    assertEquals("Value1", this.server1VM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));

    assertEquals("Value1", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));
    assertEquals("Value2", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K2 }));
    assertEquals("Value3", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K3 }));
    assertEquals("Value4", this.durableClientVM.invoke(
        DurableRegistrationDUnitTest.class, "getValue", new Object[] { K4 }));

    // Step 6: Close Cache of the DurableClient
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "closeCache");
    // pause(5000);
    // Step 7: Update KEY_STONE1,KEY_STONE2,KEY_STONE3,KEY_STONE4 on the
    // Server say with values PingPong1, PingPong2, PingPong3, PingPong4
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K1, "PingPong1" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K2, "PingPong2" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K3, "PingPong3" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K4, "PingPong4" });

    // Step 8: Re-start the Client
    this.durableClientVM
        .invoke(CacheServerTestUtil.class, "createCacheClient",
            new Object[] { getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 0),
                regionName,
                getClientDistributedSystemProperties(durableClientId),
                Boolean.TRUE });

    // Step 9: Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // pause(1000);

    // Step 10: Register all Keys
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K3, new Boolean(true) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K4, new Boolean(true) });

    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K2, new Boolean(false) });

    // Step 11: Unregister Some Keys (Here K1, K3)
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "unregisterKey", new Object[] { K1 });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "unregisterKey", new Object[] { K3 });

    pause(5000);

    // Step 12: Modify values on the server for all the Keys
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K1, "PingPong_updated_1" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K2, "PingPong_updated_2" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K3, "PingPong_updated_3" });
    this.server2VM.invoke(DurableRegistrationDUnitTest.class, "putValue",
        new Object[] { K4, "PingPong_updated_4" });

    pause(5000);

    // Step 13: Check the values for the ones not unregistered and the
    // Unregistered Keys' Values should be null
    try {
      assertEquals("PingPong_updated_2", this.durableClientVM.invoke(
          DurableRegistrationDUnitTest.class, "getValue", new Object[] { K2 }));
    }
    catch (Exception e) {
      fail("Prob in KEY_STONE2: "
          + this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
              "getValue", new Object[] { K2 }));
    }

    try {
      assertEquals("PingPong_updated_4", this.durableClientVM.invoke(
          DurableRegistrationDUnitTest.class, "getValue", new Object[] { K4 }));
    }
    catch (Exception e) {
      fail("Prob in KEY_STONE4: "
          + this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
              "getValue", new Object[] { K4 }));

    }

    try {
      assertNull(this.durableClientVM.invoke(
          DurableRegistrationDUnitTest.class, "getValue", new Object[] { K1 }));
    }
    catch (Exception e) {
      fail("Prob in KEY_STONE1: "
          + this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
              "getValue", new Object[] { K1 }));

    }

    try {
      assertNull(this.durableClientVM.invoke(
          DurableRegistrationDUnitTest.class, "getValue", new Object[] { K3 }));
    }
    catch (Exception e) {
      fail("Prob in KEY_STONE3: "
          + this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
              "getValue", new Object[] { K3 }));

    }

    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

  }

  public void testDurableClientWithRegistrationHA() {
    
    // Step 1: Start server1
    PORT2 = new Integer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    
    PORT1 = ((Integer)this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
             })).intValue();
    

    // Step 2: Bring Up the Client
    final String durableClientId = getName() + "_client";
    // keep the client alive for 600 seconds
    final int durableClientTimeout = 600;
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 1),
            regionName,
            getClientDistributedSystemProperties(durableClientId,
                durableClientTimeout) });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Step 3: Client registers Interests
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K2, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K3, new Boolean(true) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K4, new Boolean(true) });

    // Step 4: Bring up the server2

    this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true),
             PORT2 });

    pause(3000);

    // Check server2 got all the interests registered by the durable client.    
    server2VM.invoke(new CacheSerializableRunnable("Verify Interests.") {
      public void run2() throws CacheException
      {
        getLogWriter().info("### Verifying interests registered by DurableClient. ###");
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        CacheClientProxy p = null;
        
        // Get proxy for the client.        
        for (int i=0; i < 60; i++) {
          Iterator ps = ccn.getClientProxies().iterator();
          if (!ps.hasNext()) {
            pause(1000);
            continue;
          } else {
            p = (CacheClientProxy)ps.next();
            break;
          }
        }

        if (p == null) {
          fail("Proxy initialization taking long time. Increase the wait time.");
        }
        
        Iterator rs = p.getInterestRegisteredRegions().iterator();
        String rName = (String)rs.next();
        assertNotNull("Null region Name found.", rs);
        LocalRegion r = (LocalRegion)GemFireCacheImpl.getInstance().getRegion(rName);
        assertNotNull("Null region found.", r);
        FilterProfile pf = r.getFilterProfile();
        Set intrests = Collections.EMPTY_SET;
        
        Set interestKeys = pf.getKeysOfInterest(p.getProxyID().getDurableId());
        assertNotNull("durable Interests not found for the proxy", interestKeys);
        assertEquals("The number of durable keys registered during HARegion GII doesn't match.", interestKeys.size(), 2);
        interestKeys = pf.getKeysOfInterest(p.getProxyID());
        assertNotNull("non-durable Interests not found for the proxy", interestKeys);
        assertEquals("The number of non-durable keys registered during HARegion GII doesn't match.", interestKeys.size(), 2);
      }
    });


    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

  }

  public void testDurableClientDisConnectWithRegistrationHA() {
    
    // Step 1: Start server1
    PORT2 = new Integer(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    
    PORT1 = ((Integer)this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
             })).intValue();
    

    // Step 2: Bring Up the Client
    final String durableClientId = getName() + "_client";
    // keep the client alive for 600 seconds
    final int durableClientTimeout = 600;
    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 1),
            regionName,
            getClientDistributedSystemProperties(durableClientId,
                durableClientTimeout) });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Step 3: Client registers Interests
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K2, new Boolean(false) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K3, new Boolean(true) });
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "registerKey", new Object[] { K4, new Boolean(true) });

    // Close Cache of the DurableClient
    this.durableClientVM.invoke(DurableRegistrationDUnitTest.class,
        "closeCache");

    pause(2000);

    //Re-start the Client
    this.durableClientVM
        .invoke(CacheServerTestUtil.class, "createCacheClient",
            new Object[] { getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, PORT2, true, 1),
                regionName,
                getClientDistributedSystemProperties(durableClientId),
                Boolean.TRUE });

    //Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // Step 4: Bring up the server2
    this.server2VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true),
             PORT2 });

    pause(3000);

    // Check server2 got all the interests registered by the durable client.    
    server2VM.invoke(new CacheSerializableRunnable("Verify Interests.") {
      public void run2() throws CacheException
      {
        getLogWriter().info("### Verifying interests registered by DurableClient. ###");
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        CacheClientProxy p = null;
        
        // Get proxy for the client.        
        for (int i=0; i < 60; i++) {
          Iterator ps = ccn.getClientProxies().iterator();
          if (!ps.hasNext()) {
            pause(1000);
            continue;
          } else {
            p = (CacheClientProxy)ps.next();
            break;
          }
        }

        if (p == null) {
          fail("Proxy initialization taking long time. Increase the wait time.");
        }
        
        Iterator rs = p.getInterestRegisteredRegions().iterator();
        String rName = (String)rs.next();
        assertNotNull("Null region Name found.", rs);
        LocalRegion r = (LocalRegion)GemFireCacheImpl.getInstance().getRegion(rName);
        assertNotNull("Null region found.", r);
        FilterProfile pf = r.getFilterProfile();
        Set intrests = Collections.EMPTY_SET;
        
        Set interestKeys = pf.getKeysOfInterest(p.getProxyID().getDurableId());
        assertNotNull("durable Interests not found for the proxy", interestKeys);
        assertEquals("The number of durable keys registered during HARegion GII doesn't match.", interestKeys.size(), 2);
        interestKeys = pf.getKeysOfInterest(p.getProxyID());
        assertNull("non-durable Interests found for the proxy", interestKeys);
      }
    });


    // Stop the durable client
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 2
    this.server2VM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");

  }

  private static void unregisterAllKeys() {
    // Get the region
    Region region = CacheServerTestUtil.getCache().getRegion(
        DurableRegistrationDUnitTest.class.getName() + "_region");
    // Region region =
    // CacheServerTestUtil.getCache().getRegion(DurableClientSampleDUnitTest.regionName);
    assertNotNull(region);
    region.unregisterInterest(K1);
    region.unregisterInterest(K2);
    region.unregisterInterest(K3);
    region.unregisterInterest(K4);

  }

  private static void registerKeys() throws Exception {
    try {
      // Get the region
      Region region = CacheServerTestUtil.getCache().getRegion(
          DurableRegistrationDUnitTest.class.getName() + "_region");
      // Region region =
      // CacheServerTestUtil.getCache().getRegion(DurableClientSampleDUnitTest.regionName);
      assertNotNull(region);

      region.registerInterest(K1, InterestResultPolicy.KEYS_VALUES, false);
      region.registerInterest(K2, InterestResultPolicy.KEYS_VALUES, false);
      region.registerInterest(K3, InterestResultPolicy.KEYS_VALUES, true);
      region.registerInterest(K4, InterestResultPolicy.KEYS_VALUES, true);

      assertNotNull(region.getInterestList());

    }
    catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private static String getValue(String key) {
    Region r = CacheServerTestUtil.getCache().getRegion(
        DurableRegistrationDUnitTest.class.getName() + "_region");
    // Region r = CacheServerTestUtil.getCache().getRegion(regionName);
    assertNotNull(r);
    // String value = (String)r.get(key);
    // String value = (String)r.getEntry(key).getValue();
    Region.Entry re = r.getEntry(key);

    if (re == null) {
      return null;
    }
    else {
      return (String)re.getValue();
    }
  }

  private static void registerKey(String key, boolean isDurable)
      throws Exception {
    try {
      // Get the region
      Region region = CacheServerTestUtil.getCache().getRegion(
          DurableRegistrationDUnitTest.class.getName() + "_region");
      // Region region =
      // CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
      region.registerInterest(key, InterestResultPolicy.NONE, isDurable);
    }
    catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private static void unregisterKey(String key)
      throws Exception {
    try {
      // Get the region
      Region region = CacheServerTestUtil.getCache().getRegion(
          DurableRegistrationDUnitTest.class.getName() + "_region");
      // Region region =
      // CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
      region.unregisterInterest(key);
    }
    catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private static void putValue(String key, String value) {
    try {
      Region r = CacheServerTestUtil.getCache().getRegion(
          DurableRegistrationDUnitTest.class.getName() + "_region");
      // Region r = CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(r);
      if (r.getEntry(key) != null) {
        r.put(key, value);
      }
      else {
        r.create(key, value);
      }
      assertEquals(value, r.getEntry(key).getValue());
    }
    catch (Exception e) {

      fail("Put in Server has some fight");

    }
  }

  private Pool getClientPool(String host, int server1Port, int server2Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port)
      .addServer(host, server2Port)
      .setSubscriptionEnabled(establishCallbackConnection)
      .setSubscriptionRedundancy(redundancyLevel);
    return ((PoolFactoryImpl)pf).getPoolAttributes();
  }

  private Properties getClientDistributedSystemProperties(String durableClientId) {
    return getClientDistributedSystemProperties(durableClientId,
        DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  private static void checkNumberOfClientProxies(final int expected) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return expected == getNumberOfClientProxies();
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

  private Properties getClientDistributedSystemProperties(
      String durableClientId, int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME,
        durableClientId);
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME,
        String.valueOf(durableClientTimeout));
    return properties;
  }

  private static CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor()
        .getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = (CacheClientProxy)i.next();
    }
    return proxy;
  }

  private static CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer = (CacheServerImpl)CacheServerTestUtil
        .getCache().getCacheServers().iterator().next();
    assertNotNull(bridgeServer);
    return bridgeServer;
  }

  public static void closeCache() {
    Cache cache = CacheServerTestUtil.getCache();
    if (cache != null && !cache.isClosed()) {
      cache.close(true);
      cache.getDistributedSystem().disconnect();
    }
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }
  
  public void tearDown2() throws Exception
  {
    super.tearDown2();
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }
}
