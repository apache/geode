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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ConnectionStats;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CqListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionLocalMaxMemoryDUnitTest.TestObject1;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test client to server flow for delta propogation
 * 
 * @author aingle
 * @since 6.1
 */
public class ClientToServerDeltaDUnitTest extends DistributedTestCase {
  /*
   * Test configuration one server one client also 2 server 2 client
   */
  private static Cache cache = null;
  
  private static LogWriterI18n logger = null;

  VM server = null;

  VM server2 = null;

  VM client = null;

  VM client2 = null;

  private static final String KEY1 = "DELTA_KEY_1";

  private static final String REGION_NAME = "ClientToServerDeltaDunitTest_region";
  
  private static final int NO_PUT_OPERATION = 3;        

  private static PoolImpl pool;

  private static Object[] putDelta = { "Anil",
      DeltaTestImpl.ERRONEOUS_STRING_FOR_FROM_DELTA, 100,
      DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA };

  private static int updates = 0;

  private static int cqUpdates = 0;
  
  private static int create = 0;

  private static Object firstUpdate = null;

  private static Object secondUpdate = null;

  private static Boolean error;
  
  private static boolean lastKeyReceived = false;
  
  private static Region region = null;
  
  private static CSDeltaTestImpl csDelta = null;
  
  public static String DELTA_KEY = "DELTA_KEY";
  
  private static final String[] CQs = new String[] {
      "select * from /" + REGION_NAME,
      "select * from /" + REGION_NAME + " where intVar = 0",
      "select * from /" + REGION_NAME + " where intVar > 0",
      "select * from /" + REGION_NAME + " where intVar < 0"};

  public static String LAST_KEY = "LAST_KEY";
  
  /** constructor */
  public ClientToServerDeltaDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    server2 = host.getVM(1);
    client = host.getVM(2);
    client2 = host.getVM(3);
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    // reset all flags
    DeltaTestImpl.resetDeltaInvokationCounters();
    server.invoke(DeltaTestImpl.class, "resetDeltaInvokationCounters");
    server2.invoke(DeltaTestImpl.class, "resetDeltaInvokationCounters");
    client.invoke(DeltaTestImpl.class, "resetDeltaInvokationCounters");
    client2.invoke(DeltaTestImpl.class, "resetDeltaInvokationCounters");
    // close the clients first
    client.invoke(ClientToServerDeltaDUnitTest.class, "closeCache");
    client2.invoke(ClientToServerDeltaDUnitTest.class, "closeCache");
    // then close the servers
    server.invoke(ClientToServerDeltaDUnitTest.class, "closeCache");
    server2.invoke(ClientToServerDeltaDUnitTest.class, "closeCache");
  }
  
  public void initialise(Boolean cq) {
    initialise(Boolean.TRUE/* clone */, null, cq, Boolean.TRUE/* RI */,
        Boolean.TRUE/* enable delta */);
  }

  public void initialise(Boolean clone, String[] queries, Boolean cq, Boolean RI, Boolean enableDelta) {
    Integer PORT1 = ((Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache",
        new Object[] { Boolean.TRUE, Boolean.FALSE, clone, enableDelta })).intValue();

    Integer PORT2 = ((Integer)server2.invoke(
        ClientToServerDeltaDUnitTest.class, "createServerCache", new Object[] {
            Boolean.TRUE, Boolean.FALSE, clone, enableDelta })).intValue();

    client.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.FALSE, Boolean.FALSE, Boolean.FALSE });

    client2.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server2.getHost()),
            new Integer(PORT2), Boolean.TRUE, Boolean.FALSE, cq, queries, RI });
  }

  // Same as initialise() except listener flag is false.
  public void initialise2(Boolean clone, String[] queries, Boolean cq, Boolean RI, Boolean enableDelta) {
    Integer PORT1 = ((Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache",
        new Object[] { Boolean.FALSE, Boolean.FALSE, clone, enableDelta })).intValue();

    Integer PORT2 = ((Integer)server2.invoke(
        ClientToServerDeltaDUnitTest.class, "createServerCache", new Object[] {
            Boolean.FALSE, Boolean.FALSE, clone, enableDelta })).intValue();

    client.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.FALSE, Boolean.FALSE, Boolean.FALSE });

    client2.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server2.getHost()),
            new Integer(PORT2), Boolean.TRUE, Boolean.FALSE, cq, queries, RI });
  }

  /**
   * This test does the following for single key (<b>failure of fromDelta-
   * resending of full object</b>):<br>
   * 1)Verifies that we donot loose attributes updates when delta fails<br>
   */
  public void testSendingofFullDeltaObjectsWhenFromDeltaFails() {
    initialise(false);
    // set expected value on server
    server.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[1], (Integer)putDelta[2] });
    client.invoke(ClientToServerDeltaDUnitTest.class, "putWithFromDeltaERR",
        new Object[] { KEY1 });

    assertTrue("to Delta Propagation feature NOT used.", ((Boolean)client
        .invoke(DeltaTestImpl.class, "toDeltaFeatureUsed")).booleanValue());
    assertTrue("from Delta Propagation feature NOT used.", ((Boolean)server
        .invoke(DeltaTestImpl.class, "fromDeltaFeatureUsed")).booleanValue());

    assertFalse("Delta Propagation toDeltaFailed", ((Boolean)client.invoke(
        DeltaTestImpl.class, "isToDeltaFailure")).booleanValue());
    assertTrue("Delta Propagation fromDelta not Failed", ((Boolean)server
        .invoke(DeltaTestImpl.class, "isFromDeltaFailure")).booleanValue());

    boolean err = ((Boolean)server.invoke(ClientToServerDeltaDUnitTest.class,
        "getError")).booleanValue();
    assertFalse("validation fails", err);
  }

  /**
   * This test does the following for single key</br> 1)Verifies that we donot
   * loose attributes updates when delta fails<br>
   */
  public void testPutForDeltaObjects() {
    initialise(false);
    // set expected value on server
    server.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[0], (Integer)putDelta[2] });

    client.invoke(ClientToServerDeltaDUnitTest.class, "put",
        new Object[] { KEY1 });

    assertTrue("to Delta Propagation feature NOT used.", ((Boolean)client
        .invoke(DeltaTestImpl.class, "toDeltaFeatureUsed")).booleanValue());
    assertTrue("from Delta Propagation feature NOT used.", ((Boolean)server
        .invoke(DeltaTestImpl.class, "fromDeltaFeatureUsed")).booleanValue());
    assertFalse("Delta Propagation toDeltaFailed", ((Boolean)client.invoke(
        DeltaTestImpl.class, "isToDeltaFailure")).booleanValue());
    assertFalse("Delta Propagation fromDeltaFailed", ((Boolean)server.invoke(
        DeltaTestImpl.class, "isFromDeltaFailure")).booleanValue());
    boolean err = ((Boolean)server.invoke(ClientToServerDeltaDUnitTest.class,
        "getError")).booleanValue();
    assertFalse("validation fails", err);
  }

  /**
   * This test does the following for single key (<b> full cycle</b>):<br>
   * 1)Verifies that client-server, peer-peer and server-client processing delta<br>
   */
  public void testClientToClientDeltaPropagation() throws Exception {
    initialise(false);
    // set expected value on s1,s1 and c2
    server.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[0], (Integer)putDelta[2] });
    server2.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[0], (Integer)putDelta[2] });
    client2.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[0], (Integer)putDelta[2] });

    client.invoke(ClientToServerDeltaDUnitTest.class, "putDelta",
        new Object[] { KEY1 });

    Thread.sleep(5000);

    assertTrue("To Delta Propagation feature NOT used.", ((Boolean)client
        .invoke(DeltaTestImpl.class, "toDeltaFeatureUsed")).booleanValue());
    assertTrue("From Delta Propagation feature NOT used.", ((Boolean)server
        .invoke(DeltaTestImpl.class, "fromDeltaFeatureUsed")).booleanValue());
    // toDelta() should not be invoked
    assertFalse("To Delta Propagation feature used.", ((Boolean)server
        .invoke(DeltaTestImpl.class, "toDeltaFeatureUsed")).booleanValue());
    assertTrue("From Delta Propagation feature NOT used.", ((Boolean)server2
        .invoke(DeltaTestImpl.class, "fromDeltaFeatureUsed")).booleanValue());
    // toDelta() should not be invoked
    assertFalse("To Delta Propagation feature used.", ((Boolean)server2
        .invoke(DeltaTestImpl.class, "toDeltaFeatureUsed")).booleanValue());
    assertTrue("from Delta Propagation feature NOT used.", ((Boolean)client2
        .invoke(DeltaTestImpl.class, "fromDeltaFeatureUsed")).booleanValue());

    boolean err = ((Boolean)server.invoke(ClientToServerDeltaDUnitTest.class,
        "getError")).booleanValue();
    err = ((Boolean)server2.invoke(ClientToServerDeltaDUnitTest.class,
        "getError")).booleanValue();
    err = ((Boolean)client2.invoke(ClientToServerDeltaDUnitTest.class,
        "getError")).booleanValue();
    assertFalse("validation fails", err);
  }

  private static void putDeltaForCQ(String key, Integer numOfPuts, Integer[] cqIndices, Boolean[] satisfyQuery) {
    Region region = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int j = 0; j < numOfPuts; j++) {
      val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObject1("0", 0));
    for(int i=0; i<cqIndices.length; i++) {
      switch(i) {
        case 0:
          val.setStr("CASE_0");
          // select *
          break;
        case 1:
          val.setStr("CASE_1");
          // select where intVar = 0
          if (satisfyQuery[i]) {
            val.setIntVar(0);
          }
          else {
            val.setIntVar(100);
          }
          break;
        case 2:
          val.setStr("CASE_2");
          // select where intVar > 0
          if (satisfyQuery[i]) {
            val.setIntVar(100);
          }
          else {
            val.setIntVar(-100);
          }
          break;
        case 3:
          val.setStr("CASE_3");
          // select where intVar < 0
          if (satisfyQuery[i]) {
            val.setIntVar(-100);
          }
          else {
            val.setIntVar(100);
          }
          break;
        default:
          break;
      }
    }
    region.put(key, val);
    }
  }

  /*
   * put delta ; not previous deltas
   * 
   */
  private static void putDelta(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObject1("0", 0));
      switch (i) {
        case 1:
          val.setStr((String)putDelta[0]);
          break;
        case 2:
          val.setIntVar(((Integer)putDelta[2]).intValue());
          break;
      }
      r.put(key, val);
    }
  }
  
  private static void putLastKey() {
    Region r = cache.getRegion(REGION_NAME);
    r.put(LAST_KEY, LAST_KEY);
  }

  private static void putLastKeyWithDelta() {
    Region r = cache.getRegion(REGION_NAME);
    r.put(LAST_KEY, new DeltaTestImpl());
  }

  private static void setFirstSecondUpdate(Object first, Object second) {
    firstUpdate = first;
    secondUpdate = second;
  }

  private static Boolean getError() {
    return error;
  }

  /*
   * put delta full cycle
   * 
   */
  private static void put(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      switch (i) {
        case 0:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObject1("0", 0));
          break;
        case 1:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObject1("0", 0));
          val.setStr((String)putDelta[0]);
          break;
        case 2:
          val = new DeltaTestImpl(0, (String)putDelta[1], new Double(0),
              new byte[0], new TestObject1("0", 0));
          val.setIntVar(((Integer)putDelta[2]).intValue());
          break;
      }
      r.put(key, val);
    }
  }

  /*
   * put delta with some times fromDelta fails to apply client sends back full
   * object
   */
  private static void putWithFromDeltaERR(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      switch (i) {
        case 0:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObject1("0", 0));
          break;
        case 1:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObject1("0", 0));
          val.setStr((String)putDelta[1]);
          break;
        case 2:
          val = new DeltaTestImpl(0, (String)putDelta[1], new Double(0),
              new byte[0], new TestObject1("0", 0));
          val.setIntVar(((Integer)putDelta[2]).intValue());
          break;
      }
      r.put(key, val);
    }
  }

  /*
   * put delta with some times toDelta fails to apply; client sends back full
   * object
   */
  private static void putWithTODeltaERR(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      switch (i) {
        case 0:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObject1("0", 0));
          break;
        case 1:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObject1("0", 0));
          val.setIntVar(((Integer)putDelta[3]).intValue());
          break;
        case 2:
          val = new DeltaTestImpl(((Integer)putDelta[3]).intValue(), "0",
              new Double(0), new byte[0], new TestObject1("0", 0));
          val.setStr((String)putDelta[0]);
          break;
      }
      r.put(key, val);
    }
  }

  public static Integer createServerCache(Boolean attachListener,
      Boolean isEmpty) throws Exception {
    return createServerCache(attachListener, isEmpty, Boolean.TRUE/* clone */,
        Boolean.TRUE/* enable delta */);
  }

  /*
   * create server cache
   */
  public static Integer createServerCache(Boolean attachListener,
      Boolean isEmpty, Boolean clone, Boolean enableDelta) throws Exception {
    // for validation
    updates = 0;
    create = 0;
    firstUpdate = null;
    secondUpdate = null;
    error = false;
    Properties props = new Properties();
    props.setProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME,
        enableDelta.toString());
    new ClientToServerDeltaDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setConcurrencyChecksEnabled(true);
    if (isEmpty) {
      factory.setSubscriptionAttributes(new SubscriptionAttributes(
          InterestPolicy.ALL));
      factory.setDataPolicy(DataPolicy.EMPTY);
      
    }
    else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    factory.setCloningEnabled(clone);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);
    
    AttributesMutator am = region.getAttributesMutator();
    if (attachListener) {
      am.addCacheListener(new CacheListenerAdapter() {
        public void afterCreate(EntryEvent event) {
          create++;
        }

        public void afterUpdate(EntryEvent event) {
          switch (updates) {
            case 0:
              // first delta
              validateUpdates(event, firstUpdate, "FIRST");
              updates++;
              break;
            case 1:
              // combine delta
              validateUpdates(event, firstUpdate, "FIRST");
              validateUpdates(event, secondUpdate, "SECOND");
              updates++;
              break;
            default:
              break;
          }
        }
      });
    } else if (!isEmpty){
      am.addCacheListener(new CacheListenerAdapter() {
        public void afterCreate(EntryEvent event) {
          switch (create) {
            case 1:
              validateUpdates(event, firstUpdate, "FIRST");
              create++;
              break;
            case 2:
              validateUpdates(event, secondUpdate, "SECOND");
              create++;
              break;
            default:
              create++;
              break;
          }
        }
       /* public void afterUpdate(EntryEvent event) {
          updates++;
        }*/
      });

    }
    
    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static void createClientCache(String host, Integer port,
      Boolean attachListener, Boolean isEmpty, Boolean isCq) throws Exception {
    createClientCache(host, port, attachListener, isEmpty, isCq, new String[0], true);
  }

  public static void createClientCache(String host, Integer port,
      Boolean attachListener, Boolean isEmpty, Boolean isCq, String[] cqQueryString) throws Exception {
    createClientCache(host, port, attachListener, isEmpty, isCq, cqQueryString, true);
  }

  public static void createClientCache(String host, Integer port,
      Boolean attachListener, Boolean isEmpty, Boolean isCq,
      String[] cqQueryString, Boolean registerInterestAll) throws Exception {
    createClientCache(host, port, attachListener, isEmpty, isCq, cqQueryString,
        registerInterestAll, true);
  }

  /*
   * create client cache
   */
  public static void createClientCache(String host, Integer port,
      Boolean attachListener, Boolean isEmpty, Boolean isCq,
      String[] cqQueryString, Boolean registerInterestAll,
      Boolean enableSubscription) throws Exception {
    updates = 0;
    create = 0;
    firstUpdate = null;
    secondUpdate = null;
    error = false;
    lastKeyReceived = false;
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new ClientToServerDeltaDUnitTest("temp").createCache(props);
    pool = (PoolImpl)PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(2)
        .setSubscriptionEnabled(enableSubscription).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setPingInterval(1000).setSocketBufferSize(32768).create(
            "ClientToServerDeltaDunitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setConcurrencyChecksEnabled(true);
    if (isEmpty) {
      factory.setSubscriptionAttributes(new SubscriptionAttributes(
          InterestPolicy.ALL));
      factory.setDataPolicy(DataPolicy.EMPTY);      
    }
    else {
      factory.setDataPolicy(DataPolicy.NORMAL);
    }
    factory.setPoolName(pool.getName());
    factory.setCloningEnabled(false);

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);
    if (attachListener) {
      region.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
        public void afterCreate(EntryEvent event) {
          create++;
          if (LAST_KEY.equals(event.getKey())) {          
            lastKeyReceived = true;
          };
        }

        public void afterUpdate(EntryEvent event) {
          switch (updates) {
            case 0:
              // first delta
              validateUpdates(event, firstUpdate, "FIRST");
              updates++;
              break;
            case 1:
              // combine delta
              validateUpdates(event, firstUpdate, "FIRST");
              validateUpdates(event, secondUpdate, "SECOND");
              updates++;
              break;
            default:
              break;
          }
        }
      });
    }
    if (registerInterestAll) {
      region.registerInterest("ALL_KEYS");
    }
    if (isCq) {
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListenerAdapter cqlist = new CqListenerAdapter() {
        public void onEvent(CqEvent cqEvent) {
          Object key = cqEvent.getKey();
          if (LAST_KEY.equals(key)) {
            lastKeyReceived = true;
          }
          logger.fine("CQ event received for (key, value): (" + key + ", "
              + cqEvent.getNewValue() + ")");
        }
        
        public void onError(CqEvent cqEvent) {
          logger.fine("CQ error received for key: " + cqEvent.getKey());
        }
      };  
      cqf.addCqListener(cqlist);
      CqAttributes cqa = cqf.create();
      for (int i = 0; i < cqQueryString.length; i++) {
        CqQuery cq = cache.getQueryService().newCq("Delta_Query_" + i,
            cqQueryString[i], cqa);
        cq.execute();
      }
    }
  }
  
  /*
   * create cache with properties
   */
  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    logger = cache.getLoggerI18n();
  }

  // to validate updates in listener
  private static void validateUpdates(EntryEvent event, Object obj, String str) {
    if (obj instanceof String)
      assertTrue(str + " update missed ", ((DeltaTestImpl)event.getNewValue())
          .getStr().equals((String)obj));
    else if (obj instanceof Integer)
      assertTrue(str + " update missed ", ((DeltaTestImpl)event.getNewValue())
          .getIntVar() == (Integer)obj);
    else
      error = true;
  }

  /*
   * close cache
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      lastKeyReceived = false;
    }
  }
  /**
   * This test does the following for single key:<br>
   * 1)Verifies that cacheless client calls toDelta <br>
   */
  public void testEmptyClientAsFeederToServer() {
    Integer PORT1 = ((Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache", new Object[] { Boolean.FALSE,
            Boolean.FALSE })).intValue();

    server2.invoke(ClientToServerDeltaDUnitTest.class, "createServerCache",
        new Object[] { Boolean.FALSE, Boolean.FALSE });

    client.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.FALSE, Boolean.TRUE, Boolean.FALSE });

/*    server2.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[0], (Integer)putDelta[2] });*/

    client.invoke(ClientToServerDeltaDUnitTest.class, "putDelta",
        new Object[] { KEY1 });

    client.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(2)});
    server.invoke(ClientToServerDeltaDUnitTest.class, "checkFromdeltaCounter");
    server.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(0)});
    server2.invoke(ClientToServerDeltaDUnitTest.class, "checkFromdeltaCounter");
    server2.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(0)});
  }

  /**
   * This test does the following for single key:<br>
   * 1)Verifies that from delta should not called on server with empty data
   * policy just by passed delta to data store<br>
   */
  public void testEmptyServerAsFeederToPeer() {
    Integer PORT1 = ((Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache", new Object[] { Boolean.FALSE,
            Boolean.TRUE })).intValue();

    server2.invoke(ClientToServerDeltaDUnitTest.class, "createServerCache",
        new Object[] { Boolean.FALSE, Boolean.FALSE });

    client.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.FALSE, Boolean.TRUE, Boolean.FALSE });

    client.invoke(ClientToServerDeltaDUnitTest.class, "putDelta",
        new Object[] { KEY1 });

    client.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(2)});
    
    server.invoke(ClientToServerDeltaDUnitTest.class, "checkDeltaFeatureNotUsed");
    
    server2.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(0)});
    server2.invoke(ClientToServerDeltaDUnitTest.class, "checkFromdeltaCounter");
    
  }
  
  /**
   * This test does verifies that server with empty data policy sends deltas to 
   * the client which can handle deltas. Server sends full values which can not
   * handle deltas.
   * 
   */
  public void testClientsConnectedToEmptyServer() {
    Integer PORT1 = ((Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache", new Object[] { Boolean.FALSE,
            Boolean.TRUE })).intValue();

    server2.invoke(ClientToServerDeltaDUnitTest.class, "createServerCache",
        new Object[] { Boolean.FALSE, Boolean.FALSE });

    client.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.TRUE, Boolean.TRUE, Boolean.FALSE });

    client2.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.TRUE, Boolean.FALSE, Boolean.FALSE });
    
    int deltaSent =  (Integer)server2.invoke(
        ClientToServerDeltaDUnitTest.class,"putsWhichReturnsDeltaSent");
    
    client.invoke(ClientToServerDeltaDUnitTest.class, "waitForLastKey");    
    client.invoke(ClientToServerDeltaDUnitTest.class, "checkForDelta");
    
    client2.invoke(ClientToServerDeltaDUnitTest.class, "waitForLastKey");    
    client2.invoke(ClientToServerDeltaDUnitTest.class, "checkDeltaInvoked", new Object[]{new Integer(deltaSent)});
    
  }
  
  /**
   * This test does the following for single key:<br>
   * 1)Verifies that To delta called on client should be equal to fromDeltaCounter on datastore <br>
   */
  public void testClientNonEmptyEmptyServerAsFeederToPeer() {
    Integer PORT1 = ((Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache", new Object[] { Boolean.FALSE,
            Boolean.TRUE })).intValue();

    server2.invoke(ClientToServerDeltaDUnitTest.class, "createServerCache",
        new Object[] { Boolean.FALSE, Boolean.FALSE });

    client.invoke(ClientToServerDeltaDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(server.getHost()), new Integer(PORT1),
            Boolean.FALSE, Boolean.FALSE, Boolean.FALSE});

    client.invoke(ClientToServerDeltaDUnitTest.class, "putDelta",
        new Object[] { KEY1 });

    client.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(2)});
    
    server.invoke(ClientToServerDeltaDUnitTest.class, "checkDeltaFeatureNotUsed");
    
    server2.invoke(ClientToServerDeltaDUnitTest.class, "checkTodeltaCounter",
        new Object[] { new Integer(0)});
    server2.invoke(ClientToServerDeltaDUnitTest.class, "checkFromdeltaCounter");
    
  }

  /**
   * This test has client1 connected to server1 and client2 connected to
   * server2. client2 has a CQ registered on the server2. client1 creates a key
   * on server1 which satisfies the CQ and it then updates the same key with a
   * value so that it sould not satisfy the CQ. The cloning is set to false. The
   * test ensures that the client2 gets the update event. This implies that
   * server2 clones the old value prior to applying delta received from server1
   * and then processes the CQ.
   * 
   * @throws Exception
   */
  public void testC2CDeltaPropagationWithCQ() throws Exception {
    initialise(false/* clone */, new String[] { CQs[1] }, true/* CQ */,
        true/* RI */, true/* enable delta */);
    client.invoke(ClientToServerDeltaDUnitTest.class, "createKeys",
        new Object[] { new String[] { KEY1 } });
    client.invoke(ClientToServerDeltaDUnitTest.class, "putDeltaForCQ",
        new Object[] { KEY1, new Integer(1), new Integer[] { 1 },
            new Boolean[] { false, true, false, false } });
    
    client.invoke(ClientToServerDeltaDUnitTest.class, "putLastKeyWithDelta");
    
    client2.invoke(ClientToServerDeltaDUnitTest.class, "waitForLastKey");
    client2.invoke(ClientToServerDeltaDUnitTest.class, "verifyDeltaReceived");    
  }

  /**
   * This test ensures that a server sends delta bytes to a client even if that
   * client is not interested in that event but is getting the event only
   * because the event satisfies a CQ which the client has registered with the
   * server.
   * 
   * @throws Exception
   */
  public void testC2CDeltaPropagationWithCQWithoutRI() throws Exception {
    initialise(false/* clone */, new String[] { CQs[1] }, true/* CQ */,
        false/* RI */, true/* enable delta */);

    client.invoke(ClientToServerDeltaDUnitTest.class, "createKeys",
        new Object[] { new String[] { KEY1 } });
    client.invoke(ClientToServerDeltaDUnitTest.class, "putDeltaForCQ",
        new Object[] { KEY1, new Integer(1), new Integer[] { 1 },
            new Boolean[] { false, true, false, false } });
    
    client.invoke(ClientToServerDeltaDUnitTest.class, "putLastKeyWithDelta");
    
    client2.invoke(ClientToServerDeltaDUnitTest.class, "waitForLastKey");
    server2.invoke(ClientToServerDeltaDUnitTest.class, "verifyDeltaSent",
        new Object[] { Integer.valueOf(1) });
  }

  public void testClientSendsFullValueToServerWhenDeltaOffAtServer() {
    initialise2(false/* clone */, new String[] { CQs[1] }, false/* CQ */,
        true/* RI */, false/* enable delta */);
    // set expected value on server
    server.invoke(ClientToServerDeltaDUnitTest.class, "setFirstSecondUpdate",
        new Object[] { putDelta[0], (Integer)putDelta[2] });

    client.invoke(ClientToServerDeltaDUnitTest.class, "putDelta",
        new Object[] { DELTA_KEY });

    assertFalse("Delta Propagation feature used at client.", (Boolean)client
        .invoke(DeltaTestImpl.class, "toDeltaFeatureUsed"));
    assertFalse("Delta Propagation feature used at server.", (Boolean)server
        .invoke(DeltaTestImpl.class, "fromDeltaFeatureUsed"));
    assertFalse(
        "Failures at client while calculating delta. But delta-propagation is false at server.",
        ((Boolean)client.invoke(DeltaTestImpl.class, "isToDeltaFailure"))
            .booleanValue());
    assertFalse(
        "Failures at server while applying delta. But delta-propagation is false at server.",
        (Boolean)server.invoke(DeltaTestImpl.class, "isFromDeltaFailure"));
  }

  public void testC2SDeltaPropagationWithOldValueInvalidatedAtServer()
      throws Exception {
    String key = "DELTA_KEY";
    Integer port1 = (Integer)server.invoke(ClientToServerDeltaDUnitTest.class,
        "createServerCache", new Object[] {false, false, false, true});
    createClientCache("localhost", port1, false, false, false, null, false,
        false);

    LocalRegion region = (LocalRegion)cache.getRegion(REGION_NAME);
    region.put(key, new DeltaTestImpl());

    server.invoke(ClientToServerDeltaDUnitTest.class, "doInvalidate",
        new Object[] {key});
    DeltaTestImpl value = new DeltaTestImpl();
    value.setStr("UPDATED_VALUE");
    region.put(key, value);

    assertTrue(region.getCachePerfStats().getDeltasSent() == 1);
    assertTrue(region.getCachePerfStats().getDeltaFullValuesSent() == 1);
  }

  public static void doInvalidate(String key) {
    Region region = cache.getRegion(REGION_NAME);
    region.invalidate(key);
  }
  
  public static void verifyDeltaReceived() {
    assertTrue("Expected 1 fromDelta() invokation but was "
        + DeltaTestImpl.getFromDeltaInvokations(), DeltaTestImpl
        .getFromDeltaInvokations() == 1);
  }

  public static void verifyDeltaSent(Integer deltas) {
    CacheClientNotifier ccn = ((CacheServerImpl)cache
        .getCacheServers().toArray()[0]).getAcceptor()
        .getCacheClientNotifier();

    int numOfDeltasSent = ((CacheClientProxy)ccn.getClientProxies().toArray()[0])
        .getStatistics().getDeltaMessagesSent();
    assertTrue("Expected " + deltas + " deltas to be sent but "
        + numOfDeltasSent + " were sent.", numOfDeltasSent == deltas);
  }

  public static void checkFromdeltaCounter() {
    assertTrue("FromDelta counters do not match, expected: "
        + (NO_PUT_OPERATION - 1) + ", actual: "
        + DeltaTestImpl.getFromDeltaInvokations(), DeltaTestImpl
        .getFromDeltaInvokations() >= (NO_PUT_OPERATION - 1));
  }

  public static void checkTodeltaCounter(Integer count) {
    assertTrue("ToDelta counters do not match, expected: " + count.intValue()
        + ", actual: " + DeltaTestImpl.getToDeltaInvokations(), DeltaTestImpl
        .getToDeltaInvokations() >= count.intValue());
  }

  public static void checkDeltaFeatureNotUsed() {
    assertTrue("Delta Propagation feature used.", !(DeltaTestImpl
        .deltaFeatureUsed()));
  }

  public static void createKeys(String[] keys) {
    Region region = cache.getRegion(REGION_NAME);
    for (int i = 0; i < keys.length; i++) {
      region.create(keys[i], new DeltaTestImpl());
    }
  }
  
  public static int putsWhichReturnsDeltaSent() throws Exception {
    csDelta = new CSDeltaTestImpl();
    region.put(DELTA_KEY, csDelta);
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      csDelta.setIntVar(i);
      region.put(DELTA_KEY, csDelta);
    }
    region.put(LAST_KEY, "");
    
    return csDelta.deltaSent;
  }
  
  public static void checkDeltaInvoked(Integer deltaSent) {
    assertTrue("Delta applied :"
        + ((CSDeltaTestImpl)region.get(DELTA_KEY)).getDeltaApplied()
        + "\n Delta sent :" + deltaSent, ((CSDeltaTestImpl)region
        .get(DELTA_KEY)).getDeltaApplied() == deltaSent);
  }
  
  public static void checkForDelta() {
    assertTrue("Delta sent to EMPTY data policy region", DeltaTestImpl.getFromDeltaInvokations()==0);
  }

  public static void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return ClientToServerDeltaDUnitTest.lastKeyReceived;
      }
      public String description() {
        return "Last key NOT received.";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10*1000, 100, true);
  }
  
  static class CSDeltaTestImpl extends DeltaTestImpl {
    int deltaSent = 0;
    int deltaApplied = 0;

    public CSDeltaTestImpl() {
    }

    public void toDelta(DataOutput out) throws IOException {
      super.toDelta(out);
      deltaSent++;
    }

    public void fromDelta(DataInput in) throws IOException {
      super.fromDelta(in);      
      deltaApplied++;
    }

    public int getDeltaSent() {
      return deltaSent;
    }

    public int getDeltaApplied() {
      return deltaApplied;
    }

    public String toString() {
      return "CSDeltaTestImpl[deltaApplied=" + deltaApplied + "]"
          + super.toString();
    }
  }

}
