/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.DELTA_PROPAGATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DeltaTestImpl;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CqListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TestObjectWithIdentifier;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Test client to server flow for delta propogation
 *
 * @since GemFire 6.1
 */
@Category({ClientSubscriptionTest.class, SerializationTest.class})
public class ClientToServerDeltaDUnitTest extends JUnit4DistributedTestCase {
  /*
   * Test configuration one server one client also 2 server 2 client
   */
  private static Cache cache = null;

  private static LogWriter logger = null;

  VM server = null;
  VM server2 = null;
  VM client = null;
  VM client2 = null;

  private static final String KEY1 = "DELTA_KEY_1";

  private static final String REGION_NAME = "ClientToServerDeltaDunitTest_region";

  private static final int NO_PUT_OPERATION = 3;

  private static PoolImpl pool;

  private static Object[] putDelta = {"Anil", DeltaTestImpl.ERRONEOUS_STRING_FOR_FROM_DELTA, 100,
      DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA};

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

  private static final String[] CQs = new String[] {"select * from /" + REGION_NAME,
      "select * from /" + REGION_NAME + " where intVar = 0",
      "select * from /" + REGION_NAME + " where intVar > 0",
      "select * from /" + REGION_NAME + " where intVar < 0"};

  public static String LAST_KEY = "LAST_KEY";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    server2 = host.getVM(1);
    client = host.getVM(2);
    client2 = host.getVM(3);
  }

  @Override
  public final void preTearDown() throws Exception {
    // reset all flags
    DeltaTestImpl.resetDeltaInvokationCounters();
    server.invoke(() -> DeltaTestImpl.resetDeltaInvokationCounters());
    server2.invoke(() -> DeltaTestImpl.resetDeltaInvokationCounters());
    client.invoke(() -> DeltaTestImpl.resetDeltaInvokationCounters());
    client2.invoke(() -> DeltaTestImpl.resetDeltaInvokationCounters());
    // close the clients first
    client.invoke(() -> ClientToServerDeltaDUnitTest.closeCache());
    client2.invoke(() -> ClientToServerDeltaDUnitTest.closeCache());
    // then close the servers
    server.invoke(() -> ClientToServerDeltaDUnitTest.closeCache());
    server2.invoke(() -> ClientToServerDeltaDUnitTest.closeCache());
  }

  public void initialise(Boolean cq) {
    initialise(Boolean.TRUE/* clone */, null, cq, Boolean.TRUE/* RI */,
        Boolean.TRUE/* enable delta */);
  }

  public void initialise(Boolean clone, String[] queries, Boolean cq, Boolean RI,
      Boolean enableDelta) {
    Integer PORT1 = ((Integer) server.invoke(() -> ClientToServerDeltaDUnitTest
        .createServerCache(Boolean.TRUE, Boolean.FALSE, clone, enableDelta))).intValue();

    Integer PORT2 = ((Integer) server2.invoke(() -> ClientToServerDeltaDUnitTest
        .createServerCache(Boolean.TRUE, Boolean.FALSE, clone, enableDelta))).intValue();

    client.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.FALSE,
        Boolean.FALSE, Boolean.FALSE));

    client2.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2), Boolean.TRUE,
        Boolean.FALSE, cq, queries, RI));
  }

  // Same as initialise() except listener flag is false.
  public void initialise2(Boolean clone, String[] queries, Boolean cq, Boolean RI,
      Boolean enableDelta) {
    Integer PORT1 = ((Integer) server.invoke(() -> ClientToServerDeltaDUnitTest
        .createServerCache(Boolean.FALSE, Boolean.FALSE, clone, enableDelta))).intValue();

    Integer PORT2 = ((Integer) server2.invoke(() -> ClientToServerDeltaDUnitTest
        .createServerCache(Boolean.FALSE, Boolean.FALSE, clone, enableDelta))).intValue();

    client.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.FALSE,
        Boolean.FALSE, Boolean.FALSE));

    client2.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2), Boolean.TRUE,
        Boolean.FALSE, cq, queries, RI));
  }

  /**
   * This test does the following for single key (<b>failure of fromDelta- resending of full
   * object</b>):<br>
   * 1)Verifies that we donot loose attributes updates when delta fails<br>
   */
  @Test
  public void testSendingofFullDeltaObjectsWhenFromDeltaFails() {
    initialise(false);
    // set expected value on server
    server.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate(putDelta[1],
        (Integer) putDelta[2]));
    client.invoke(() -> ClientToServerDeltaDUnitTest.putWithFromDeltaERR(KEY1));

    assertTrue("to Delta Propagation feature NOT used.",
        ((Boolean) client.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed())).booleanValue());
    assertTrue("from Delta Propagation feature NOT used.",
        ((Boolean) server.invoke(() -> DeltaTestImpl.fromDeltaFeatureUsed())).booleanValue());

    assertFalse("Delta Propagation toDeltaFailed",
        ((Boolean) client.invoke(() -> DeltaTestImpl.isToDeltaFailure())).booleanValue());
    assertTrue("Delta Propagation fromDelta not Failed",
        ((Boolean) server.invoke(() -> DeltaTestImpl.isFromDeltaFailure())).booleanValue());

    boolean err =
        ((Boolean) server.invoke(() -> ClientToServerDeltaDUnitTest.getError())).booleanValue();
    assertFalse("validation fails", err);
  }

  /**
   * This test does the following for single key</br>
   * 1)Verifies that we donot loose attributes updates when delta fails<br>
   */
  @Test
  public void testPutForDeltaObjects() {
    initialise(false);
    // set expected value on server
    server.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate(putDelta[0],
        (Integer) putDelta[2]));

    client.invoke(() -> ClientToServerDeltaDUnitTest.put(KEY1));

    assertTrue("to Delta Propagation feature NOT used.",
        ((Boolean) client.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed())).booleanValue());
    assertTrue("from Delta Propagation feature NOT used.",
        ((Boolean) server.invoke(() -> DeltaTestImpl.fromDeltaFeatureUsed())).booleanValue());
    assertFalse("Delta Propagation toDeltaFailed",
        ((Boolean) client.invoke(() -> DeltaTestImpl.isToDeltaFailure())).booleanValue());
    assertFalse("Delta Propagation fromDeltaFailed",
        ((Boolean) server.invoke(() -> DeltaTestImpl.isFromDeltaFailure())).booleanValue());
    boolean err =
        ((Boolean) server.invoke(() -> ClientToServerDeltaDUnitTest.getError())).booleanValue();
    assertFalse("validation fails", err);
  }

  /**
   * This test does the following for single key (<b> full cycle</b>):<br>
   * 1)Verifies that client-server, peer-peer and server-client processing delta<br>
   */
  @Test
  public void testClientToClientDeltaPropagation() throws Exception {
    initialise(false);
    // set expected value on s1,s1 and c2
    server.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate(putDelta[0],
        (Integer) putDelta[2]));
    server2.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate(putDelta[0],
        (Integer) putDelta[2]));
    client2.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate(putDelta[0],
        (Integer) putDelta[2]));

    client.invoke(() -> ClientToServerDeltaDUnitTest.putDelta(KEY1));

    Thread.sleep(5000);

    assertTrue("To Delta Propagation feature NOT used.",
        ((Boolean) client.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed())).booleanValue());
    assertTrue("From Delta Propagation feature NOT used.",
        ((Boolean) server.invoke(() -> DeltaTestImpl.fromDeltaFeatureUsed())).booleanValue());
    // toDelta() should not be invoked
    assertFalse("To Delta Propagation feature used.",
        ((Boolean) server.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed())).booleanValue());
    assertTrue("From Delta Propagation feature NOT used.",
        ((Boolean) server2.invoke(() -> DeltaTestImpl.fromDeltaFeatureUsed())).booleanValue());
    // toDelta() should not be invoked
    assertFalse("To Delta Propagation feature used.",
        ((Boolean) server2.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed())).booleanValue());
    assertTrue("from Delta Propagation feature NOT used.",
        ((Boolean) client2.invoke(() -> DeltaTestImpl.fromDeltaFeatureUsed())).booleanValue());

    boolean err =
        ((Boolean) server.invoke(() -> ClientToServerDeltaDUnitTest.getError())).booleanValue();
    err = ((Boolean) server2.invoke(() -> ClientToServerDeltaDUnitTest.getError())).booleanValue();
    err = ((Boolean) client2.invoke(() -> ClientToServerDeltaDUnitTest.getError())).booleanValue();
    assertFalse("validation fails", err);
  }

  private static void putDeltaForCQ(String key, Integer numOfPuts, Integer[] cqIndices,
      Boolean[] satisfyQuery) {
    Region region = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int j = 0; j < numOfPuts; j++) {
      val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObjectWithIdentifier("0", 0));
      for (int i = 0; i < cqIndices.length; i++) {
        switch (i) {
          case 0:
            val.setStr("CASE_0");
            // select *
            break;
          case 1:
            val.setStr("CASE_1");
            // select where intVar = 0
            if (satisfyQuery[i]) {
              val.setIntVar(0);
            } else {
              val.setIntVar(100);
            }
            break;
          case 2:
            val.setStr("CASE_2");
            // select where intVar > 0
            if (satisfyQuery[i]) {
              val.setIntVar(100);
            } else {
              val.setIntVar(-100);
            }
            break;
          case 3:
            val.setStr("CASE_3");
            // select where intVar < 0
            if (satisfyQuery[i]) {
              val.setIntVar(-100);
            } else {
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
   */
  private static void putDelta(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObjectWithIdentifier("0", 0));
      switch (i) {
        case 1:
          val.setStr((String) putDelta[0]);
          break;
        case 2:
          val.setIntVar(((Integer) putDelta[2]).intValue());
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
   */
  private static void put(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      switch (i) {
        case 0:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          break;
        case 1:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          val.setStr((String) putDelta[0]);
          break;
        case 2:
          val = new DeltaTestImpl(0, (String) putDelta[1], new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          val.setIntVar(((Integer) putDelta[2]).intValue());
          break;
      }
      r.put(key, val);
    }
  }

  /*
   * put delta with some times fromDelta fails to apply client sends back full object
   */
  private static void putWithFromDeltaERR(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      switch (i) {
        case 0:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          break;
        case 1:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          val.setStr((String) putDelta[1]);
          break;
        case 2:
          val = new DeltaTestImpl(0, (String) putDelta[1], new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          val.setIntVar(((Integer) putDelta[2]).intValue());
          break;
      }
      r.put(key, val);
    }
  }

  /*
   * put delta with some times toDelta fails to apply; client sends back full object
   */
  private static void putWithTODeltaERR(String key) {
    Region r = cache.getRegion(REGION_NAME);
    DeltaTestImpl val = null;
    for (int i = 0; i < NO_PUT_OPERATION; i++) {
      switch (i) {
        case 0:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          break;
        case 1:
          val = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
              new TestObjectWithIdentifier("0", 0));
          val.setIntVar(((Integer) putDelta[3]).intValue());
          break;
        case 2:
          val = new DeltaTestImpl(((Integer) putDelta[3]).intValue(), "0", new Double(0),
              new byte[0], new TestObjectWithIdentifier("0", 0));
          val.setStr((String) putDelta[0]);
          break;
      }
      r.put(key, val);
    }
  }

  public static Integer createServerCache(Boolean attachListener, Boolean isEmpty)
      throws Exception {
    return createServerCache(attachListener, isEmpty, Boolean.TRUE/* clone */,
        Boolean.TRUE/* enable delta */);
  }

  /*
   * create server cache
   */
  public static Integer createServerCache(Boolean attachListener, Boolean isEmpty, Boolean clone,
      Boolean enableDelta) throws Exception {
    // for validation
    updates = 0;
    create = 0;
    firstUpdate = null;
    secondUpdate = null;
    error = false;
    Properties props = new Properties();
    props.setProperty(DELTA_PROPAGATION, enableDelta.toString());
    new ClientToServerDeltaDUnitTest().createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setConcurrencyChecksEnabled(true);
    if (isEmpty) {
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      factory.setDataPolicy(DataPolicy.EMPTY);

    } else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    factory.setCloningEnabled(clone);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);

    AttributesMutator am = region.getAttributesMutator();
    if (attachListener) {
      am.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterCreate(EntryEvent event) {
          create++;
        }

        @Override
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
    } else if (!isEmpty) {
      am.addCacheListener(new CacheListenerAdapter() {
        @Override
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
        /*
         * public void afterUpdate(EntryEvent event) { updates++; }
         */
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

  public static void createClientCache(String host, Integer port, Boolean attachListener,
      Boolean isEmpty, Boolean isCq) throws Exception {
    createClientCache(host, port, attachListener, isEmpty, isCq, new String[0], true);
  }

  public static void createClientCache(String host, Integer port, Boolean attachListener,
      Boolean isEmpty, Boolean isCq, String[] cqQueryString) throws Exception {
    createClientCache(host, port, attachListener, isEmpty, isCq, cqQueryString, true);
  }

  public static void createClientCache(String host, Integer port, Boolean attachListener,
      Boolean isEmpty, Boolean isCq, String[] cqQueryString, Boolean registerInterestAll)
      throws Exception {
    createClientCache(host, port, attachListener, isEmpty, isCq, cqQueryString, registerInterestAll,
        true);
  }

  /*
   * create client cache
   */
  public static void createClientCache(String host, Integer port, Boolean attachListener,
      Boolean isEmpty, Boolean isCq, String[] cqQueryString, Boolean registerInterestAll,
      Boolean enableSubscription) throws Exception {
    updates = 0;
    create = 0;
    firstUpdate = null;
    secondUpdate = null;
    error = false;
    lastKeyReceived = false;
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new ClientToServerDeltaDUnitTest().createCache(props);
    pool = (PoolImpl) PoolManager.createFactory().addServer(host, port.intValue())
        .setThreadLocalConnections(true).setMinConnections(2)
        .setSubscriptionEnabled(enableSubscription).setSubscriptionRedundancy(0)
        .setReadTimeout(10000).setPingInterval(1000).setSocketBufferSize(32768)
        .create("ClientToServerDeltaDunitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setConcurrencyChecksEnabled(true);
    if (isEmpty) {
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      factory.setDataPolicy(DataPolicy.EMPTY);
    } else {
      factory.setDataPolicy(DataPolicy.NORMAL);
    }
    factory.setPoolName(pool.getName());
    factory.setCloningEnabled(false);

    // region with empty data policy
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(REGION_NAME, attrs);
    if (attachListener) {
      region.getAttributesMutator().addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterCreate(EntryEvent event) {
          create++;
          if (LAST_KEY.equals(event.getKey())) {
            lastKeyReceived = true;
          } ;
        }

        @Override
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
        @Override
        public void onEvent(CqEvent cqEvent) {
          Object key = cqEvent.getKey();
          if (LAST_KEY.equals(key)) {
            lastKeyReceived = true;
          }
          logger.fine(
              "CQ event received for (key, value): (" + key + ", " + cqEvent.getNewValue() + ")");
        }

        @Override
        public void onError(CqEvent cqEvent) {
          logger.fine("CQ error received for key: " + cqEvent.getKey());
        }
      };
      cqf.addCqListener(cqlist);
      CqAttributes cqa = cqf.create();
      for (int i = 0; i < cqQueryString.length; i++) {
        CqQuery cq = cache.getQueryService().newCq("Delta_Query_" + i, cqQueryString[i], cqa);
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
    logger = cache.getLogger();
  }

  // to validate updates in listener
  private static void validateUpdates(EntryEvent event, Object obj, String str) {
    if (obj instanceof String)
      assertTrue(str + " update missed ",
          ((DeltaTestImpl) event.getNewValue()).getStr().equals((String) obj));
    else if (obj instanceof Integer)
      assertTrue(str + " update missed ",
          ((DeltaTestImpl) event.getNewValue()).getIntVar() == (Integer) obj);
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
  @Test
  public void testEmptyClientAsFeederToServer() {
    Integer PORT1 = ((Integer) server
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.FALSE)))
            .intValue();

    server2
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.FALSE));

    client.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.FALSE,
        Boolean.TRUE, Boolean.FALSE));

    /*
     * server2.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate( putDelta[0],
     * (Integer)putDelta[2] ));
     */

    client.invoke(() -> ClientToServerDeltaDUnitTest.putDelta(KEY1));

    client.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(2)));
    server.invoke(() -> ClientToServerDeltaDUnitTest.checkFromdeltaCounter());
    server.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(0)));
    server2.invoke(() -> ClientToServerDeltaDUnitTest.checkFromdeltaCounter());
    server2.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(0)));
  }

  /**
   * This test does the following for single key:<br>
   * 1)Verifies that from delta should not called on server with empty data policy just by passed
   * delta to data store<br>
   */
  @Test
  public void testEmptyServerAsFeederToPeer() {
    Integer PORT1 = ((Integer) server
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.TRUE)))
            .intValue();

    server2
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.FALSE));

    client.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.FALSE,
        Boolean.TRUE, Boolean.FALSE));

    client.invoke(() -> ClientToServerDeltaDUnitTest.putDelta(KEY1));

    client.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(2)));

    server.invoke(() -> ClientToServerDeltaDUnitTest.checkDeltaFeatureNotUsed());

    server2.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(0)));
    server2.invoke(() -> ClientToServerDeltaDUnitTest.checkFromdeltaCounter());
  }

  /**
   * This test does verifies that server with empty data policy sends deltas to the client which can
   * handle deltas. Server sends full values which can not handle deltas.
   */
  @Test
  public void testClientsConnectedToEmptyServer() {
    Integer PORT1 = ((Integer) server
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.TRUE)))
            .intValue();

    server2
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.FALSE));

    client.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.TRUE,
        Boolean.TRUE, Boolean.FALSE));

    client2.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.TRUE,
        Boolean.FALSE, Boolean.FALSE));

    int deltaSent =
        (Integer) server2.invoke(() -> ClientToServerDeltaDUnitTest.putsWhichReturnsDeltaSent());

    client.invoke(() -> ClientToServerDeltaDUnitTest.waitForLastKey());
    client.invoke(() -> ClientToServerDeltaDUnitTest.checkForDelta());

    client2.invoke(() -> ClientToServerDeltaDUnitTest.waitForLastKey());
    client2.invoke(() -> ClientToServerDeltaDUnitTest.checkDeltaInvoked(new Integer(deltaSent)));
  }

  /**
   * This test does the following for single key:<br>
   * 1)Verifies that To delta called on client should be equal to fromDeltaCounter on datastore <br>
   */
  @Test
  public void testClientNonEmptyEmptyServerAsFeederToPeer() {
    Integer PORT1 = ((Integer) server
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.TRUE)))
            .intValue();

    server2
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(Boolean.FALSE, Boolean.FALSE));

    client.invoke(() -> ClientToServerDeltaDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server.getHost()), new Integer(PORT1), Boolean.FALSE,
        Boolean.FALSE, Boolean.FALSE));

    client.invoke(() -> ClientToServerDeltaDUnitTest.putDelta(KEY1));

    client.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(2)));

    server.invoke(() -> ClientToServerDeltaDUnitTest.checkDeltaFeatureNotUsed());

    server2.invoke(() -> ClientToServerDeltaDUnitTest.checkTodeltaCounter(new Integer(0)));
    server2.invoke(() -> ClientToServerDeltaDUnitTest.checkFromdeltaCounter());
  }

  /**
   * This test has client1 connected to server1 and client2 connected to server2. client2 has a CQ
   * registered on the server2. client1 creates a key on server1 which satisfies the CQ and it then
   * updates the same key with a value so that it sould not satisfy the CQ. The cloning is set to
   * false. The test ensures that the client2 gets the update event. This implies that server2
   * clones the old value prior to applying delta received from server1 and then processes the CQ.
   */
  @Test
  public void testC2CDeltaPropagationWithCQ() throws Exception {
    initialise(false/* clone */, new String[] {CQs[1]}, true/* CQ */, true/* RI */,
        true/* enable delta */);
    client.invoke(ClientToServerDeltaDUnitTest.class, "createKeys",
        new Object[] {new String[] {KEY1}});
    client.invoke(ClientToServerDeltaDUnitTest.class, "putDeltaForCQ", new Object[] {KEY1,
        new Integer(1), new Integer[] {1}, new Boolean[] {false, true, false, false}});

    client.invoke(() -> ClientToServerDeltaDUnitTest.putLastKeyWithDelta());

    client2.invoke(() -> ClientToServerDeltaDUnitTest.waitForLastKey());
    client2.invoke(() -> ClientToServerDeltaDUnitTest.verifyDeltaReceived());
  }

  /**
   * This test ensures that a server sends delta bytes to a client even if that client is not
   * interested in that event but is getting the event only because the event satisfies a CQ which
   * the client has registered with the server.
   */
  @Test
  public void testC2CDeltaPropagationWithCQWithoutRI() throws Exception {
    initialise(false/* clone */, new String[] {CQs[1]}, true/* CQ */, false/* RI */,
        true/* enable delta */);

    client.invoke(ClientToServerDeltaDUnitTest.class, "createKeys",
        new Object[] {new String[] {KEY1}});
    client.invoke(ClientToServerDeltaDUnitTest.class, "putDeltaForCQ", new Object[] {KEY1,
        new Integer(1), new Integer[] {1}, new Boolean[] {false, true, false, false}});

    client.invoke(() -> ClientToServerDeltaDUnitTest.putLastKeyWithDelta());

    client2.invoke(() -> ClientToServerDeltaDUnitTest.waitForLastKey());
    server2.invoke(() -> ClientToServerDeltaDUnitTest.verifyDeltaSent(Integer.valueOf(1)));
  }

  @Test
  public void testClientSendsFullValueToServerWhenDeltaOffAtServer() {
    initialise2(false/* clone */, new String[] {CQs[1]}, false/* CQ */, true/* RI */,
        false/* enable delta */);
    // set expected value on server
    server.invoke(() -> ClientToServerDeltaDUnitTest.setFirstSecondUpdate(putDelta[0],
        (Integer) putDelta[2]));

    client.invoke(() -> ClientToServerDeltaDUnitTest.putDelta(DELTA_KEY));

    assertFalse("Delta Propagation feature used at client.",
        (Boolean) client.invoke(() -> DeltaTestImpl.toDeltaFeatureUsed()));
    assertFalse("Delta Propagation feature used at server.",
        (Boolean) server.invoke(() -> DeltaTestImpl.fromDeltaFeatureUsed()));
    assertFalse(
        "Failures at client while calculating delta. But delta-propagation is false at server.",
        ((Boolean) client.invoke(() -> DeltaTestImpl.isToDeltaFailure())).booleanValue());
    assertFalse(
        "Failures at server while applying delta. But delta-propagation is false at server.",
        (Boolean) server.invoke(() -> DeltaTestImpl.isFromDeltaFailure()));
  }

  @Test
  public void testC2SDeltaPropagationWithOldValueInvalidatedAtServer() throws Exception {
    String key = "DELTA_KEY";
    Integer port1 = (Integer) server
        .invoke(() -> ClientToServerDeltaDUnitTest.createServerCache(false, false, false, true));
    createClientCache("localhost", port1, false, false, false, null, false, false);

    LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
    region.put(key, new DeltaTestImpl());

    server.invoke(() -> ClientToServerDeltaDUnitTest.doInvalidate(key));
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
    assertTrue(
        "Expected 1 fromDelta() invokation but was " + DeltaTestImpl.getFromDeltaInvokations(),
        DeltaTestImpl.getFromDeltaInvokations() == 1);
  }

  public static void verifyDeltaSent(Integer deltas) {
    CacheClientNotifier ccn = ((CacheServerImpl) cache.getCacheServers().toArray()[0]).getAcceptor()
        .getCacheClientNotifier();

    int numOfDeltasSent = ((CacheClientProxy) ccn.getClientProxies().toArray()[0]).getStatistics()
        .getDeltaMessagesSent();
    assertTrue("Expected " + deltas + " deltas to be sent but " + numOfDeltasSent + " were sent.",
        numOfDeltasSent == deltas);
  }

  public static void checkFromdeltaCounter() {
    assertTrue(
        "FromDelta counters do not match, expected: " + (NO_PUT_OPERATION - 1) + ", actual: "
            + DeltaTestImpl.getFromDeltaInvokations(),
        DeltaTestImpl.getFromDeltaInvokations() >= (NO_PUT_OPERATION - 1));
  }

  public static void checkTodeltaCounter(Integer count) {
    assertTrue(
        "ToDelta counters do not match, expected: " + count.intValue() + ", actual: "
            + DeltaTestImpl.getToDeltaInvokations(),
        DeltaTestImpl.getToDeltaInvokations() >= count.intValue());
  }

  public static void checkDeltaFeatureNotUsed() {
    assertTrue("Delta Propagation feature used.", !(DeltaTestImpl.deltaFeatureUsed()));
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
    assertTrue(
        "Delta applied :" + ((CSDeltaTestImpl) region.get(DELTA_KEY)).getDeltaApplied()
            + "\n Delta sent :" + deltaSent,
        ((CSDeltaTestImpl) region.get(DELTA_KEY)).getDeltaApplied() == deltaSent);
  }

  public static void checkForDelta() {
    assertTrue("Delta sent to EMPTY data policy region",
        DeltaTestImpl.getFromDeltaInvokations() == 0);
  }

  public static void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return ClientToServerDeltaDUnitTest.lastKeyReceived;
      }

      @Override
      public String description() {
        return "Last key NOT received.";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  static class CSDeltaTestImpl extends DeltaTestImpl {

    int deltaSent = 0;
    int deltaApplied = 0;

    public CSDeltaTestImpl() {}

    @Override
    public void toDelta(DataOutput out) throws IOException {
      super.toDelta(out);
      deltaSent++;
    }

    @Override
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

    @Override
    public String toString() {
      return "CSDeltaTestImpl[deltaApplied=" + deltaApplied + "]" + super.toString();
    }
  }

}
