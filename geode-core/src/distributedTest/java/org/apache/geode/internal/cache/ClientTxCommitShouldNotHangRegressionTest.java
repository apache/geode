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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.LocatorTestBase;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Client should NOT hang in tx commit when server does not define all regions.
 *
 * <pre>
 * 1. Server side
 *    ServerA: creating only RegionA with replicated region and setting distributed-ack
 *    ServerB: creating only RegionB with replicated region and setting distributed-ack
 * 2. Client side
 *    ClientTransactionA: put something and commit to RegionA with using transaction
 *    ClientTransactionB: put something and commit to RegionB with using transaction
 *
 * Start server processes
 *   (1) start ServerA
 *   (2) start ServerB
 *
 * Execute clients
 *   (1) execute ClientTransactionA -> you can finish it successfully.
 *   (2) execute ClientTransactionB -> no responses from ServerB while committing the transaction.
 *   Then if stopping ServerA process, then the response is back from ServerB.
 * </pre>
 *
 * <p>
 * TRAC #47667: Delay in Multi-transaction Committing for Replicated Regions with distributed-ack
 * Scope
 *
 * <p>
 * Better description of bug: Client hangs in tx commit when server does not define all regions.
 * Underlying exception that caused the hang was:
 *
 * <pre>
 * [severe 2013/05/15 17:53:07.037 PDT gemfire_2_1 <P2P message reader for mclaren(16125)<v1>:49192/33706 SHARED=true ORDERED=false UID=2> tid=0x31] Error deserializing message
 *     java.lang.IllegalArgumentException: Illegal initial capacity: -1
 *     at java.util.HashMap.<init>(HashMap.java:172)
 *     at java.util.HashMap.<init>(HashMap.java:199)
 *     at java.util.HashSet.<init>(HashSet.java:125)
 *     at com.gemstone.gemfire.internal.cache.TXRegionLockRequestImpl.readEntryKeySet(TXRegionLockRequestImpl.java:108)
 *     at com.gemstone.gemfire.internal.cache.TXRegionLockRequestImpl.fromData(TXRegionLockRequestImpl.java:89)
 *     at com.gemstone.gemfire.internal.cache.TXRegionLockRequestImpl.createFromData(TXRegionLockRequestImpl.java:135)
 *     at com.gemstone.gemfire.internal.cache.locks.TXLockBatch.fromData(TXLockBatch.java:111)
 *     at com.gemstone.gemfire.internal.DSFIDFactory.readTXLockBatch(DSFIDFactory.java:2474)
 *     at com.gemstone.gemfire.internal.DSFIDFactory.create(DSFIDFactory.java:726)
 *     at com.gemstone.gemfire.internal.InternalDataSerializer.basicReadObject(InternalDataSerializer.java:2632)
 *     at com.gemstone.gemfire.DataSerializer.readObject(DataSerializer.java:3217)
 *     at com.gemstone.gemfire.distributed.internal.locks.DLockRequestProcessor$DLockResponseMessage.fromData(DLockRequestProcessor.java:1228)
 *     at com.gemstone.gemfire.internal.DSFIDFactory.readDLockResponseMessage(DSFIDFactory.java:1325)
 *     at com.gemstone.gemfire.internal.DSFIDFactory.create(DSFIDFactory.java:235)
 *     at com.gemstone.gemfire.internal.InternalDataSerializer.readDSFID(InternalDataSerializer.java:2524)
 *     at com.gemstone.gemfire.internal.tcp.Connection.processNIOBuffer(Connection.java:3484)
 *     at com.gemstone.gemfire.internal.tcp.Connection.runNioReader(Connection.java:1794)
 *     at com.gemstone.gemfire.internal.tcp.Connection.run(Connection.java:1675)
 *     at java.lang.Thread.run(Thread.java:662)
 * </pre>
 */

@SuppressWarnings("serial")
public class ClientTxCommitShouldNotHangRegressionTest extends LocatorTestBase {

  private static ClientCache clientCache;

  private String hostName;
  private int locatorPort;
  private String region1Name;
  private String region2Name;

  private VM client;

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUpTest() throws Exception {
    VM locator = getHost(0).getVM(0);
    VM server1 = getHost(0).getVM(1);
    VM server2 = getHost(0).getVM(2);
    client = getHost(0).getVM(3);

    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    region1Name = uniqueName + "_R1";
    region2Name = uniqueName + "_R2";

    hostName = NetworkUtils.getServerHostName();

    locatorPort = locator.invoke("Start locator", () -> startLocator(hostName, ""));

    String locators = getLocatorString(hostName, locatorPort);

    server1.invoke("Start server",
        () -> startBridgeServer(new String[] {region1Name}, locators, new String[] {region1Name}));
    server2.invoke("Start server",
        () -> startBridgeServer(new String[] {region2Name}, locators, new String[] {region2Name}));

    client.invoke("Create client", () -> {
      ClientCacheFactory ccf = new ClientCacheFactory();
      ccf.addPoolLocator(hostName, locatorPort);

      clientCache = ccf.create();

      PoolManager.createFactory().addLocator(hostName, locatorPort).setServerGroup(region1Name)
          .create(region1Name);
      PoolManager.createFactory().addLocator(hostName, locatorPort).setServerGroup(region2Name)
          .create(region2Name);

      clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .setPoolName(region1Name).create(region1Name);
      clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .setPoolName(region2Name).create(region2Name);
    });
  }

  @After
  public void tearDownTest() throws Exception {
    disconnectAllFromDS();

    invokeInEveryVM(() -> {
      clientCache = null;
    });
  }

  @Test
  public void clientTxCommitShouldNotHangWhenServerDoesNotDefineAllRegions() {
    client.invoke("insert data in transaction", () -> {
      Region<Integer, String> region1 = clientCache.getRegion(region1Name);
      Region<Integer, String> region2 = clientCache.getRegion(region2Name);

      CacheTransactionManager transactionManager = clientCache.getCacheTransactionManager();

      transactionManager.begin();
      region1.put(1, "value1");
      transactionManager.commit();

      transactionManager.begin();
      region2.put(2, "value2");
      transactionManager.commit();
    });
  }
}
