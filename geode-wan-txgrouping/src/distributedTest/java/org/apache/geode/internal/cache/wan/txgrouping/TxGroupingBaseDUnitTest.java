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
package org.apache.geode.internal.cache.wan.txgrouping;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class TxGroupingBaseDUnitTest implements Serializable {

  protected static final String REGION_NAME = "TheRegion";

  protected final String shipmentRegionName = "ShipmentsRegion";
  protected final String customerRegionName = "CustomersRegion";
  protected final String orderRegionName = "OrdersRegion";

  protected static LocatorLauncher locatorLauncher;
  protected static ServerLauncher serverLauncher;

  protected VM londonLocatorVM;
  protected VM newYorkLocatorVM;
  protected VM newYorkServerVM;
  protected VM londonServer1VM;
  protected VM londonServer2VM;
  protected VM londonServer3VM;
  protected VM londonServer4VM;
  protected VM[] londonServersVM;

  protected String newYorkName;

  protected int londonId;
  protected int newYorkId;

  protected int londonLocatorPort;
  protected int newYorkLocatorPort;

  protected int newYorkReceiverPort;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private static List<Integer> dispatcherThreads = new ArrayList<>(Arrays.asList(1, 3, 5));
  // this will be set for each test method run with one of the values from above list
  private static int numDispatcherThreadsForTheRun = 1;

  @Before
  public void setUp() {
    londonLocatorVM = getVM(0);
    newYorkLocatorVM = getVM(1);
    newYorkServerVM = getVM(2);
    londonServer1VM = getVM(3);
    londonServer2VM = getVM(4);
    londonServer3VM = getVM(5);
    londonServer4VM = getVM(6);
    londonServersVM = new VM[] {londonServer1VM, londonServer2VM, londonServer3VM, londonServer4VM};

    newYorkName = "ny";

    londonId = 1;
    newYorkId = 2;

    int[] ports = getRandomAvailableTCPPorts(3);
    londonLocatorPort = ports[0];
    newYorkLocatorPort = ports[1];
    newYorkReceiverPort = ports[2];

    newYorkLocatorVM.invoke("start New York locator", () -> {
      Properties config = createLocatorConfig(newYorkId, newYorkLocatorPort, londonLocatorPort);
      cacheRule.createCache(config);
    });

    londonLocatorVM.invoke("start London locator", () -> {
      Properties config = createLocatorConfig(londonId, londonLocatorPort, newYorkLocatorPort);
      cacheRule.createCache(config);
    });
    Collections.shuffle(dispatcherThreads);
    int dispatcherThreadsNo = dispatcherThreads.get(0);
    Invoke.invokeInEveryVM(() -> setNumDispatcherThreadsForTheRun(dispatcherThreadsNo));

  }

  @After
  public void tearDown() {
    newYorkServerVM.invoke(() -> {
      if (serverLauncher != null) {
        serverLauncher.stop();
        serverLauncher = null;
      }
    });

    for (VM server : londonServersVM) {
      server.invoke(() -> {
        if (serverLauncher != null) {
          serverLauncher.stop();
          serverLauncher = null;
        }
      });
    }

    newYorkLocatorVM.invoke(() -> {
      if (locatorLauncher != null) {
        locatorLauncher.stop();
        locatorLauncher = null;
      }
    });

    londonLocatorVM.invoke(() -> {
      if (locatorLauncher != null) {
        locatorLauncher.stop();
        locatorLauncher = null;
      }
    });
  }

  protected Properties createLocatorConfig(int systemId, int locatorPort, int remoteLocatorPort) {
    Properties config = new Properties();
    config.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(systemId));
    config.setProperty(LOCATORS, "localhost[" + locatorPort + ']');
    config.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocatorPort + ']');
    config.setProperty(START_LOCATOR,
        "localhost[" + locatorPort + "],server=true,peer=true,hostname-for-clients=localhost");
    return config;
  }

  protected void startServerWithSender(int systemId, int locatorPort, int remoteSystemId,
      String remoteName, boolean isParallel, boolean groupTransactionEvents, int batchSize)
      throws IOException {
    startServerWithSender(systemId, locatorPort, remoteSystemId, remoteName, isParallel,
        groupTransactionEvents, batchSize, 0);
  }

  protected void startServerWithSender(int systemId, int locatorPort, int remoteSystemId,
      String remoteName, boolean isParallel, boolean groupTransactionEvents, int batchSize,
      int dispatcherThreads) throws IOException {
    cacheRule.createCache(createServerConfig(locatorPort));

    String uniqueName = "server-" + systemId;
    File[] dirs = new File[] {temporaryFolder.newFolder(uniqueName)};

    GatewaySenderFactory senderFactory = createGatewaySenderFactory(dirs, uniqueName);
    senderFactory.setParallel(isParallel);
    senderFactory.setGroupTransactionEvents(groupTransactionEvents);
    senderFactory.setBatchSize(batchSize);
    if (dispatcherThreads > 0) {
      senderFactory.setDispatcherThreads(dispatcherThreads);
    }
    GatewaySender sender = senderFactory.create(remoteName, remoteSystemId);
    sender.start();
  }

  protected void startServerWithReceiver(int locatorPort,
      int receiverPort) throws IOException {
    startServerWithReceiver(locatorPort, receiverPort, true);
  }

  protected void startServerWithReceiver(int locatorPort,
      int receiverPort, boolean start) throws IOException {
    cacheRule.createCache(createServerConfig(locatorPort));

    GatewayReceiverFactory receiverFactory = createGatewayReceiverFactory(receiverPort);
    GatewayReceiver receiver = receiverFactory.create();
    if (start) {
      receiver.start();
    }
  }

  protected void startReceiver() throws IOException {
    cacheRule.getCache().getGatewayReceivers().iterator().next().start();
  }

  protected GatewayReceiverFactory createGatewayReceiverFactory(int receiverPort) {
    GatewayReceiverFactory receiverFactory = cacheRule.getCache().createGatewayReceiverFactory();

    receiverFactory.setStartPort(receiverPort);
    receiverFactory.setEndPort(receiverPort);
    receiverFactory.setManualStart(true);
    return receiverFactory;
  }

  protected Properties createServerConfig(int locatorPort) {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + locatorPort + ']');
    return config;
  }

  protected GatewaySenderFactory createGatewaySenderFactory(File[] dirs, String diskStoreName) {
    InternalGatewaySenderFactory senderFactory =
        (InternalGatewaySenderFactory) cacheRule.getCache().createGatewaySenderFactory();

    senderFactory.setMaximumQueueMemory(100);
    senderFactory.setBatchSize(10);
    senderFactory.setBatchConflationEnabled(false);
    senderFactory.setManualStart(true);
    senderFactory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    senderFactory.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);

    DiskStoreFactory dsf = cacheRule.getCache().createDiskStoreFactory();
    DiskStore store = dsf.setDiskDirs(dirs).create(diskStoreName);
    senderFactory.setDiskStoreName(store.getName());

    return senderFactory;
  }

  protected boolean isRunning(GatewaySender sender) {
    return sender != null && sender.isRunning();
  }

  protected void validateRegionSize(String regionName, final int regionSize) {
    final Region<Object, Object> r = cacheRule.getCache().getRegion(SEPARATOR + regionName);
    assertThat(r).isNotNull();
    await().untilAsserted(() -> assertThat(r.keySet().size()).isEqualTo(regionSize));
  }

  protected List<Integer> getSenderStats(String senderId, final int expectedQueueSize) {
    AbstractGatewaySender sender =
        (AbstractGatewaySender) cacheRule.getCache().getGatewaySender(senderId);
    GatewaySenderStats statistics = sender.getStatistics();
    if (expectedQueueSize != -1) {
      final RegionQueue regionQueue;
      regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
      if (sender.isParallel()) {
        ConcurrentParallelGatewaySenderQueue parallelGatewaySenderQueue =
            (ConcurrentParallelGatewaySenderQueue) regionQueue;
        PartitionedRegion pr =
            parallelGatewaySenderQueue.getRegions().toArray(new PartitionedRegion[1])[0];
      }
      await()
          .untilAsserted(() -> assertThat(regionQueue.size()).isEqualTo(expectedQueueSize));
    }
    List<Integer> stats = new ArrayList<>();
    stats.add(statistics.getEventQueueSize());
    stats.add(statistics.getEventsReceived());
    stats.add(statistics.getEventsQueued());
    stats.add(statistics.getEventsDistributed());
    stats.add(statistics.getBatchesDistributed());
    stats.add(statistics.getBatchesRedistributed());
    stats.add(statistics.getEventsFiltered());
    stats.add(statistics.getEventsNotQueuedConflated());
    stats.add(statistics.getEventsConflatedFromBatches());
    stats.add(statistics.getConflationIndexesMapSize());
    stats.add(statistics.getSecondaryEventQueueSize());
    stats.add(statistics.getEventsProcessedByPQRM());
    stats.add(statistics.getEventsExceedingAlertThreshold());
    stats.add((int) statistics.getBatchesWithIncompleteTransactions());
    return stats;
  }

  protected GatewaySender getGatewaySender(String senderId) {
    Set<GatewaySender> senders = cacheRule.getCache().getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    return sender;
  }

  protected void doTxPuts(String regionName, final long putsPerTransaction,
      final long transactions) {
    doTxPuts(regionName, putsPerTransaction, transactions, 0);
  }

  protected void doTxPuts(String regionName, final long putsPerTransaction,
      final long transactions, long initialKeyId) {
    Region<Object, Object> region = cacheRule.getCache().getRegion(Region.SEPARATOR + regionName);
    CacheTransactionManager mgr = cacheRule.getCache().getCacheTransactionManager();
    for (int i = 0; i < transactions; i++) {
      long keyId = initialKeyId + (i * putsPerTransaction);
      doOneTxWithPuts(region, mgr, putsPerTransaction, keyId);
    }
  }

  private void doOneTxWithPuts(Region<Object, Object> region, CacheTransactionManager mgr,
      long putsPerTransaction, long initialKeyId) {
    mgr.begin();
    for (int j = 0; j < putsPerTransaction; j++) {
      long key = initialKeyId + j;
      String value = "Value_" + key;
      region.put(key, value);
    }
    mgr.commit();
  }

  protected void checkGatewayReceiverStats(int processBatches, int eventsReceived,
      int creates) {
    checkGatewayReceiverStats(processBatches, eventsReceived, creates, false);
  }

  protected void checkGatewayReceiverStats(int processBatches, int eventsReceived,
      int creates, boolean isExact) {
    Set<GatewayReceiver> gatewayReceivers = cacheRule.getCache().getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertThat(stats).isInstanceOf(GatewayReceiverStats.class);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats) stats;
    if (isExact) {
      assertThat(gatewayReceiverStats.getProcessBatchRequests()).isEqualTo(processBatches);
    } else {
      assertThat(gatewayReceiverStats.getProcessBatchRequests())
          .isGreaterThanOrEqualTo(processBatches);
    }
    assertThat(eventsReceived).isEqualTo(gatewayReceiverStats.getEventsReceived());
    assertThat(creates).isEqualTo(gatewayReceiverStats.getCreateRequest());
  }

  protected void doTxPutsWithRetryIfError(String regionName, final long putsPerTransaction,
      final long transactions, long initialKeyId) {
    Region<Object, Object> region = cacheRule.getCache().getRegion(Region.SEPARATOR + regionName);
    CacheTransactionManager mgr = cacheRule.getCache().getCacheTransactionManager();
    for (int i = 0; i < transactions; i++) {
      long keyId = initialKeyId + (i * putsPerTransaction);
      doOneTxWithPutsWithRetryIfError(region, mgr, putsPerTransaction, keyId);
    }
  }

  private void doOneTxWithPutsWithRetryIfError(Region<Object, Object> region,
      CacheTransactionManager mgr, long putsPerTransaction, long initialKeyId) {
    while (true) {
      try {
        mgr.begin();
        for (int j = 0; j < putsPerTransaction; j++) {
          long key = initialKeyId + j;
          String value = "Value_" + key;
          region.put(key, value);
        }
        mgr.commit();
        return;
      } catch (TransactionException ignore) {
      } catch (IllegalStateException ignore) {
        try {
          mgr.rollback();
        } catch (Exception ignored) {
        }
      }
    }
  }

  public void createCustomerOrderShipmentPartitionedRegion(String senderId) {
    createCustomerOrderShipmentPartitionedRegion(senderId, 0);
  }

  public void createCustomerOrderShipmentPartitionedRegion(String senderId, int redundantCopies) {
    RegionFactory<Object, Object> fact =
        cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    if (senderId != null) {
      fact.addGatewaySenderId(senderId);
    }

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    paf.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(paf.create());
    fact.create(customerRegionName);

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setColocatedWith(customerRegionName)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    fact = cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    if (senderId != null) {
      fact.addGatewaySenderId(senderId);
    }
    fact.setPartitionAttributes(paf.create());
    fact.create(orderRegionName);

    paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setColocatedWith(orderRegionName)
        .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    fact = cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    if (senderId != null) {
      fact.addGatewaySenderId(senderId);
    }
    fact.setPartitionAttributes(paf.create());
    fact.create(shipmentRegionName);
  }

  public void doOrderAndShipmentPutsInsideTransactions(int customerId, int eventsPerTransaction,
      int transactions) {
    doOrderAndShipmentPutsInsideTransactions(customerId, eventsPerTransaction, transactions, false);
  }

  public void doOrderAndShipmentPutsInsideTransactions(int customerId, int eventsPerTransaction,
      int transactions, boolean retryIfError) {
    CacheTransactionManager cacheTransactionManager =
        cacheRule.getCache().getCacheTransactionManager();
    for (int i = 0; i < transactions; i++) {
      int keyId = i * eventsPerTransaction;
      if (retryIfError) {
        doOneTxOrderAndShipmentPutsWithRetryIfError(cacheTransactionManager, keyId,
            eventsPerTransaction, customerId);
      } else {
        doOneTxOrderAndShipmentPuts(cacheTransactionManager, keyId, eventsPerTransaction,
            customerId);
      }
    }
  }

  private void doOneTxOrderAndShipmentPuts(
      CacheTransactionManager cacheTransactionManager, int keyId, int eventsPerTransaction,
      int customerId) {
    cacheTransactionManager.begin();
    doOneOrderAndShipmentPuts(keyId, eventsPerTransaction, customerId);
    cacheTransactionManager.commit();
  }

  private void doOneTxOrderAndShipmentPutsWithRetryIfError(
      CacheTransactionManager cacheTransactionManager, int keyId, int eventsPerTransaction,
      int customerId) {
    while (true) {
      try {
        cacheTransactionManager.begin();
        doOneOrderAndShipmentPuts(keyId, eventsPerTransaction, customerId);
        cacheTransactionManager.commit();
        break;
      } catch (TransactionException exception) {
      } catch (IllegalStateException exception) {
        try {
          cacheTransactionManager.rollback();
        } catch (Exception ignored) {
        }
      }
    }
  }

  private void doOneOrderAndShipmentPuts(int keyId, int eventsPerTransaction, int customerId) {
    Region<Object, Object> orderRegion = cacheRule.getCache().getRegion(orderRegionName);
    Region<Object, Object> shipmentRegion = cacheRule.getCache().getRegion(shipmentRegionName);
    OrderId orderId = new OrderId(keyId, new CustId(customerId));
    orderRegion.put(orderId, new Order());
    for (int i = 0; i < eventsPerTransaction - 1; i++) {
      ShipmentId shipmentId = new ShipmentId(keyId + i, orderId);
      shipmentRegion.put(shipmentId, new Shipment());
    }
  }

  protected Integer getRegionSize(String regionName) {
    final Region<Object, Object> r = cacheRule.getCache().getRegion(SEPARATOR + regionName);
    return r.keySet().size();
  }

  protected void checkGatewayReceiverStatsHA(int processBatches, int eventsReceived,
      int creates) {
    Set<GatewayReceiver> gatewayReceivers = cacheRule.getCache().getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();
    assertThat(stats).isInstanceOf(GatewayReceiverStats.class);

    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats) stats;
    assertThat(gatewayReceiverStats.getProcessBatchRequests())
        .isGreaterThanOrEqualTo(processBatches);
    assertThat(gatewayReceiverStats.getEventsReceived()).isGreaterThanOrEqualTo(eventsReceived);
    assertThat(gatewayReceiverStats.getCreateRequest()).isGreaterThanOrEqualTo(creates);
  }

  protected void putGivenKeyValues(String regionName, Map<?, ?> keyValues) {
    Region<Object, Object> r = cacheRule.getCache().getRegion(SEPARATOR + regionName);
    assertThat(r).isNotNull();
    for (Object key : keyValues.keySet()) {
      r.put(key, keyValues.get(key));
    }
  }

  protected void checkConflatedStats(String senderId, final int eventsConflated) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertThat(statistics.getEventsNotQueuedConflated()).isEqualTo(eventsConflated);
  }

  protected GatewaySenderStats getGatewaySenderStats(String senderId) {
    GatewaySender sender = cacheRule.getCache().getGatewaySender(senderId);
    return ((AbstractGatewaySender) sender).getStatistics();
  }

  protected void validateGatewaySenderQueueAllBucketsDrained(final String senderId) {
    GatewaySender sender = getGatewaySender(senderId);
    final AbstractGatewaySender abstractSender = (AbstractGatewaySender) sender;
    await().untilAsserted(() -> assertThat(abstractSender.getEventQueueSize()).isEqualTo(0));
    await()
        .untilAsserted(() -> assertThat(abstractSender.getSecondaryEventQueueSize()).isEqualTo(0));
  }

  public static void setNumDispatcherThreadsForTheRun(int numThreads) {
    numDispatcherThreadsForTheRun = numThreads;
  }
}
