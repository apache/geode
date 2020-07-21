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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.AsyncEventQueueMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.MBeanUtil;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.pdx.SimpleClass1;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WANTestBase extends DistributedTestCase {

  protected static Cache cache;
  protected static Region region;

  protected static PartitionedRegion customerRegion;
  protected static PartitionedRegion orderRegion;
  protected static PartitionedRegion shipmentRegion;

  protected static final String customerRegionName = "CUSTOMER";
  protected static final String orderRegionName = "ORDER";
  protected static final String shipmentRegionName = "SHIPMENT";

  protected static VM vm0;
  protected static VM vm1;
  protected static VM vm2;
  protected static VM vm3;
  protected static VM vm4;
  protected static VM vm5;
  protected static VM vm6;
  protected static VM vm7;

  protected static QueueListener listener1;
  protected static QueueListener listener2;

  protected static AsyncEventListener eventListener1;
  protected static AsyncEventListener eventListener2;

  private static final long MAX_WAIT = 10000;

  protected static GatewayEventFilter eventFilter;

  protected static List<Integer> dispatcherThreads = new ArrayList<Integer>(Arrays.asList(1, 3, 5));
  // this will be set for each test method run with one of the values from above list
  protected static int numDispatcherThreadsForTheRun = 1;

  private static final Logger logger = LogService.getLogger();

  @BeforeClass
  public static void beforeClassWANTestBase() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);
    vm4 = getHost(0).getVM(4);
    vm5 = getHost(0).getVM(5);
    vm6 = getHost(0).getVM(6);
    vm7 = getHost(0).getVM(7);
  }

  @Before
  public void setUpWANTestBase() throws Exception {
    shuffleNumDispatcherThreads();
    Invoke.invokeInEveryVM(() -> setNumDispatcherThreadsForTheRun(dispatcherThreads.get(0)));
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("Software caused connection abort");
    IgnoredException.addIgnoredException("Connection reset");
    postSetUpWANTestBase();
  }

  protected void postSetUpWANTestBase() throws Exception {
    // nothing
  }

  public static void shuffleNumDispatcherThreads() {
    Collections.shuffle(dispatcherThreads);
  }

  public static void setNumDispatcherThreadsForTheRun(int numThreads) {
    numDispatcherThreadsForTheRun = numThreads;
  }

  public static void stopOldLocator() {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
  }

  public static void createLocator(int dsId, int port, Set<String> localLocatorsList,
      Set<String> remoteLocatorsList) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    StringBuffer localLocatorBuffer = new StringBuffer(localLocatorsList.toString());
    localLocatorBuffer.deleteCharAt(0);
    localLocatorBuffer.deleteCharAt(localLocatorBuffer.lastIndexOf("]"));
    String localLocator = localLocatorBuffer.toString();
    localLocator = localLocator.replace(" ", "");

    props.setProperty(LOCATORS, localLocator);
    props.setProperty(START_LOCATOR,
        "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    StringBuffer remoteLocatorBuffer = new StringBuffer(remoteLocatorsList.toString());
    remoteLocatorBuffer.deleteCharAt(0);
    remoteLocatorBuffer.deleteCharAt(remoteLocatorBuffer.lastIndexOf("]"));
    String remoteLocator = remoteLocatorBuffer.toString();
    remoteLocator = remoteLocator.replace(" ", "");
    props.setProperty(REMOTE_LOCATORS, remoteLocator);
    test.startLocatorDistributedSystem(props);
  }

  private void startLocator(int dsId, int locatorPort, int startLocatorPort, int remoteLocPort,
      boolean startServerLocator) {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    props.setProperty(START_LOCATOR, "localhost[" + startLocatorPort + "],server="
        + startServerLocator + ",peer=true,hostname-for-clients=localhost");
    if (remoteLocPort != -1) {
      props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    }
    startLocatorDistributedSystem(props);
  }

  private void startLocatorDistributedSystem(Properties props) {
    // Start start the locator with a LOCATOR_DM_TYPE and not a NORMAL_DM_TYPE
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    try {
      getSystem(props);
    } finally {
      System.clearProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE);
    }
  }

  public static Integer createFirstLocatorWithDSId(int dsId) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, port, port, -1, true);
    return port;
  }

  public static Integer createFirstPeerLocator(int dsId) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, port, port, -1, false);
    return port;
  }

  public static Integer createSecondLocator(int dsId, int locatorPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, locatorPort, port, -1, true);
    return port;
  }

  public static Integer createSecondPeerLocator(int dsId, int locatorPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, locatorPort, port, -1, false);
    return port;
  }

  public static Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, port, port, remoteLocPort, true);
    return port;
  }

  public static void bringBackLocatorOnOldPort(int dsId, int remoteLocPort, int oldPort) {
    WANTestBase test = new WANTestBase();
    test.startLocator(dsId, oldPort, oldPort, remoteLocPort, true);
  }


  public static Integer createFirstRemotePeerLocator(int dsId, int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, port, port, remoteLocPort, false);
    return port;
  }

  public static Integer createSecondRemoteLocator(int dsId, int localPort, int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, localPort, port, remoteLocPort, true);
    return port;
  }

  public static Integer createSecondRemoteLocatorWithAPI(int dsId, int localPort, int remoteLocPort,
      String hostnameForClients) throws IOException {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + localPort + "]");
    props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    Locator locator = Locator.startLocatorAndDS(0, null, InetAddress.getByName("0.0.0.0"), props,
        true, true, hostnameForClients);
    return locator.getPort();
  }

  public static Integer createSecondRemotePeerLocator(int dsId, int localPort, int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    test.startLocator(dsId, localPort, port, remoteLocPort, false);
    return port;
  }

  public static int createReceiverInSecuredCache() {
    GatewayReceiverFactory fact = WANTestBase.cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("Failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }

  public static void createReplicatedRegion(String regionName, String senderIds, Boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      fact.setOffHeap(offHeap);
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
      exp2.remove();
    }
  }


  public static void createReplicatedProxyRegion(String regionName, String senderIds,
      Boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      fact.setOffHeap(offHeap);
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
      exp2.remove();
    }
  }

  public static void createNormalRegion(String regionName, String senderIds) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.LOCAL);
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }

    Region r = fact.create(regionName);
    assertNotNull(r);
  }

  public static void createPersistentReplicatedRegion(String regionName, String senderIds,
      Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }

    fact.setOffHeap(offHeap);
    Region r = fact.create(regionName);
    assertNotNull(r);
  }

  public static void createReplicatedRegionWithAsyncEventQueue(String regionName,
      String asyncQueueIds, Boolean offHeap) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE);
      if (asyncQueueIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(asyncQueueIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String asyncQueueId = tokenizer.nextToken();
          fact.addAsyncEventQueueId(asyncQueueId);
        }
      }

      fact.setOffHeap(offHeap);
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp1.remove();
    }
  }

  public static void createReplicatedRegionWithSenderAndAsyncEventQueue(String regionName,
      String senderIds, String asyncChannelId, Boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      fact.setOffHeap(offHeap);
      fact.addAsyncEventQueueId(asyncChannelId);
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
    }
  }

  public static void createReplicatedRegion(String regionName, String senderIds, Scope scope,
      DataPolicy policy, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory();
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }

    fact.setDataPolicy(policy);
    fact.setScope(scope);
    fact.setOffHeap(offHeap);
    Region r = fact.create(regionName);
    assertNotNull(r);
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous) {

    if (diskStoreName != null) {
      File directory = new File(
          asyncChannelId + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      File[] dirs1 = new File[] {directory};
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      dsf.create(diskStoreName);
    }

    AsyncEventListener asyncEventListener = new MyAsyncEventListener();

    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setPersistent(isPersistent);
    factory.setDiskStoreName(diskStoreName);
    factory.setDiskSynchronous(isDiskSynchronous);
    factory.setBatchConflationEnabled(isConflation);
    factory.setMaximumQueueMemory(maxMemory);
    factory.setParallel(isParallel);
    // set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    factory.create(asyncChannelId, asyncEventListener);
  }

  public static void createPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    createPartitionedRegion(regionName, senderIds, redundantCopies, totalNumBuckets, offHeap,
        RegionShortcut.PARTITION);
  }

  public static void createPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap, RegionShortcut shortcut) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(shortcut);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  // TODO:OFFHEAP: add offheap flavor
  public static void createPartitionedRegionWithPersistence(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      fact.setPartitionAttributes(pfact.create());
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createColocatedPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, String colocatedWith) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      pfact.setColocatedWith(colocatedWith);
      fact.setPartitionAttributes(pfact.create());
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void addSenderThroughAttributesMutator(String regionName, String senderIds) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    AttributesMutator mutator = r.getAttributesMutator();
    mutator.addGatewaySenderId(senderIds);
  }

  public static void addAsyncEventQueueThroughAttributesMutator(String regionName, String queueId) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    AttributesMutator mutator = r.getAttributesMutator();
    mutator.addAsyncEventQueueId(queueId);
  }

  public static void createPartitionedRegionAsAccessor(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION_PROXY);
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    Region r = fact.create(regionName);
    assertNotNull(r);
  }

  public static void createPartitionedRegionWithSerialParallelSenderIds(String regionName,
      String serialSenderIds, String parallelSenderIds, String colocatedWith, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    if (serialSenderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(serialSenderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    if (parallelSenderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(parallelSenderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setColocatedWith(colocatedWith);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r = fact.create(regionName);
    assertNotNull(r);
  }

  public static void createPersistentPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {

    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {

      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = fact.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createCustomerOrderShipmentPartitionedRegion(String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumBuckets)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      customerRegion =
          (PartitionedRegion) fact.create(customerRegionName);
      assertNotNull(customerRegion);
      logger.info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumBuckets)
          .setColocatedWith(customerRegionName)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      orderRegion =
          (PartitionedRegion) fact.create(orderRegionName);
      assertNotNull(orderRegion);
      logger.info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumBuckets)
          .setColocatedWith(orderRegionName)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      shipmentRegion =
          (PartitionedRegion) fact.create(shipmentRegionName);
      assertNotNull(shipmentRegion);
      logger.info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    } finally {
      exp.remove();
    }
  }

  public static void createColocatedPartitionedRegions(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r = fact.create(regionName);
    assertNotNull(r);

    pfact.setColocatedWith(r.getName());
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r1 = fact.create(regionName + "_child1");
    assertNotNull(r1);

    Region r2 = fact.create(regionName + "_child2");
    assertNotNull(r2);
  }

  public static void createColocatedPartitionedRegions2(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    if (senderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r = fact.create(regionName);
    assertNotNull(r);

    fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    pfact.setColocatedWith(r.getName());
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r1 = fact.create(regionName + "_child1");
    assertNotNull(r1);

    Region r2 = fact.create(regionName + "_child2");
    assertNotNull(r2);
  }

  public static void createCacheInVMs(Integer locatorPort, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> createCache(locatorPort));
    }
  }

  public static void addListenerToSleepAfterCreateEvent(int milliSeconds, final String regionName) {
    cache.getRegion(regionName).getAttributesMutator()
        .addCacheListener(new CacheListenerAdapter<Object, Object>() {
          @Override
          public void afterCreate(final EntryEvent<Object, Object> event) {
            try {
              Thread.sleep(milliSeconds);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        });
  }

  private static CacheListener myListener;

  public static void removeCacheListener() {
    cache.getRegion(getTestMethodName() + "_RR_1").getAttributesMutator()
        .removeCacheListener(myListener);

  }


  public static void createCache(Integer locPort) {
    createCache(false, locPort);
  }

  public static void createManagementCache(Integer locPort) {
    createCache(true, locPort);
  }

  public static void createCacheConserveSockets(Boolean conserveSockets, Integer locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    props.setProperty(CONSERVE_SOCKETS, conserveSockets.toString());
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  protected static void createCache(boolean management, Integer locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    if (management) {
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_START, "false");
      props.setProperty(JMX_MANAGER_PORT, "0");
      props.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    }
    props.setProperty(MCAST_PORT, "0");
    String logLevel = System.getProperty(LOG_LEVEL, "info");
    props.setProperty(LOG_LEVEL, logLevel);
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  protected static void createCacheWithSSL(Integer locPort) {
    WANTestBase test = new WANTestBase();

    boolean gatewaySslenabled = true;
    String gatewaySslprotocols = "any";
    String gatewaySslciphers = "any";
    boolean gatewaySslRequireAuth = true;

    Properties gemFireProps = test.getDistributedSystemProperties();
    gemFireProps.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    gemFireProps.put(GATEWAY_SSL_ENABLED, String.valueOf(gatewaySslenabled));
    gemFireProps.put(GATEWAY_SSL_PROTOCOLS, gatewaySslprotocols);
    gemFireProps.put(GATEWAY_SSL_CIPHERS, gatewaySslciphers);
    gemFireProps.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION, String.valueOf(gatewaySslRequireAuth));

    gemFireProps.put(GATEWAY_SSL_KEYSTORE_TYPE, "jks");
    // this uses the default.keystore which is all jdk compliant in geode-dunit module
    gemFireProps.put(GATEWAY_SSL_KEYSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD, "password");

    gemFireProps.setProperty(MCAST_PORT, "0");
    gemFireProps.setProperty(LOCATORS, "localhost[" + locPort + "]");

    logger.info("Starting cache ds with following properties \n" + gemFireProps);

    InternalDistributedSystem ds = test.getSystem(gemFireProps);
    cache = CacheFactory.create(ds);
  }

  public static void createCache_PDX(Integer locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);

    cache = new InternalCacheBuilder(props)
        .setPdxPersistent(true)
        .setPdxDiskStore("PDX_TEST")
        .setIsExistingOk(false)
        .create(ds);

    File pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {pdxDir};
    dsf.setDiskDirs(dirs1).setMaxOplogSize(1).create("PDX_TEST");
  }

  public static void createCache(Integer locPort1, Integer locPort2) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort1 + "],localhost[" + locPort2 + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public static void createCacheWithoutLocator(Integer mCastPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "" + mCastPort);
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  /**
   * Method that creates a cache server
   *
   * @return Integer Port on which the server is started.
   */
  public static Integer createCacheServer() {
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    server1.setPort(0);
    try {
      server1.start();
    } catch (IOException e) {
      org.apache.geode.test.dunit.Assert.fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }

  /**
   * Returns a Map that contains the count for number of cache server and number of Receivers.
   *
   */
  public static Map getCacheServers() {
    List cacheServers = cache.getCacheServers();

    Map cacheServersMap = new HashMap();
    Iterator itr = cacheServers.iterator();
    int bridgeServerCounter = 0;
    int receiverServerCounter = 0;
    while (itr.hasNext()) {
      CacheServerImpl cacheServer = (CacheServerImpl) itr.next();
      if (cacheServer.getAcceptor().isGatewayReceiver()) {
        receiverServerCounter++;
      } else {
        bridgeServerCounter++;
      }
    }
    cacheServersMap.put("BridgeServer", bridgeServerCounter);
    cacheServersMap.put("ReceiverServer", receiverServerCounter);
    return cacheServersMap;
  }

  public static void startSenderInVMs(String senderId, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> startSender(senderId));
    }
  }

  public static void startSenderInVMsAsync(String senderId, VM... vms) {
    List<AsyncInvocation> tasks = new LinkedList<>();
    for (VM vm : vms) {
      tasks.add(vm.invokeAsync(() -> startSender(senderId)));
    }
    for (AsyncInvocation invocation : tasks) {
      try {
        invocation.await();
      } catch (InterruptedException e) {
        fail("Starting senders was interrupted");
      }
    }
  }


  public static void startSenderwithCleanQueuesInVMsAsync(String senderId, VM... vms) {
    List<AsyncInvocation> tasks = new LinkedList<>();
    for (VM vm : vms) {
      tasks.add(vm.invokeAsync(() -> startSenderwithCleanQueues(senderId)));
    }
    for (AsyncInvocation invocation : tasks) {
      try {
        invocation.await();
      } catch (InterruptedException e) {
        fail("Starting senders was interrupted");
      }
    }
  }

  public static void startSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");

    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.start();
    } finally {
      exp.remove();
      exp1.remove();
      exln.remove();
    }

  }

  public static void startSenderwithCleanQueues(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");

    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.startWithCleanQueue();
    } finally {
      exp.remove();
      exp1.remove();
      exln.remove();
    }

  }

  public static void enableConflation(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender) s;
        break;
      }
    }
    sender.test_setBatchConflationEnabled(true);
  }

  public static Map getSenderToReceiverConnectionInfo(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    Map connectionInfo = null;
    if (!sender.isParallel() && ((AbstractGatewaySender) sender).isPrimary()) {
      connectionInfo = new HashMap();
      GatewaySenderEventDispatcher dispatcher =
          ((AbstractGatewaySender) sender).getEventProcessor().getDispatcher();
      if (dispatcher instanceof GatewaySenderEventRemoteDispatcher) {
        ServerLocation serverLocation =
            ((GatewaySenderEventRemoteDispatcher) dispatcher).getConnection(false).getServer();
        connectionInfo.put("serverHost", serverLocation.getHostName());
        connectionInfo.put("serverPort", serverLocation.getPort());

      }
    }
    return connectionInfo;
  }

  public static void moveAllPrimaryBuckets(String senderId, final DistributedMember destination,
      final String regionName) {

    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    final RegionQueue regionQueue;
    regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
    if (sender.isParallel()) {
      ConcurrentParallelGatewaySenderQueue parallelGatewaySenderQueue =
          (ConcurrentParallelGatewaySenderQueue) regionQueue;
      PartitionedRegion prQ =
          parallelGatewaySenderQueue.getRegions().toArray(new PartitionedRegion[1])[0];

      Set<Integer> primaryBucketIds = prQ.getDataStore().getAllLocalPrimaryBucketIds();
      for (int bid : primaryBucketIds) {
        movePrimary(destination, regionName, bid);
      }

      // double check after moved all primary buckets
      primaryBucketIds = prQ.getDataStore().getAllLocalPrimaryBucketIds();
      assertTrue(primaryBucketIds.isEmpty());
    }
  }

  public static void movePrimary(final DistributedMember destination, final String regionName,
      final int bucketId) {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

    BecomePrimaryBucketResponse response = BecomePrimaryBucketMessage
        .send((InternalDistributedMember) destination, region, bucketId, true);
    assertNotNull(response);
    assertTrue(response.waitForResponse());
  }

  public static int getSecondaryQueueSizeInStats(String senderId) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    GatewaySenderStats statistics = sender.getStatistics();
    return statistics.getSecondaryEventQueueSize();
  }

  public static void checkConnectionStats(String senderId) {
    AbstractGatewaySender sender =
        (AbstractGatewaySender) CacheFactory.getAnyInstance().getGatewaySender(senderId);
    assertNotNull(sender);

    Collection statsCollection = sender.getProxy().getEndpointManager().getAllStats().values();
    assertTrue(statsCollection.iterator().next() instanceof ConnectionStats);
  }

  public static List<Integer> getSenderStats(String senderId, final int expectedQueueSize) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
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
          .untilAsserted(() -> assertEquals("Expected queue entries: " + expectedQueueSize
              + " but actual entries: " + regionQueue.size(), expectedQueueSize,
              regionQueue.size()));
    }
    ArrayList<Integer> stats = new ArrayList<Integer>();
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
    return stats;
  }

  protected static int getTotalBucketQueueSize(PartitionedRegion prQ, boolean isPrimary) {
    int size = 0;
    if (prQ != null) {
      Set<Map.Entry<Integer, BucketRegion>> allBuckets = prQ.getDataStore().getAllLocalBuckets();
      List<Integer> thisProcessorBuckets = new ArrayList<Integer>();

      for (Map.Entry<Integer, BucketRegion> bucketEntry : allBuckets) {
        BucketRegion bucket = bucketEntry.getValue();
        int bId = bucket.getId();
        if ((isPrimary && bucket.getBucketAdvisor().isPrimary())
            || (!isPrimary && !bucket.getBucketAdvisor().isPrimary())) {
          size += bucket.size();
        }
      }
    }
    return size;
  }

  public static List<Integer> getSenderStatsForDroppedEvents(String senderId) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    GatewaySenderStats statistics = sender.getStatistics();
    ArrayList<Integer> stats = new ArrayList<Integer>();
    int eventNotQueued = statistics.getEventsDroppedDueToPrimarySenderNotRunning();
    if (eventNotQueued > 0) {
      logger
          .info("Found " + eventNotQueued + " events dropped due to primary sender is not running");
    }
    stats.add(eventNotQueued);
    stats.add(statistics.getEventsNotQueued());
    stats.add(statistics.getEventsNotQueuedConflated());
    return stats;
  }

  public static void checkQueueStats(String senderId, final int queueSize, final int eventsReceived,
      final int eventsQueued, final int eventsDistributed) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertEquals(queueSize, statistics.getEventQueueSize());
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsQueued, statistics.getEventsQueued());
    assert (statistics.getEventsDistributed() >= eventsDistributed);
  }

  public static void validateGatewaySenderQueueHasContent(String senderId, VM... targetVms) {
    await()
        .until(() -> Arrays.stream(targetVms).map(vm -> vm.invoke(() -> {
          AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
          logger.info(displaySerialQueueContent(sender));
          return sender.getEventQueueSize();
        })).mapToInt(i -> i).sum(), greaterThan(0));
  }

  public static void checkGatewayReceiverStats(int processBatches, int eventsReceived,
      int creates) {
    checkGatewayReceiverStats(processBatches, eventsReceived, creates, false);
  }

  public static void checkGatewayReceiverStats(int processBatches, int eventsReceived,
      int creates, boolean isExact) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats) stats;
    if (isExact) {
      assertTrue(gatewayReceiverStats.getProcessBatchRequests() == processBatches);
    } else {
      assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    }
    assertEquals(eventsReceived, gatewayReceiverStats.getEventsReceived());
    assertEquals(creates, gatewayReceiverStats.getCreateRequest());
  }

  public static void checkMinimumGatewayReceiverStats(int processBatches, int eventsReceived) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats) stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertTrue(gatewayReceiverStats.getEventsReceived() >= eventsReceived);
  }

  public static void checkExceptionStats(int exceptionsOccurred) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats) stats;
    if (exceptionsOccurred == 0) {
      assertEquals(exceptionsOccurred, gatewayReceiverStats.getExceptionsOccurred());
    } else {
      assertTrue(gatewayReceiverStats.getExceptionsOccurred() >= exceptionsOccurred);
    }
  }

  public static void checkGatewayReceiverStatsHA(int processBatches, int eventsReceived,
      int creates) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats) stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertTrue(gatewayReceiverStats.getEventsReceived() >= eventsReceived);
    assertTrue(gatewayReceiverStats.getCreateRequest() >= creates);
  }

  public static void checkEventFilteredStats(String senderId, final int eventsFiltered) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertEquals(eventsFiltered, statistics.getEventsFiltered());
  }

  public static void checkConflatedStats(String senderId, final int eventsConflated) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertEquals(eventsConflated, statistics.getEventsNotQueuedConflated());
  }

  public static void checkStats_Failover(String senderId, final int eventsReceived) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsReceived,
        (statistics.getEventsQueued() + statistics.getUnprocessedTokensAddedByPrimary()
            + statistics.getUnprocessedEventsRemovedByPrimary()));
  }

  public static void checkBatchStats(String senderId, final int batches) {
    checkBatchStats(senderId, batches, false);
  }

  public static void checkBatchStats(String senderId, final int batches, boolean isExact) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    if (isExact) {
      assert (statistics.getBatchesDistributed() == batches);
    } else {
      assert (statistics.getBatchesDistributed() >= batches);
    }
    assertEquals(0, statistics.getBatchesRedistributed());
  }

  public static void checkBatchStats(String senderId, final int batches,
      boolean isExact, final boolean batchesRedistributed) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    if (isExact) {
      assert (statistics.getBatchesDistributed() == batches);
    } else {
      assert (statistics.getBatchesDistributed() >= batches);
    }
    assertEquals(batchesRedistributed, (statistics.getBatchesRedistributed() > 0));
  }

  public static void checkBatchStats(String senderId, final boolean batchesDistributed,
      final boolean batchesRedistributed) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertEquals(batchesDistributed, (statistics.getBatchesDistributed() > 0));
    assertEquals(batchesRedistributed, (statistics.getBatchesRedistributed() > 0));
  }

  public static void checkUnProcessedStats(String senderId, int events) {
    GatewaySenderStats statistics = getGatewaySenderStats(senderId);
    assertEquals(events, (statistics.getUnprocessedEventsAddedBySecondary()
        + statistics.getUnprocessedTokensRemovedBySecondary()));
    assertEquals(events, (statistics.getUnprocessedEventsRemovedByPrimary()
        + statistics.getUnprocessedTokensAddedByPrimary()));
  }

  public static GatewaySenderStats getGatewaySenderStats(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    return ((AbstractGatewaySender) sender).getStatistics();
  }

  public static void waitForSenderRunningState(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      final GatewaySender sender = getGatewaySenderById(senders, senderId);
      await()
          .untilAsserted(
              () -> assertEquals("Expected sender isRunning state to " + "be true but is false",
                  true, (sender != null && sender.isRunning())));
    } finally {
      exln.remove();
    }
  }

  public static void waitForSenderToBecomePrimary(String senderId) {
    Set<GatewaySender> senders = ((GemFireCacheImpl) cache).getAllGatewaySenders();
    final GatewaySender sender = getGatewaySenderById(senders, senderId);
    await()
        .untilAsserted(
            () -> assertEquals("Expected sender primary state to " + "be true but is false",
                true, (sender != null && ((AbstractGatewaySender) sender).isPrimary())));
  }

  private static GatewaySender getGatewaySenderById(Set<GatewaySender> senders, String senderId) {
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        return s;
      }
    }
    // if none of the senders matches with the supplied senderid, return null
    return null;
  }

  public static HashMap checkQueue() {
    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener1.createList);
    listenerAttrs.put("Update", listener1.updateList);
    listenerAttrs.put("Destroy", listener1.destroyList);
    return listenerAttrs;
  }

  public static void checkQueueOnSecondary(final Map primaryUpdatesMap) {
    final HashMap secondaryUpdatesMap = new HashMap();
    secondaryUpdatesMap.put("Create", listener1.createList);
    secondaryUpdatesMap.put("Update", listener1.updateList);
    secondaryUpdatesMap.put("Destroy", listener1.destroyList);

    await().untilAsserted(() -> {
      secondaryUpdatesMap.put("Create", listener1.createList);
      secondaryUpdatesMap.put("Update", listener1.updateList);
      secondaryUpdatesMap.put("Destroy", listener1.destroyList);
      assertEquals(
          "Expected secondary map to be " + primaryUpdatesMap + " but it is " + secondaryUpdatesMap,
          true, secondaryUpdatesMap.equals(primaryUpdatesMap));
    });
  }

  public static HashMap checkQueue2() {
    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener2.createList);
    listenerAttrs.put("Update", listener2.updateList);
    listenerAttrs.put("Destroy", listener2.destroyList);
    return listenerAttrs;
  }

  public static HashMap checkPR(String regionName) {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    QueueListener listener = (QueueListener) region.getCacheListener();

    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener.createList);
    listenerAttrs.put("Update", listener.updateList);
    listenerAttrs.put("Destroy", listener.destroyList);
    return listenerAttrs;
  }

  public static HashMap checkBR(String regionName, int numBuckets) {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    HashMap listenerAttrs = new HashMap();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      QueueListener listener = (QueueListener) br.getCacheListener();
      listenerAttrs.put("Create" + i, listener.createList);
      listenerAttrs.put("Update" + i, listener.updateList);
      listenerAttrs.put("Destroy" + i, listener.destroyList);
    }
    return listenerAttrs;
  }

  public static HashMap checkQueue_BR(String senderId, int numBuckets) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    RegionQueue parallelQueue =
        ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];

    PartitionedRegion region = (PartitionedRegion) parallelQueue.getRegion();
    HashMap listenerAttrs = new HashMap();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      if (br != null) {
        QueueListener listener = (QueueListener) br.getCacheListener();
        if (listener != null) {
          listenerAttrs.put("Create" + i, listener.createList);
          listenerAttrs.put("Update" + i, listener.updateList);
          listenerAttrs.put("Destroy" + i, listener.destroyList);
        }
      }
    }
    return listenerAttrs;
  }

  public static void addListenerOnBucketRegion(String regionName, int numBuckets) {
    WANTestBase test = new WANTestBase();
    test.addCacheListenerOnBucketRegion(regionName, numBuckets);
  }

  private void addCacheListenerOnBucketRegion(String regionName, int numBuckets) {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      AttributesMutator mutator = br.getAttributesMutator();
      listener1 = new QueueListener();
      mutator.addCacheListener(listener1);
    }
  }

  public static void addListenerOnQueueBucketRegion(String senderId, int numBuckets) {
    WANTestBase test = new WANTestBase();
    test.addCacheListenerOnQueueBucketRegion(senderId, numBuckets);
  }

  private void addCacheListenerOnQueueBucketRegion(String senderId, int numBuckets) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    RegionQueue parallelQueue =
        ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];

    PartitionedRegion region = (PartitionedRegion) parallelQueue.getRegion();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      if (br != null) {
        AttributesMutator mutator = br.getAttributesMutator();
        CacheListener listener = new QueueListener();
        mutator.addCacheListener(listener);
      }
    }

  }

  public static void addQueueListener(String senderId, boolean isParallel) {
    WANTestBase test = new WANTestBase();
    test.addCacheQueueListener(senderId, isParallel);
  }

  public static void addSecondQueueListener(String senderId, boolean isParallel) {
    WANTestBase test = new WANTestBase();
    test.addSecondCacheQueueListener(senderId, isParallel);
  }

  public static void addListenerOnRegion(String regionName) {
    WANTestBase test = new WANTestBase();
    test.addCacheListenerOnRegion(regionName);
  }

  private void addCacheListenerOnRegion(String regionName) {
    Region region = cache.getRegion(regionName);
    AttributesMutator mutator = region.getAttributesMutator();
    listener1 = new QueueListener();
    mutator.addCacheListener(listener1);
  }

  private void addCacheQueueListener(String senderId, boolean isParallel) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    listener1 = new QueueListener();
    if (!isParallel) {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      for (RegionQueue q : queues) {
        q.addCacheListener(listener1);
      }
    } else {
      RegionQueue parallelQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      parallelQueue.addCacheListener(listener1);
    }
  }

  private void addSecondCacheQueueListener(String senderId, boolean isParallel) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    listener2 = new QueueListener();
    if (!isParallel) {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      for (RegionQueue q : queues) {
        q.addCacheListener(listener2);
      }
    } else {
      RegionQueue parallelQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      parallelQueue.addCacheListener(listener2);
    }
  }

  public static void pauseSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.pause();
      ((AbstractGatewaySender) sender).getEventProcessor().waitForDispatcherToPause();

    } finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void resumeSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.resume();
    } finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void stopSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      AbstractGatewaySenderEventProcessor eventProcessor = null;
      if (sender instanceof AbstractGatewaySender) {
        eventProcessor = ((AbstractGatewaySender) sender).getEventProcessor();
      }
      sender.stop();

      Set<RegionQueue> queues = null;
      if (eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor) {
        queues = ((ConcurrentSerialGatewaySenderEventProcessor) eventProcessor).getQueues();
        for (RegionQueue queue : queues) {
          if (queue instanceof SerialGatewaySenderQueue) {
            assertFalse(((SerialGatewaySenderQueue) queue).isRemovalThreadAlive());
          }
        }
      }
    } finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void stopReceivers() {
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      receiver.stop();
    }
  }

  public static void startReceivers() {
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      try {
        receiver.start();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static GatewaySenderFactory configureGateway(DiskStoreFactory dsf, File[] dirs1,
      String dsName, boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, boolean isManualStart, int numDispatchers,
      OrderPolicy policy, int socketBufferSize) {

    InternalGatewaySenderFactory gateway =
        (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
    gateway.setParallel(isParallel);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.setManualStart(isManualStart);
    gateway.setDispatcherThreads(numDispatchers);
    gateway.setOrderPolicy(policy);
    gateway.setLocatorDiscoveryCallback(new MyLocatorCallback());
    gateway.setSocketBufferSize(socketBufferSize);
    if (filter != null) {
      eventFilter = filter;
      gateway.addGatewayEventFilter(filter);
    }
    if (isPersistent) {
      gateway.setPersistenceEnabled(true);
      gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
    } else {
      DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
      gateway.setDiskStoreName(store.getName());
    }
    return gateway;
  }

  public static void createSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart) {
    createSender(dsName, remoteDsId, isParallel, maxMemory, batchSize, isConflation, isPersistent,
        filter, isManualStart, false, -1);
  }

  public static void createSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart,
      boolean groupTransactionEvents, int batchTimeInterval) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      File persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {persistentDirectory};
      GatewaySenderFactory gateway = configureGateway(dsf, dirs1, dsName, isParallel, maxMemory,
          batchSize, isConflation, isPersistent, filter, isManualStart,
          numDispatcherThreadsForTheRun, GatewaySender.DEFAULT_ORDER_POLICY,
          GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
      gateway.setGroupTransactionEvents(groupTransactionEvents);
      gateway.create(dsName, remoteDsId);
      if (batchTimeInterval > 0) {
        gateway.setBatchTimeInterval(batchTimeInterval);
      }
    } finally {
      exln.remove();
    }
  }

  public static void createSenderWithMultipleDispatchers(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, boolean isManualStart, int numDispatchers,
      OrderPolicy orderPolicy) {
    createSenderWithMultipleDispatchers(dsName, remoteDsId,
        isParallel, maxMemory, batchSize, isConflation,
        isPersistent, filter, isManualStart, numDispatchers,
        orderPolicy, GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
  }

  public static void createSenderWithMultipleDispatchers(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, boolean isManualStart, int numDispatchers,
      OrderPolicy orderPolicy, int socketBufferSize) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      File persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {persistentDirectory};
      GatewaySenderFactory gateway =
          configureGateway(dsf, dirs1, dsName, isParallel, maxMemory, batchSize, isConflation,
              isPersistent, filter, isManualStart, numDispatchers, orderPolicy, socketBufferSize);
      gateway.create(dsName, remoteDsId);

    } finally {
      exln.remove();
    }
  }

  public static void createSenderWithoutDiskStore(String dsName, int remoteDsId, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isManualStart) {

    GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
    gateway.setParallel(true);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setManualStart(isManualStart);
    // set dispatcher threads
    gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.create(dsName, remoteDsId);
  }

  public static void createSenderAlertThresholdWithoutDiskStore(String dsName, int remoteDsId,
      Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isManualStart, int alertthreshold) {

    GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
    gateway.setAlertThreshold(alertthreshold);
    gateway.setParallel(true);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setManualStart(isManualStart);
    // set dispatcher threads
    gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.create(dsName, remoteDsId);
  }

  public static void createConcurrentSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart, int concurrencyLevel, OrderPolicy policy) {

    File persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};
    GatewaySenderFactory gateway = configureGateway(dsf, dirs1, dsName, isParallel, maxMemory,
        batchSize, isConflation, isPersistent, filter, isManualStart, concurrencyLevel, policy,
        GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
    gateway.create(dsName, remoteDsId);
  }

  public static void createSenderForValidations(String dsName, int remoteDsId, boolean isParallel,
      Integer alertThreshold, boolean isConflation, boolean isPersistent,
      List<GatewayEventFilter> eventFilters, List<GatewayTransportFilter> transportFilters,
      boolean isManualStart, boolean isDiskSync) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(RegionDestroyedException.class.getName());
    try {
      File persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {persistentDirectory};

      if (isParallel) {
        GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
        gateway.setParallel(true);
        gateway.setAlertThreshold(alertThreshold);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (eventFilters != null) {
          for (GatewayEventFilter filter : eventFilters) {
            gateway.addGatewayEventFilter(filter);
          }
        }
        if (transportFilters != null) {
          for (GatewayTransportFilter filter : transportFilters) {
            gateway.addGatewayTransportFilter(filter);
          }
        }
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName + "_Parallel").getName());
        } else {
          DiskStore store = dsf.setDiskDirs(dirs1).create(dsName + "_Parallel");
          gateway.setDiskStoreName(store.getName());
        }
        gateway.setDiskSynchronous(isDiskSync);
        gateway.setBatchConflationEnabled(isConflation);
        gateway.setManualStart(isManualStart);
        // set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        gateway.create(dsName, remoteDsId);

      } else {
        GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
        gateway.setAlertThreshold(alertThreshold);
        gateway.setManualStart(isManualStart);
        // set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (eventFilters != null) {
          for (GatewayEventFilter filter : eventFilters) {
            gateway.addGatewayEventFilter(filter);
          }
        }
        if (transportFilters != null) {
          for (GatewayTransportFilter filter : transportFilters) {
            gateway.addGatewayTransportFilter(filter);
          }
        }
        gateway.setBatchConflationEnabled(isConflation);
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName + "_Serial").getName());
        } else {
          DiskStore store = dsf.setDiskDirs(dirs1).create(dsName + "_Serial");
          gateway.setDiskStoreName(store.getName());
        }
        gateway.setDiskSynchronous(isDiskSync);
        gateway.create(dsName, remoteDsId);
      }
    } finally {
      exp1.remove();
    }
  }

  public static String createSenderWithDiskStore(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, String dsStore, boolean isManualStart) {
    File persistentDirectory = null;
    if (dsStore == null) {
      persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    } else {
      persistentDirectory = new File(dsStore);
    }
    logger.info("The ds is : " + persistentDirectory.getName());

    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};

    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        String dsname = dsf.setDiskDirs(dirs1).create(dsName).getName();
        gateway.setDiskStoreName(dsname);
        logger.info("The DiskStoreName is : " + dsname);
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
        logger.info("The ds is : " + store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);

        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
    return persistentDirectory.getName();
  }


  public static void createSenderWithListener(String dsName, int remoteDsName, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean attachTwoListeners, boolean isManualStart) {
    File persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};

    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsName);

    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }

      eventListener1 = new MyGatewaySenderEventListener();
      ((InternalGatewaySenderFactory) gateway).addAsyncEventListener(eventListener1);
      if (attachTwoListeners) {
        eventListener2 = new MyGatewaySenderEventListener2();
        ((InternalGatewaySenderFactory) gateway).addAsyncEventListener(eventListener2);
      }
      ((InternalGatewaySenderFactory) gateway).create(dsName);
    }
  }

  public static void createReceiverInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> createReceiver());
    }
  }

  public static int createReceiver() {
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(
          "Test " + getTestMethodName() + " failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }

  public static int createReceiverWithSSL(int locPort) {
    WANTestBase test = new WANTestBase();

    Properties gemFireProps = test.getDistributedSystemProperties();

    gemFireProps.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    gemFireProps.put(GATEWAY_SSL_ENABLED, "true");
    gemFireProps.put(GATEWAY_SSL_PROTOCOLS, "any");
    gemFireProps.put(GATEWAY_SSL_CIPHERS, "any");
    gemFireProps.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION, "true");

    gemFireProps.put(GATEWAY_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.put(GATEWAY_SSL_KEYSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD, "password");

    gemFireProps.setProperty(MCAST_PORT, "0");
    gemFireProps.setProperty(LOCATORS, "localhost[" + locPort + "]");

    logger.info("Starting cache ds with following properties \n" + gemFireProps);

    InternalDistributedSystem ds = test.getSystem(gemFireProps);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  public static void createReceiverAndServer(int locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int receiverPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(receiverPort);
    fact.setEndPort(receiverPort);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + receiverPort);
    }
    CacheServer server = cache.addCacheServer();
    int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(serverPort);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      org.apache.geode.test.dunit.Assert.fail("Failed to start server ", e);
    }
  }

  public static int createServer(int locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      org.apache.geode.test.dunit.Assert.fail("Failed to start server ", e);
    }
    return port;
  }

  public static void createClientWithLocator(int port0, String host, String regionName) {
    createClientWithLocator(port0, host);

    RegionFactory factory = cache.createRegionFactory(RegionShortcut.LOCAL);
    factory.setPoolName("pool");

    region = factory.create(regionName);
    region.registerInterest("ALL_KEYS");
    assertNotNull(region);
    logger.info("Distributed Region " + regionName + " created Successfully :" + region.toString());
  }

  public static void createClientWithLocator(final int port0, final String host) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(20000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create("pool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
  }

  public static int createReceiver_PDX(int locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    File pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");

    cache = new InternalCacheBuilder(props)
        .setPdxPersistent(true)
        .setPdxDiskStore("pdxStore")
        .setIsExistingOk(false)
        .create(ds);

    cache.createDiskStoreFactory().setDiskDirs(new File[] {pdxDir}).setMaxOplogSize(1)
        .create("pdxStore");
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  public static void doDistTXPuts(String regionName, int numPuts) {
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
    txMgr.setDistributed(true);

    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 1; i <= numPuts; i++) {
        txMgr.begin();
        r.put(i, i);
        txMgr.commit();
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }


  public static void doPuts(String regionName, int numPuts, Object value) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, value);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }

  public static void doPuts(String regionName, int numPuts) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, "Value_" + i);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }

  public static void doPutsSameKey(String regionName, int numPuts, String key) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        r.put(key, "Value_" + i);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }


  public static void doPutsAfter300(String regionName, int numPuts) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 300; i < numPuts; i++) {
      r.put(i, "Value_" + i);
    }
  }

  public static void doPutsFrom(String regionName, int from, int numPuts) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = from; i < numPuts; i++) {
      r.put(i, "Value_" + i);
    }
  }

  public static void doDestroys(String regionName, int keyNum) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < keyNum; i++) {
      r.destroy(i);
    }
  }

  public static void doPutAll(String regionName, int numPuts, int size) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numPuts; i++) {
      Map putAllMap = new HashMap();
      for (long j = 0; j < size; j++) {
        putAllMap.put((size * i) + j, "Value_" + i);
      }
      r.putAll(putAllMap, "putAllCallback");
      putAllMap.clear();
    }
  }


  public static void doPutsWithKeyAsString(String regionName, int numPuts) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numPuts; i++) {
      r.put("Object_" + i, "Value_" + i);
    }
  }

  public static void putGivenKeyValue(String regionName, Map keyValues) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (Object key : keyValues.keySet()) {
      r.put(key, keyValues.get(key));
    }
  }

  public static void doOrderAndShipmentPutsInsideTransactions(Map keyValues,
      int eventsPerTransaction) {
    Region orderRegion = cache.getRegion(orderRegionName);
    Region shipmentRegion = cache.getRegion(shipmentRegionName);
    assertNotNull(orderRegion);
    assertNotNull(shipmentRegion);
    int eventInTransaction = 0;
    CacheTransactionManager cacheTransactionManager = cache.getCacheTransactionManager();
    for (Object key : keyValues.keySet()) {
      if (eventInTransaction == 0) {
        cacheTransactionManager.begin();
      }
      Region r;
      if (key instanceof OrderId) {
        r = orderRegion;
      } else {
        r = shipmentRegion;
      }
      r.put(key, keyValues.get(key));
      if (++eventInTransaction == eventsPerTransaction) {
        cacheTransactionManager.commit();
        eventInTransaction = 0;
      }
    }
    if (eventInTransaction != 0) {
      cacheTransactionManager.commit();
    }
  }

  public static void doPutsInsideTransactions(String regionName, Map keyValues,
      int eventsPerTransaction) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    int eventInTransaction = 0;
    CacheTransactionManager cacheTransactionManager = cache.getCacheTransactionManager();
    for (Object key : keyValues.keySet()) {
      if (eventInTransaction == 0) {
        cacheTransactionManager.begin();
      }
      r.put(key, keyValues.get(key));
      if (++eventInTransaction == eventsPerTransaction) {
        cacheTransactionManager.commit();
        eventInTransaction = 0;
      }
    }
    if (eventInTransaction != 0) {
      cacheTransactionManager.commit();
    }
  }

  public static void destroyRegion(String regionName) {
    destroyRegion(regionName, -1);
  }

  public static void destroyRegion(String regionName, final int min) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    await().until(() -> r.size() > min);
    r.destroyRegion();
  }

  public static void localDestroyRegion(String regionName) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(PRLocallyDestroyedException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      r.localDestroyRegion();
    } finally {
      exp.remove();
    }
  }


  public static Map putCustomerPartitionedRegion(int numPuts) {
    String valueSuffix = "";
    return putCustomerPartitionedRegion(numPuts, valueSuffix);
  }

  public static Map updateCustomerPartitionedRegion(int numPuts) {
    String valueSuffix = "_update";
    return putCustomerPartitionedRegion(numPuts, valueSuffix);
  }

  protected static Map putCustomerPartitionedRegion(int numPuts, String valueSuffix) {
    assertNotNull(cache);
    assertNotNull(customerRegion);
    Map custKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i + valueSuffix);
      try {
        customerRegion.put(custid, customer);
        assertTrue(customerRegion.containsKey(custid));
        assertEquals(customer, customerRegion.get(custid));
        custKeyValues.put(custid, customer);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      logger.info("Customer :- { " + custid + " : " + customer + " }");
    }
    return custKeyValues;
  }

  public static Map putOrderPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      int oid = i + 1;
      OrderId orderId = new OrderId(oid, custid);
      Order order = new Order("ORDER" + oid);
      try {
        orderRegion.put(orderId, order);
        orderKeyValues.put(orderId, order);
        assertTrue(orderRegion.containsKey(orderId));
        assertEquals(order, orderRegion.get(orderId));

      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + orderId + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map putOrderPartitionedRegionUsingCustId(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Order order = new Order("ORDER" + i);
      try {
        orderRegion.put(custid, order);
        orderKeyValues.put(custid, order);
        assertTrue(orderRegion.containsKey(custid));
        assertEquals(order, orderRegion.get(custid));

      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putOrderPartitionedRegionUsingCustId : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + custid + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map updateOrderPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      int oid = i + 1;
      OrderId orderId = new OrderId(oid, custid);
      Order order = new Order("ORDER" + oid + "_update");
      try {
        orderRegion.put(orderId, order);
        orderKeyValues.put(orderId, order);
        assertTrue(orderRegion.containsKey(orderId));
        assertEquals(order, orderRegion.get(orderId));

      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + orderId + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map updateOrderPartitionedRegionUsingCustId(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Order order = new Order("ORDER" + i + "_update");
      try {
        orderRegion.put(custid, order);
        assertTrue(orderRegion.containsKey(custid));
        assertEquals(order, orderRegion.get(custid));
        orderKeyValues.put(custid, order);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateOrderPartitionedRegionUsingCustId : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + custid + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map putShipmentPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(shipmentRegion);
    Map shipmentKeyValue = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      int oid = i + 1;
      OrderId orderId = new OrderId(oid, custid);
      int sid = oid + 1;
      ShipmentId shipmentId = new ShipmentId(sid, orderId);
      Shipment shipment = new Shipment("Shipment" + sid);
      try {
        shipmentRegion.put(shipmentId, shipment);
        assertTrue(shipmentRegion.containsKey(shipmentId));
        assertEquals(shipment, shipmentRegion.get(shipmentId));
        shipmentKeyValue.put(shipmentId, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + shipmentId + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static void putcolocatedPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(customerRegion);
    assertNotNull(orderRegion);
    assertNotNull(shipmentRegion);
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("Customer" + custid, "Address" + custid);
      customerRegion.put(custid, customer);
      int oid = i + 1;
      OrderId orderId = new OrderId(oid, custid);
      Order order = new Order("Order" + orderId);
      orderRegion.put(orderId, order);
      int sid = oid + 1;
      ShipmentId shipmentId = new ShipmentId(sid, orderId);
      Shipment shipment = new Shipment("Shipment" + sid);
      shipmentRegion.put(shipmentId, shipment);
    }
  }

  public static Map putShipmentPartitionedRegionUsingCustId(int numPuts) {
    assertNotNull(cache);
    assertNotNull(shipmentRegion);
    Map shipmentKeyValue = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Shipment shipment = new Shipment("Shipment" + i);
      try {
        shipmentRegion.put(custid, shipment);
        assertTrue(shipmentRegion.containsKey(custid));
        assertEquals(shipment, shipmentRegion.get(custid));
        shipmentKeyValue.put(custid, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putShipmentPartitionedRegionUsingCustId : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + custid + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static Map updateShipmentPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(shipmentRegion);
    Map shipmentKeyValue = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      int oid = i + 1;
      OrderId orderId = new OrderId(oid, custid);
      int sid = oid + 1;
      ShipmentId shipmentId = new ShipmentId(sid, orderId);
      Shipment shipment = new Shipment("Shipment" + sid + "_update");
      try {
        shipmentRegion.put(shipmentId, shipment);
        assertTrue(shipmentRegion.containsKey(shipmentId));
        assertEquals(shipment, shipmentRegion.get(shipmentId));
        shipmentKeyValue.put(shipmentId, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + shipmentId + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static Map updateShipmentPartitionedRegionUsingCustId(int numPuts) {
    assertNotNull(cache);
    assertNotNull(shipmentRegion);
    Map shipmentKeyValue = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Shipment shipment = new Shipment("Shipment" + i + "_update");
      try {
        shipmentRegion.put(custid, shipment);
        assertTrue(shipmentRegion.containsKey(custid));
        assertEquals(shipment, shipmentRegion.get(custid));
        shipmentKeyValue.put(custid, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateShipmentPartitionedRegionUsingCustId : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + custid + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static void doPutsPDXSerializable(String regionName, int numPuts) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (int i = 0; i < numPuts; i++) {
      r.put("Key_" + i, new SimpleClass(i, (byte) i));
    }
  }

  public static void doPutsPDXSerializable2(String regionName, int numPuts) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (int i = 0; i < numPuts; i++) {
      r.put("Key_" + i, new SimpleClass1(false, (short) i, "" + i, i, "" + i, "" + i, i, i));
    }
  }


  public static void doTxPuts(String regionName) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    CacheTransactionManager mgr = cache.getCacheTransactionManager();

    mgr.begin();
    r.put(0, 0);
    r.put(100, 100);
    r.put(200, 200);
    mgr.commit();
  }

  public static void doTxPutsWithRetryIfError(String regionName, final long putsPerTransaction,
      final long transactions, long offset) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);

    long keyOffset = offset * ((putsPerTransaction + (10 * transactions)) * 100);
    long i = 0;
    long j = 0;
    CacheTransactionManager mgr = cache.getCacheTransactionManager();;
    for (i = 0; i < transactions; i++) {
      boolean done = false;
      do {
        try {
          mgr.begin();
          for (j = 0; j < putsPerTransaction; j++) {
            long key = keyOffset + ((j + (10 * i)) * 100);
            String value = "Value_" + key;
            r.put(key, value);
          }
          mgr.commit();
          done = true;
        } catch (TransactionException e) {
          logger.info("Something went wrong with transaction [{},{}]. Retrying. Error: {}", i, j,
              e.getMessage());
          e.printStackTrace();
        } catch (IllegalStateException e1) {
          logger.info("Something went wrong with transaction [{},{}]. Retrying. Error: {}", i, j,
              e1.getMessage());
          e1.printStackTrace();
          try {
            mgr.rollback();
            logger.info("Rolled back transaction [{},{}]. Retrying. Error: {}", i, j,
                e1.getMessage());
          } catch (Exception e2) {
            logger.info(
                "Something went wrong when rolling back transaction [{},{}]. Retrying transaction. Error: {}",
                i, j, e2.getMessage());
            e2.printStackTrace();
          }
        }
      } while (!done);
    }
  }

  public static void doNextPuts(String regionName, int start, int numPuts) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = start; i < numPuts; i++) {
        r.put(i, i);
      }
    } finally {
      exp.remove();
    }
  }

  public static void checkQueueSize(String senderId, int numQueueEntries) {
    await()
        .untilAsserted(() -> testQueueSize(senderId, numQueueEntries));
  }

  public static void testQueueSize(String senderId, int numQueueEntries) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    if (sender.isParallel()) {
      int totalSize = 0;
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      for (RegionQueue q : queues) {
        ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.size();
      }
      assertEquals(numQueueEntries, totalSize);
    } else {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }

  /**
   * To be used only for ParallelGatewaySender.
   *
   * @param senderId Id of the ParallelGatewaySender
   * @param numQueueEntries Expected number of ParallelGatewaySenderQueue entries
   */
  public static void checkPRQLocalSize(String senderId, final int numQueueEntries) {
    GatewaySender sender = null;
    for (GatewaySender s : cache.getGatewaySenders()) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      await().untilAsserted(() -> {
        int size = 0;
        for (RegionQueue q : queues) {
          ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
          size += prQ.localSize();
        }
        assertEquals(
            " Expected local queue entries: " + numQueueEntries + " but actual entries: " + size,
            numQueueEntries, size);
      });
    }
  }

  /**
   * To be used only for ParallelGatewaySender.
   *
   * @param senderId Id of the ParallelGatewaySender
   */
  public static int getPRQLocalSize(String senderId) {
    GatewaySender sender = null;
    for (GatewaySender s : cache.getGatewaySenders()) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    if (sender.isParallel()) {
      int totalSize = 0;
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      for (RegionQueue q : queues) {
        ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.localSize();
      }
      return totalSize;
    }
    return -1;
  }

  public static void doMultiThreadedPuts(String regionName, int numPuts) {
    final AtomicInteger ai = new AtomicInteger(-1);
    final ExecutorService execService = Executors.newFixedThreadPool(5, new ThreadFactory() {
      AtomicInteger threadNum = new AtomicInteger();

      @Override
      public Thread newThread(final Runnable r) {
        Thread result = new Thread(r, "Client Put Thread-" + threadNum.incrementAndGet());
        result.setDaemon(true);
        return result;
      }
    });

    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);

    List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
    for (long i = 0; i < 5; i++)
      tasks.add(new PutTask(r, ai, numPuts));

    try {
      List<Future<Object>> l = execService.invokeAll(tasks);
      for (Future<Object> f : l)
        f.get();
    } catch (InterruptedException e1) { // TODO: eats exception
      e1.printStackTrace();
    } catch (ExecutionException e) { // TODO: eats exceptions
      e.printStackTrace();
    }
    execService.shutdown();
  }

  public static void validateRegionSize(String regionName, final int regionSize) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {
      final Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      if (regionSize != r.keySet().size()) {
        await()
            .untilAsserted(() -> assertEquals(
                "Expected region entries: " + regionSize + " but actual entries: "
                    + r.keySet().size() + " present region keyset " + r.keySet(),
                regionSize, r.keySet().size()));
      }
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void validateAsyncEventListener(String asyncQueueId, final int expectedSize) {
    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final Map eventsMap = ((MyAsyncEventListener) theListener).getEventsMap();
    assertNotNull(eventsMap);
    await()
        .untilAsserted(() -> assertEquals(
            "Expected map entries: " + expectedSize + " but actual entries: " + eventsMap.size(),
            expectedSize, eventsMap.size()));
  }

  public static void waitForAsyncQueueToGetEmpty(String asyncQueueId) {
    AsyncEventQueue theAsyncEventQueue = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theAsyncEventQueue = asyncChannel;
      }
    }

    final GatewaySender sender = ((AsyncEventQueueImpl) theAsyncEventQueue).getSender();

    if (sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      await().untilAsserted(() -> {
        int size = 0;
        for (RegionQueue q : queues) {
          size += q.size();
        }
        assertEquals("Expected queue size to be : " + 0 + " but actual entries: " + size, 0, size);
      });
    } else {
      await().untilAsserted(() -> {
        Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
        int size = 0;
        for (RegionQueue q : queues) {
          size += q.size();
        }
        assertEquals("Expected queue size to be : " + 0 + " but actual entries: " + size, 0, size);
      });
    }
  }

  public static int getAsyncEventListenerMapSize(String asyncEventQueueId) {
    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final Map eventsMap = ((MyAsyncEventListener) theListener).getEventsMap();
    assertNotNull(eventsMap);
    logger.info("The events map size is " + eventsMap.size());
    return eventsMap.size();
  }

  public static void validateRegionSize_PDX(String regionName, final int regionSize) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    await().untilAsserted(() -> {
      assertEquals("Expected region entries: " + regionSize + " but actual entries: "
          + r.keySet().size() + " present region keyset " + r.keySet(), true,
          (regionSize <= r.keySet().size()));
      assertEquals("Expected region size: " + regionSize + " but actual size: " + r.size(), true,
          (regionSize == r.size()));
    });
    for (int i = 0; i < regionSize; i++) {
      final int temp = i;
      await()
          .untilAsserted(() -> assertEquals(
              "keySet = " + r.keySet() + " values() = " + r.values() + "Region Size = " + r.size(),
              new SimpleClass(temp, (byte) temp), r.get("Key_" + temp)));
    }
  }

  public static void validateRegionSizeOnly_PDX(String regionName, final int regionSize) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    await()
        .untilAsserted(
            () -> assertEquals(
                "Expected region entries: " + regionSize + " but actual entries: "
                    + r.keySet().size() + " present region keyset " + r.keySet(),
                true, (regionSize <= r.keySet().size())));
  }

  public static void validateQueueSizeStat(String id, final int queueSize) {
    final AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(id);
    await()
        .untilAsserted(() -> assertEquals(queueSize, sender.getEventQueueSize()));
    assertEquals(queueSize, sender.getEventQueueSize());
  }

  public static void validateSecondaryQueueSizeStat(String id, final int queueSize) {
    final AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(id);
    await()
        .untilAsserted(() -> assertEquals(
            "Expected unprocessedEventMap is drained but actual is "
                + sender.getStatistics().getUnprocessedEventMapSize(),
            queueSize, sender.getStatistics().getUnprocessedEventMapSize()));
    assertEquals(queueSize, sender.getStatistics().getUnprocessedEventMapSize());
  }

  /**
   * This method is specifically written for pause and stop operations. This method validates that
   * the region size remains same for at least minimum number of verification attempts and also it
   * remains below a specified limit value. This validation will suffice for testing of pause/stop
   * operations.
   *
   */
  public static void validateRegionSizeRemainsSame(String regionName, final int regionSizeLimit) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      final int MIN_VERIFICATION_RUNS = 20;
      int sameRegionSizeCounter = 0;
      long previousSize = -1;

      @Override
      public boolean done() {
        if (r.keySet().size() == previousSize) {
          sameRegionSizeCounter++;
          int s = r.keySet().size();
          // if the sameRegionSizeCounter exceeds the minimum verification runs and regionSize is
          // below specified limit, then return true
          if (sameRegionSizeCounter >= MIN_VERIFICATION_RUNS && s <= regionSizeLimit) {
            return true;
          } else {
            return false;
          }

        } else { // current regionSize is not same as recorded previous regionSize
          previousSize = r.keySet().size(); // update the previousSize variable with current region
                                            // size
          sameRegionSizeCounter = 0;// reset the sameRegionSizeCounter
          return false;
        }
      }

      @Override
      public String description() {
        return "Expected region size to remain same below a specified limit but actual region size does not remain same or exceeded the specified limit "
            + sameRegionSizeCounter + " :regionSize " + previousSize;
      }
    };
    await().untilAsserted(wc);
  }

  public static String getRegionFullPath(String regionName) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    return r.getFullPath();
  }

  public static Integer getRegionSize(String regionName) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    return r.keySet().size();
  }

  public static void validateRegionContents(String regionName, final Map keyValues) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    await().untilAsserted(() -> {
      boolean matchFlag = true;
      for (Object key : keyValues.keySet()) {
        if (!r.get(key).equals(keyValues.get(key))) {
          logger.info("The values are for key " + "  " + key + " " + r.get(key) + " in the map "
              + keyValues.get(key));
          matchFlag = false;
        }
      }
      assertEquals("Expected region entries doesn't match", true, matchFlag);
    });
  }



  public static void doHeavyPuts(String regionName, int numPuts) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    // GatewaySender.DEFAULT_BATCH_SIZE * OBJECT_SIZE should be more than MAXIMUM_QUEUE_MEMORY
    // to guarantee overflow
    for (long i = 0; i < numPuts; i++) {
      r.put(i, new byte[1024 * 1024]);
    }
  }

  public static void addCacheListenerAndDestroyRegion(String regionName) {
    final Region region = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(region);
    CacheListenerAdapter cl = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if ((Long) event.getKey() == 99) {
          region.destroyRegion();
        }
      }
    };
    region.getAttributesMutator().addCacheListener(cl);
  }

  public static Boolean killSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    IgnoredException exp =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      AbstractGatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = (AbstractGatewaySender) s;
          break;
        }
      }
      if (sender.isPrimary()) {
        logger.info("Gateway sender is killed by a test");
        cache.getDistributedSystem().disconnect();
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    } finally {
      exp.remove();
      exp1.remove();
      exln.remove();
    }
  }

  public static void killSender() {
    logger.info("Gateway sender is going to be killed by a test");
    cache.close();
    cache.getDistributedSystem().disconnect();
    logger.info("Gateway sender is killed by a test");
  }

  public static void checkAllSiteMetaData(
      Map<Integer, Set<InetSocketAddress>> dsIdToLocatorAddresses, final int siteSizeToCheck) {
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    await().untilAsserted(() -> {
      Map<Integer, Set<DistributionLocatorId>> allSiteMetaData =
          ((InternalLocator) locator).getLocatorMembershipListener().getAllLocatorsInfo();
      assertThat(allSiteMetaData.size()).isEqualTo(siteSizeToCheck);
      for (Map.Entry<Integer, Set<InetSocketAddress>> entry : dsIdToLocatorAddresses.entrySet()) {
        Set<DistributionLocatorId> foundLocatorIds = allSiteMetaData.get(entry.getKey());
        Set<InetSocketAddress> expectedLocators = entry.getValue();
        final Set<InetSocketAddress> foundLocators = foundLocatorIds.stream()
            .map(distributionLocatorId -> new InetSocketAddress(
                distributionLocatorId.getHostnameForClients(), distributionLocatorId.getPort()))
            .collect(Collectors.toSet());
        assertEquals(expectedLocators, foundLocators);
      }
    });
  }

  public static Long checkAllSiteMetaDataFor3Sites(final Map<Integer, Set<String>> dsVsPort) {
    await()
        .untilAsserted(
            () -> assertEquals("System is not initialized", true, (getSystemStatic() != null)));
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    LocatorMembershipListener listener = ((InternalLocator) locator).getLocatorMembershipListener();
    if (listener == null) {
      fail(
          "No locator membership listener available. WAN is likely not enabled. Is this test in the WAN project?");
    }
    final Map<Integer, Set<DistributionLocatorId>> allSiteMetaData = listener.getAllLocatorsInfo();
    System.out.println("allSiteMetaData : " + allSiteMetaData);

    await().untilAsserted(() -> {
      assertEquals(true, (dsVsPort.size() == allSiteMetaData.size()));
      boolean completeFlag = true;
      for (Map.Entry<Integer, Set<String>> entry : dsVsPort.entrySet()) {
        Set<DistributionLocatorId> locators = allSiteMetaData.get(entry.getKey());
        for (String locatorInMetaData : entry.getValue()) {
          DistributionLocatorId locatorId = new DistributionLocatorId(locatorInMetaData);
          if (!locators.contains(locatorId)) {
            completeFlag = false;
            break;
          }
        }
        if (false == completeFlag) {
          break;
        }
      }
      assertEquals(
          "Expected site Metadata: " + dsVsPort + " but actual meta data: " + allSiteMetaData, true,
          completeFlag);
    });
    return System.currentTimeMillis();
  }

  public static void putRemoteSiteLocators(int remoteDsId, Set<String> remoteLocators) {
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    if (remoteLocators != null) {
      // Add fake remote locators to the locators map
      ((InternalLocator) locator).getLocatorMembershipListener().getAllServerLocatorsInfo()
          .put(remoteDsId, remoteLocators);
    }
  }

  public static void checkLocatorsinSender(String senderId, InetSocketAddress locatorToWaitFor)
      throws InterruptedException {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    MyLocatorCallback callback =
        (MyLocatorCallback) ((AbstractGatewaySender) sender).getLocatorDiscoveryCallback();

    boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    assertTrue("Waited " + MAX_WAIT + " for " + locatorToWaitFor
        + " to be discovered on client. List is now: " + callback.getDiscovered(), discovered);
  }

  public static void validateQueueContents(final String senderId, final int regionSize) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }

      if (!sender.isParallel()) {
        final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
        await().untilAsserted(() -> {
          int size = 0;
          for (RegionQueue q : queues) {
            size += q.size();
          }
          assertEquals("Expected queue entries: " + regionSize + " but actual entries: " + size,
              regionSize, size);
        });
      } else if (sender.isParallel()) {
        final RegionQueue regionQueue;
        regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
        await().untilAsserted(() -> assertEquals(
            "Expected queue entries: " + regionSize + " but actual entries: " + regionQueue.size(),
            regionSize, regionQueue.size()));
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }

  }

  // Ensure that the sender's queue(s) have been closed.
  public static void validateQueueClosedForConcurrentSerialGatewaySender(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final Set<RegionQueue> regionQueue;
    if (sender instanceof AbstractGatewaySender) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues();
    } else {
      regionQueue = null;
    }
    assertEquals(null, regionQueue);
  }

  public static void validateQueueContentsForConcurrentSerialGatewaySender(final String senderId,
      final int regionSize) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final Set<RegionQueue> regionQueue;
    if (!sender.isParallel()) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues();
    } else {
      regionQueue = null;
    }
    await().untilAsserted(() -> {
      int size = 0;
      for (RegionQueue q : regionQueue) {
        size += q.size();
      }
      assertEquals(true, regionSize == size);
    });
  }

  public static Integer getSecondaryQueueContentSize(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    AbstractGatewaySender abstractSender = (AbstractGatewaySender) sender;
    int size = abstractSender.getSecondaryEventQueueSize();
    return size;
  }

  public static String displayQueueContent(final RegionQueue queue) {
    if (queue instanceof ParallelGatewaySenderQueue) {
      ParallelGatewaySenderQueue pgsq = (ParallelGatewaySenderQueue) queue;
      return pgsq.displayContent();
    } else if (queue instanceof ConcurrentParallelGatewaySenderQueue) {
      ConcurrentParallelGatewaySenderQueue pgsq = (ConcurrentParallelGatewaySenderQueue) queue;
      return pgsq.displayContent();
    }
    return null;
  }

  public static String displaySerialQueueContent(final AbstractGatewaySender sender) {
    StringBuilder message = new StringBuilder();
    message.append("Is Primary: ").append(sender.isPrimary()).append(", ").append("Queue Size: ")
        .append(sender.getEventQueueSize());

    if (sender.getQueues() != null) {
      message.append(", ").append("Queue Count: ").append(sender.getQueues().size());
      Stream<Object> stream = sender.getQueues().stream()
          .map(regionQueue -> ((SerialGatewaySenderQueue) regionQueue).displayContent());

      List<Object> list = stream.collect(Collectors.toList());
      message.append(", ").append("Keys: ").append(list.toString());
    }

    AbstractGatewaySenderEventProcessor abstractProcessor = sender.getEventProcessor();
    if (abstractProcessor == null) {
      message.append(", ").append("Null Event Processor: ");
    }
    if (!sender.isPrimary()) {
      message.append("\n").append("Unprocessed Events: ")
          .append(abstractProcessor.printUnprocessedEvents()).append("\n");
      message.append("\n").append("Unprocessed Tokens: ")
          .append(abstractProcessor.printUnprocessedTokens()).append("\n");
    }

    return message.toString();
  }

  public static Integer getQueueContentSize(final String senderId) {
    return getQueueContentSize(senderId, false);
  }

  public static Integer getQueueContentSize(final String senderId, boolean includeSecondary) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (!sender.isParallel()) {
      // if sender is serial, the queues will be all primary or all secondary at one member
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      return size;
    } else if (sender.isParallel()) {
      RegionQueue regionQueue = null;
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      if (regionQueue instanceof ConcurrentParallelGatewaySenderQueue) {
        return ((ConcurrentParallelGatewaySenderQueue) regionQueue).localSize(includeSecondary);
      } else if (regionQueue instanceof ParallelGatewaySenderQueue) {
        return ((ParallelGatewaySenderQueue) regionQueue).localSize(includeSecondary);
      } else {
        fail("Not implemented yet");
      }
    }
    fail("Not yet implemented?");
    return 0;
  }

  public static void validateParallelSenderQueueBucketSize(final String senderId,
      final int bucketSize) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    RegionQueue regionQueue =
        ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
    Set<BucketRegion> buckets = ((PartitionedRegion) regionQueue.getRegion()).getDataStore()
        .getAllLocalPrimaryBucketRegions();
    for (BucketRegion bucket : buckets) {
      assertEquals(
          "Expected bucket entries for bucket " + bucket.getId() + " is different than actual.",
          bucketSize, bucket.keySet().size());
    }
  }

  public static void validateParallelSenderQueueAllBucketsDrained(final String senderId) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(RegionDestroyedException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      final AbstractGatewaySender abstractSender = (AbstractGatewaySender) sender;
      RegionQueue queue = abstractSender.getEventProcessor().queue;
      await().untilAsserted(() -> {
        assertEquals("Expected events in all primary queues are drained but actual is "
            + abstractSender.getEventQueueSize() + ". Queue content is: "
            + displayQueueContent(queue), 0, abstractSender.getEventQueueSize());
      });
      assertEquals("Expected events in all primary queues after drain is 0", 0,
          abstractSender.getEventQueueSize());
      await().untilAsserted(() -> {
        assertEquals("Expected events in all secondary queues are drained but actual is "
            + abstractSender.getSecondaryEventQueueSize() + ". Queue content is: "
            + displayQueueContent(queue), 0, abstractSender.getSecondaryEventQueueSize());
      });
      assertEquals("Expected events in all secondary queues after drain is 0", 0,
          abstractSender.getSecondaryEventQueueSize());
    } finally {
      exp.remove();
      exp1.remove();
    }

  }

  public static Integer validateAfterAck(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    final MyGatewayEventFilter_AfterAck filter =
        (MyGatewayEventFilter_AfterAck) sender.getGatewayEventFilters().get(0);
    return filter.getAckList().size();
  }

  public static int verifyAndGetEventsDispatchedByConcurrentDispatchers(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    ConcurrentParallelGatewaySenderEventProcessor cProc =
        (ConcurrentParallelGatewaySenderEventProcessor) ((AbstractGatewaySender) sender)
            .getEventProcessor();
    if (cProc == null)
      return 0;

    int totalDispatched = 0;
    for (ParallelGatewaySenderEventProcessor lProc : cProc.getProcessors()) {
      totalDispatched += lProc.getNumEventsDispatched();
    }
    assertTrue(totalDispatched > 0);
    return totalDispatched;
  }

  public static Long getNumberOfEntriesOverflownToDisk(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    long numEntries = 0;
    if (sender.isParallel()) {
      RegionQueue regionQueue;
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      numEntries = ((ConcurrentParallelGatewaySenderQueue) regionQueue)
          .getNumEntriesOverflowOnDiskTestOnly();
    }
    return numEntries;
  }

  public static Long getNumberOfEntriesInVM(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    RegionQueue regionQueue;
    long numEntries = 0;
    if (sender.isParallel()) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      numEntries = ((ConcurrentParallelGatewaySenderQueue) regionQueue).getNumEntriesInVMTestOnly();
    }
    return numEntries;
  }

  public static void verifyTmpDroppedEventSize(String senderId, int size) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    AbstractGatewaySender ags = (AbstractGatewaySender) sender;
    await().untilAsserted(() -> assertEquals("Expected tmpDroppedEvents size: " + size
        + " but actual size: " + ags.getTmpDroppedEventSize(), size, ags.getTmpDroppedEventSize()));
  }

  public static void verifyQueueSize(String senderId, int size) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (!sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int queueSize = 0;
      for (RegionQueue q : queues) {
        queueSize += q.size();
      }

      assertEquals("verifyQueueSize failed for sender " + senderId, size, queueSize);
    } else if (sender.isParallel()) {
      RegionQueue regionQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      assertEquals("verifyQueueSize failed for sender " + senderId, size, regionQueue.size());
    }
  }

  public static void verifyRegionQueueNotEmpty(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (!sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int queueSize = 0;
      for (RegionQueue q : queues) {
        queueSize += q.size();
      }
      assertTrue(queues.size() > 0);
      assertTrue(queueSize > 0);
    } else if (sender.isParallel()) {
      RegionQueue regionQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      assertTrue(regionQueue.size() > 0);
    }
  }

  public static void waitForConcurrentSerialSenderQueueToDrain(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender =
        senders.stream().filter(s -> s.getId().equals(senderId)).findFirst().get();

    await().untilAsserted(() -> {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      for (RegionQueue q : queues) {
        assertEquals(0, q.size());
      }
    });
  }

  /**
   * Test methods for sender operations
   *
   */
  public static void verifySenderPausedState(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    assertTrue(sender.isPaused());
  }

  public static void verifySenderResumedState(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    assertFalse(sender.isPaused());
    assertTrue(sender.isRunning());
  }

  public static void verifySenderStoppedState(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    assertFalse(sender.isRunning());
  }

  public static void verifySenderRunningState(String senderId) {
    GatewaySender sender = cache.getGatewaySender(senderId);
    assertTrue(sender.isRunning());
  }

  public static void verifySenderConnectedState(String senderId, boolean shouldBeConnected) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    if (shouldBeConnected) {
      assertThat(sender.getEventProcessor().getDispatcher().isConnectedToRemote()).isTrue();
    } else {
      assertThat(sender.getEventProcessor().getDispatcher().isConnectedToRemote()).isFalse();
    }
  }

  public static void verifyPool(String senderId, boolean poolShouldExist,
      int expectedPoolLocatorsSize) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    PoolImpl pool = sender.getProxy();
    if (poolShouldExist) {
      assertNotNull(pool);
      assertEquals(expectedPoolLocatorsSize, pool.getLocators().size());
    } else {
      assertNull(pool);
    }
  }

  public static void removeSenderFromTheRegion(String senderId, String regionName) {
    Region region = cache.getRegion(regionName);
    assertNotNull(region);
    region.getAttributesMutator().removeGatewaySenderId(senderId);
  }

  public static void destroySender(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    sender.destroy();
  }

  public static void verifySenderDestroyed(String senderId, boolean isParallel) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender) s;
        break;
      }
    }
    assertNull(sender);

    String queueRegionNameSuffix = null;
    if (isParallel) {
      queueRegionNameSuffix = ParallelGatewaySenderQueue.QSTRING;
    } else {
      queueRegionNameSuffix = "_SERIAL_GATEWAY_SENDER_QUEUE";
    }

    Set<InternalRegion> allRegions = ((GemFireCacheImpl) cache).getAllRegions();
    for (InternalRegion region : allRegions) {
      if (region.getName().indexOf(senderId + queueRegionNameSuffix) != -1) {
        fail("Region underlying the sender is not destroyed.");
      }
    }
  }

  public static void destroyAsyncEventQueue(String id) {
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(id);
    assertNotNull(aeq);
    aeq.destroy();
  }

  protected static void verifyListenerEvents(final long expectedNumEvents) {
    await()
        .until(() -> listener1.getNumEvents() == expectedNumEvents);
  }

  protected Integer[] createLNAndNYLocators() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));
    return new Integer[] {lnPort, nyPort};
  }

  protected void validateRegionSizes(String regionName, int expectedRegionSize, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> validateRegionSize(regionName, expectedRegionSize));
    }
  }

  public static class MyLocatorCallback extends LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();

    private final Set removedLocators = new HashSet();

    @Override
    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    @Override
    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    public boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }


    private synchronized boolean waitFor(Set set, InetSocketAddress locator, long time)
        throws InterruptedException {
      long remaining = time;
      long endTime = System.currentTimeMillis() + time;
      while (!set.contains(locator) && remaining >= 0) {
        wait(remaining);
        remaining = endTime - System.currentTimeMillis();
      }
      return set.contains(locator);
    }

    public synchronized Set getDiscovered() {
      return new HashSet(discoveredLocators);
    }

  }

  protected static class PutTask implements Callable {
    private final Region region;

    private final AtomicInteger key_value;

    private final int numPuts;

    public PutTask(Region region, AtomicInteger key_value, int numPuts) {
      this.region = region;
      this.key_value = key_value;
      this.numPuts = numPuts;
    }

    @Override
    public Object call() throws Exception {
      while (true) {
        int key = key_value.incrementAndGet();
        if (key < numPuts) {
          region.put(key, key);
        } else {
          break;
        }
      }
      return null;
    }
  }

  public static class MyGatewayEventFilter implements GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter";

    boolean beforeEnqueueInvoked;
    boolean beforeTransmitInvoked;
    boolean afterAckInvoked;

    public MyGatewayEventFilter() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      this.beforeEnqueueInvoked = true;
      return !((Long) event.getKey() >= 500 && (Long) event.getKey() < 600);
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      this.beforeTransmitInvoked = true;
      return !((Long) event.getKey() >= 600 && (Long) event.getKey() < 700);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      this.afterAckInvoked = true;
      // TODO Auto-generated method stub
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter))
        return false;
      MyGatewayEventFilter filter = (MyGatewayEventFilter) obj;
      return this.Id.equals(filter.Id);
    }
  }

  public static class MyGatewayEventFilter_AfterAck implements GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter_AfterAck";

    ConcurrentSkipListSet<Long> ackList = new ConcurrentSkipListSet<Long>();

    public MyGatewayEventFilter_AfterAck() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      ackList.add((Long) event.getKey());
    }

    public Set getAckList() {
      return ackList;
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter))
        return false;
      MyGatewayEventFilter filter = (MyGatewayEventFilter) obj;
      return this.Id.equals(filter.Id);
    }
  }

  public static class PDXGatewayEventFilter implements GatewayEventFilter, Serializable {

    String Id = "PDXGatewayEventFilter";

    public int beforeEnqueueInvoked;
    public int beforeTransmitInvoked;
    public int afterAckInvoked;

    public PDXGatewayEventFilter() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      System.out.println("Invoked enqueue for " + event);
      this.beforeEnqueueInvoked++;
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      System.out.println("Invoked transmit for " + event);
      this.beforeTransmitInvoked++;
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      System.out.println("Invoked afterAck for " + event);
      this.afterAckInvoked++;
      // TODO Auto-generated method stub
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter))
        return false;
      MyGatewayEventFilter filter = (MyGatewayEventFilter) obj;
      return this.Id.equals(filter.Id);
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    cleanupVM();
    List<AsyncInvocation> invocations = new ArrayList<AsyncInvocation>();
    final Host host = getHost(0);
    for (int i = 0; i < host.getVMCount(); i++) {
      invocations.add(host.getVM(i).invokeAsync(() -> WANTestBase.cleanupVM()));
    }
    for (AsyncInvocation invocation : invocations) {
      invocation.await();
    }
  }

  public static void cleanupVM() throws IOException {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    closeCache();
    JUnit4DistributedTestCase.cleanDiskDirs();
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      cache = null;
    } else {
      WANTestBase test = new WANTestBase();
      if (test.isConnectedToDS()) {
        test.getSystem().disconnect();
      }
    }
  }

  public static void deletePDXDir() throws IOException {
    File pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    FileUtils.deleteDirectory(pdxDir);
  }

  public static void shutdownLocator() {
    WANTestBase test = new WANTestBase();
    test.getSystem().disconnect();
  }

  public static void printEventListenerMap() {
    ((MyGatewaySenderEventListener) eventListener1).printMap();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    // For now all WANTestBase tests allocate off-heap memory even though
    // many of them never use it.
    // The problem is that WANTestBase has static methods that create instances
    // of WANTestBase (instead of instances of the subclass). So we can't override
    // this method so that only the off-heap subclasses allocate off heap memory.
    Properties props = new Properties();
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "200m");

    return props;
  }

  /**
   * Returns true if the test should create off-heap regions. OffHeap tests should over-ride this
   * method and return false.
   */
  public boolean isOffHeap() {
    return false;
  }

  /**
   * Checks whether a Async Queue MBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkAsyncQueueMBean(final VM vm, final boolean shouldExist) {
    SerializableRunnable checkAsyncQueueMBean =
        new SerializableRunnable("Check Async Queue MBean") {
          @Override
          public void run() {
            ManagementService service = ManagementService.getManagementService(cache);
            AsyncEventQueueMXBean bean = service.getLocalAsyncEventQueueMXBean("pn");
            if (shouldExist) {
              assertNotNull(bean);
            } else {
              assertNull(bean);
            }
          }
        };
    vm.invoke(checkAsyncQueueMBean);
  }

  /**
   * Checks Proxy GatewayReceiver
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkProxyReceiver(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkProxySender = new SerializableRunnable("Check Proxy Receiver") {
      @Override
      public void run() {
        ManagementService service = ManagementService.getManagementService(cache);
        GatewayReceiverMXBean bean = null;
        try {
          bean = MBeanUtil.getGatewayReceiverMbeanProxy(senderMember);
        } catch (Exception e) {
          fail("Could not obtain Sender Proxy in desired time " + e);
        }
        assertNotNull(bean);
        final ObjectName receiverMBeanName = service.getGatewayReceiverMBeanName(senderMember);
        try {
          MBeanUtil.printBeanDetails(receiverMBeanName);
        } catch (Exception e) {
          fail("Error while Printing Bean Details " + e);
        }

      }
    };
    vm.invoke(checkProxySender);
  }

  /**
   * Checks Proxy GatewaySender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkProxySender(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkProxySender = new SerializableRunnable("Check Proxy Sender") {
      @Override
      public void run() {
        ManagementService service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = null;
        try {
          bean = MBeanUtil.getGatewaySenderMbeanProxy(senderMember, "pn");
        } catch (Exception e) {
          fail("Could not obtain Sender Proxy in desired time " + e);
        }
        assertNotNull(bean);
        final ObjectName senderMBeanName = service.getGatewaySenderMBeanName(senderMember, "pn");
        try {
          MBeanUtil.printBeanDetails(senderMBeanName);
        } catch (Exception e) {
          fail("Error while Printing Bean Details " + e);
        }

        if (service.isManager()) {
          DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
          await().untilAsserted(() -> {
            Map<String, Boolean> dsMap = dsBean.viewRemoteClusterStatus();
            dsMap.entrySet().stream()
                .forEach(entry -> assertTrue("Should be true " + entry.getKey(), entry.getValue()));
          });
        }

      }
    };
    vm.invoke(checkProxySender);
  }

  /**
   * Checks whether a GatewayReceiverMBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkReceiverMBean(final VM vm) {
    SerializableRunnable checkMBean = new SerializableRunnable("Check Receiver MBean") {
      @Override
      public void run() {
        ManagementService service = ManagementService.getManagementService(cache);
        GatewayReceiverMXBean bean = service.getLocalGatewayReceiverMXBean();
        assertNotNull(bean);
      }
    };
    vm.invoke(checkMBean);
  }

  @SuppressWarnings("serial")
  public static void checkReceiverNavigationAPIS(final VM vm,
      final DistributedMember receiverMember) {
    SerializableRunnable checkNavigationAPIS =
        new SerializableRunnable("Check Receiver Navigation APIs") {
          @Override
          public void run() {
            ManagementService service = ManagementService.getManagementService(cache);
            DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
            ObjectName expectedName = service.getGatewayReceiverMBeanName(receiverMember);
            try {
              await("wait for member to be added to DistributedSystemBridge membership list")
                  .untilAsserted(
                      () -> assertThat(bean.fetchGatewayReceiverObjectName(receiverMember.getId()))
                          .isNotNull());
              ObjectName actualName = bean.fetchGatewayReceiverObjectName(receiverMember.getId());
              assertEquals(expectedName, actualName);
            } catch (Exception e) {
              fail("Receiver Navigation Failed " + e);
            }

            assertEquals(1, bean.listGatewayReceiverObjectNames().length);

          }
        };
    vm.invoke(checkNavigationAPIS);
  }

  /**
   * Checks whether a GatewayReceiverMBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkSenderMBean(final VM vm, final String regionPath, boolean connected) {
    SerializableRunnable checkMBean = new SerializableRunnable("Check Sender MBean") {
      @Override
      public void run() {
        ManagementService service = ManagementService.getManagementService(cache);

        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        await()
            .untilAsserted(() -> assertEquals(connected, bean.isConnected()));

        ObjectName regionBeanName = service.getRegionMBeanName(
            cache.getDistributedSystem().getDistributedMember(), SEPARATOR + regionPath);
        RegionMXBean rBean = service.getMBeanInstance(regionBeanName, RegionMXBean.class);
        assertTrue(rBean.isGatewayEnabled());


      }
    };
    vm.invoke(checkMBean);
  }

  @SuppressWarnings("serial")
  public static void checkSenderNavigationAPIS(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkNavigationAPIS =
        new SerializableRunnable("Check Sender Navigation APIs") {
          @Override
          public void run() {
            ManagementService service = ManagementService.getManagementService(cache);
            DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
            ObjectName expectedName = service.getGatewaySenderMBeanName(senderMember, "pn");
            try {
              ObjectName actualName = bean.fetchGatewaySenderObjectName(senderMember.getId(), "pn");
              assertEquals(expectedName, actualName);
            } catch (Exception e) {
              fail("Sender Navigation Failed " + e);
            }

            assertEquals(2, bean.listGatewaySenderObjectNames().length);
            try {
              assertEquals(1, bean.listGatewaySenderObjectNames(senderMember.getId()).length);
            } catch (Exception e) {
              fail("Sender Navigation Failed " + e);
            }

          }
        };
    vm.invoke(checkNavigationAPIS);
  }

  /**
   * start a gateway sender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void startGatewaySender(final VM vm) {
    SerializableRunnable stopGatewaySender = new SerializableRunnable("Start Gateway Sender") {
      @Override
      public void run() {
        ManagementService service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        bean.start();
        assertTrue(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }

  /**
   * stops a gateway sender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void stopGatewaySender(final VM vm) {
    SerializableRunnable stopGatewaySender = new SerializableRunnable("Stop Gateway Sender") {
      @Override
      public void run() {
        ManagementService service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        bean.stop();
        assertFalse(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }

  /**
   * Checks Proxy Async Queue
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkProxyAsyncQueue(final VM vm, final DistributedMember senderMember,
      final boolean shouldExist) {
    SerializableRunnable checkProxyAsyncQueue =
        new SerializableRunnable("Check Proxy Async Queue") {
          @Override
          public void run() {
            SystemManagementService service =
                (SystemManagementService) ManagementService.getManagementService(cache);
            final ObjectName queueMBeanName =
                service.getAsyncEventQueueMBeanName(senderMember, "pn");
            AsyncEventQueueMXBean bean = null;
            if (shouldExist) {
              // Verify the MBean proxy exists
              try {
                bean = MBeanUtil.getAsyncEventQueueMBeanProxy(senderMember, "pn");
              } catch (Exception e) {
                fail("Could not obtain Sender Proxy in desired time " + e);
              }
              assertNotNull(bean);

              try {
                MBeanUtil.printBeanDetails(queueMBeanName);
              } catch (Exception e) {
                fail("Error while Printing Bean Details " + e);
              }
            } else {
              // Verify the MBean proxy doesn't exist
              bean = service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class);
              assertNull(bean);
            }
          }
        };
    vm.invoke(checkProxyAsyncQueue);
  }

  public static DistributedMember getMember() {
    return ((GemFireCacheImpl) cache).getMyId();
  }

  public static ManagementService getManagementService() {
    return ManagementService.getManagementService(cache);
  }

  /**
   * Checks Proxy GatewaySender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkRemoteClusterStatus(final VM vm, final DistributedMember senderMember) {
    SerializableRunnable checkProxySender = new SerializableRunnable("DS Map Size") {
      @Override
      public void run() {
        await().untilAsserted(() -> {
          final ManagementService service = ManagementService.getManagementService(cache);
          final DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
          assertEquals(
              "Failed while waiting for getDistributedSystemMXBean to complete and get results",
              true, dsBean != null);
        });
        ManagementService service = ManagementService.getManagementService(cache);
        final DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
        assertNotNull(dsBean);
        Map<String, Boolean> dsMap = dsBean.viewRemoteClusterStatus();
        logger.info("Ds Map is: " + dsMap.size());
        assertNotNull(dsMap);
        assertEquals(true, dsMap.size() > 0);
      }
    };
    vm.invoke(checkProxySender);
  }


}
