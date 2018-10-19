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
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueStats;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;


public class AsyncEventQueueTestBase extends JUnit4DistributedTestCase {

  protected static Cache cache;

  protected static VM vm0;

  protected static VM vm1;

  protected static VM vm2;

  protected static VM vm3;

  protected static VM vm4;

  protected static AsyncEventListener eventListener1;

  private static final long MAX_WAIT = 60000;

  protected static GatewayEventFilter eventFilter;

  protected static boolean destroyFlag = false;

  protected static List<Integer> dispatcherThreads = new ArrayList<Integer>(Arrays.asList(1, 3, 5));

  // this will be set for each test method run with one of the values from above
  // list
  protected static int numDispatcherThreadsForTheRun = 1;

  public AsyncEventQueueTestBase() {
    super();
  }

  @Override
  public final void preSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    vm4 = host.getVM(4);
  }

  @Override
  public final void postSetUp() throws Exception {
    // this is done to vary the number of dispatchers for sender
    // during every test method run
    shuffleNumDispatcherThreads();
    Invoke.invokeInEveryVM(() -> setNumDispatcherThreadsForTheRun(dispatcherThreads.get(0)));
  }

  public static void shuffleNumDispatcherThreads() {
    Collections.shuffle(dispatcherThreads);
  }

  public static void setNumDispatcherThreadsForTheRun(int numThreads) {
    numDispatcherThreadsForTheRun = numThreads;
  }

  public static Integer createFirstLocatorWithDSId(int dsId) {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    // props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + port + "]");
    props.setProperty(START_LOCATOR,
        "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    test.startLocatorDistributedSystem(props);
    return port;
  }

  public static Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + port + "]");
    props.setProperty(START_LOCATOR,
        "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    test.startLocatorDistributedSystem(props);
    return port;
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

  public static void createReplicatedRegionWithAsyncEventQueue(String regionName,
      String asyncQueueIds, Boolean offHeap) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {
      AttributesFactory fact = new AttributesFactory();
      addAsyncEventQueueIds(fact, asyncQueueIds);
      fact.setDataPolicy(DataPolicy.REPLICATE);
      fact.setOffHeap(offHeap);
      RegionFactory regionFactory = cache.createRegionFactory(fact.create());
      Region r = regionFactory.create(regionName);
      assertNotNull(r);
    } finally {
      exp1.remove();
    }
  }

  public static void createReplicatedRegionWithCacheLoaderAndAsyncEventQueue(String regionName,
      String asyncQueueIds) {

    AttributesFactory fact = new AttributesFactory();
    addAsyncEventQueueIds(fact, asyncQueueIds);
    fact.setDataPolicy(DataPolicy.REPLICATE);
    // set the CacheLoader
    fact.setCacheLoader(new MyCacheLoader());
    RegionFactory regionFactory = cache.createRegionFactory(fact.create());
    Region r = regionFactory.create(regionName);
    assertNotNull(r);
  }

  private static void addAsyncEventQueueIds(AttributesFactory fact, String asyncQueueIds) {
    if (asyncQueueIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(asyncQueueIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String asyncQueueId = tokenizer.nextToken();
        fact.addAsyncEventQueueId(asyncQueueId);
      }
    }
  }

  public static void createReplicatedRegionWithSenderAndAsyncEventQueue(String regionName,
      String senderIds, String asyncChannelId, Boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    try {

      AttributesFactory fact = new AttributesFactory();
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setDataPolicy(DataPolicy.REPLICATE);
      fact.setOffHeap(offHeap);
      fact.setScope(Scope.DISTRIBUTED_ACK);
      RegionFactory regionFactory = cache.createRegionFactory(fact.create());
      regionFactory.addAsyncEventQueueId(asyncChannelId);
      Region r = regionFactory.create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
    }
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous) {
    createAsyncEventQueue(asyncChannelId, isParallel, maxMemory, batchSize, isConflation,
        isPersistent, diskStoreName, isDiskSynchronous, new MyAsyncEventListener());
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous,
      final AsyncEventListener asyncEventListener) {
    createAsyncEventQueue(asyncChannelId, isParallel, maxMemory, batchSize, isConflation,
        isPersistent, diskStoreName, isDiskSynchronous, numDispatcherThreadsForTheRun,
        asyncEventListener);
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, int numDispatcherThreads,
      final AsyncEventListener asyncEventListener) {
    createDiskStore(asyncChannelId, diskStoreName);

    AsyncEventQueueFactory factory = getInitialAsyncEventQueueFactory(isParallel, maxMemory,
        batchSize, isPersistent, diskStoreName);
    factory.setDiskSynchronous(isDiskSynchronous);
    factory.setBatchConflationEnabled(isConflation);
    // set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreads);
    // Set GatewayEventSubstitutionFilter
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
  }

  private static void createDiskStore(String asyncChannelId, String diskStoreName) {
    if (diskStoreName != null) {
      File directory = new File(
          asyncChannelId + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      File[] dirs1 = new File[] {directory};
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      DiskStore ds = dsf.create(diskStoreName);
    }
  }

  public static void createAsyncEventQueueWithListener2(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isPersistent, String diskStoreName) {

    createDiskStore(asyncChannelId, diskStoreName);

    AsyncEventListener asyncEventListener = new MyAsyncEventListener2();

    AsyncEventQueueFactory factory = getInitialAsyncEventQueueFactory(isParallel, maxMemory,
        batchSize, isPersistent, diskStoreName);
    // set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, String asyncListenerClass) throws Exception {
    createAsyncEventQueue(asyncChannelId, isParallel, maxMemory, batchSize, isConflation,
        isPersistent, diskStoreName, isDiskSynchronous, asyncListenerClass, null);
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, String asyncListenerClass,
      String substitutionFilterClass) throws Exception {

    createDiskStore(asyncChannelId, diskStoreName);

    AsyncEventQueueFactory factory = getInitialAsyncEventQueueFactory(isParallel, maxMemory,
        batchSize, isPersistent, diskStoreName);
    factory.setDiskSynchronous(isDiskSynchronous);
    factory.setBatchConflationEnabled(isConflation);
    if (substitutionFilterClass != null) {
      factory.setGatewayEventSubstitutionListener(
          (GatewayEventSubstitutionFilter) getClass(substitutionFilterClass).newInstance());
    }
    // set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId,
        (AsyncEventListener) getClass(asyncListenerClass).newInstance());
  }

  private static Class getClass(String simpleClassName) throws Exception {
    String packagePrefix = "org.apache.geode.internal.cache.wan.";
    String className = packagePrefix + simpleClassName;
    Class clazz = null;
    clazz = Class.forName(className);
    return clazz;
  }

  public static void createAsyncEventQueueWithCustomListener(String asyncChannelId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, String diskStoreName, boolean isDiskSynchronous) {
    createAsyncEventQueueWithCustomListener(asyncChannelId, isParallel, maxMemory, batchSize,
        isConflation, isPersistent, diskStoreName, isDiskSynchronous,
        GatewaySender.DEFAULT_DISPATCHER_THREADS);
  }

  public static void createAsyncEventQueueWithCustomListener(String asyncChannelId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, String diskStoreName, boolean isDiskSynchronous, int nDispatchers) {

    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());

    try {
      createDiskStore(asyncChannelId, diskStoreName);

      AsyncEventListener asyncEventListener = new CustomAsyncEventListener();

      AsyncEventQueueFactory factory = getInitialAsyncEventQueueFactory(isParallel, maxMemory,
          batchSize, isPersistent, diskStoreName);
      factory.setDispatcherThreads(nDispatchers);
      AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
    } finally {
      exp.remove();
    }
  }

  private static AsyncEventQueueFactory getInitialAsyncEventQueueFactory(boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isPersistent, String diskStoreName) {
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setPersistent(isPersistent);
    factory.setDiskStoreName(diskStoreName);
    factory.setMaximumQueueMemory(maxMemory);
    factory.setParallel(isParallel);
    return factory;
  }

  public static void createConcurrentAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, int dispatcherThreads, OrderPolicy policy) {

    createDiskStore(asyncChannelId, diskStoreName);

    AsyncEventListener asyncEventListener = new MyAsyncEventListener();

    AsyncEventQueueFactory factory = getInitialAsyncEventQueueFactory(isParallel, maxMemory,
        batchSize, isPersistent, diskStoreName);
    factory.setDiskSynchronous(isDiskSynchronous);
    factory.setBatchConflationEnabled(isConflation);
    factory.setOrderPolicy(policy);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
  }

  public static String createAsyncEventQueueWithDiskStore(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isPersistent, String diskStoreName) {

    AsyncEventListener asyncEventListener = new MyAsyncEventListener();

    File persistentDirectory = null;
    if (diskStoreName == null) {
      persistentDirectory = new File(
          asyncChannelId + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    } else {
      persistentDirectory = new File(diskStoreName);
    }
    LogWriterUtils.getLogWriter().info("The ds is : " + persistentDirectory.getName());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};

    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setParallel(isParallel);
    if (isPersistent) {
      factory.setPersistent(isPersistent);
      factory.setDiskStoreName(dsf.setDiskDirs(dirs1).create(asyncChannelId).getName());
    }
    factory.setMaximumQueueMemory(maxMemory);
    // set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
    return persistentDirectory.getName();
  }

  public static void pauseAsyncEventQueue(String asyncChannelId) {
    AsyncEventQueue theChannel = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncChannelId.equals(asyncChannel.getId())) {
        theChannel = asyncChannel;
      }
    }

    ((AsyncEventQueueImpl) theChannel).getSender().pause();
  }

  public static void pauseAsyncEventQueueAndWaitForDispatcherToPause(String asyncChannelId) {
    AsyncEventQueue theChannel = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncChannelId.equals(asyncChannel.getId())) {
        theChannel = asyncChannel;
        break;
      }
    }

    ((AsyncEventQueueImpl) theChannel).getSender().pause();

    ((AbstractGatewaySender) ((AsyncEventQueueImpl) theChannel).getSender()).getEventProcessor()
        .waitForDispatcherToPause();
  }

  public static void resumeAsyncEventQueue(String asyncQueueId) {
    AsyncEventQueue theQueue = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theQueue = asyncChannel;
      }
    }

    ((AsyncEventQueueImpl) theQueue).getSender().resume();
  }

  public static void waitForAsyncEventQueueSize(String senderId, int numQueueEntries,
      boolean localSize) {
    await()
        .untilAsserted(() -> checkAsyncEventQueueSize(senderId, numQueueEntries, localSize));
  }

  public static void checkAsyncEventQueueSize(String asyncQueueId, int numQueueEntries) {
    checkAsyncEventQueueSize(asyncQueueId, numQueueEntries, false);
  }

  public static void checkAsyncEventQueueSize(String asyncQueueId, int numQueueEntries,
      boolean localSize) {
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(asyncQueueId);
    GatewaySender sender = aeq.getSender();

    if (sender.isParallel()) {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      Region queueRegion = queues.toArray(new RegionQueue[queues.size()])[0].getRegion();
      if (localSize) {
        queueRegion = PartitionRegionHelper.getLocalData(queueRegion);
      }
      assertEquals(numQueueEntries, queueRegion.size());
    } else {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }

  public static void createPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      AttributesFactory fact = new AttributesFactory();
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          // GatewaySender sender = cache.getGatewaySender(senderId);
          // assertNotNull(sender);
          fact.addGatewaySenderId(senderId);
        }
      }
      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      fact.setPartitionAttributes(pfact.create());
      Region r = cache.createRegionFactory(fact.create()).create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createPartitionedRegionWithAsyncEventQueue(String regionName,
      String asyncEventQueueId, Boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      AttributesFactory fact = new AttributesFactory();

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(16);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create()).addAsyncEventQueueId(asyncEventQueueId)
          .create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createFixedPartitionedRegionWithAsyncEventQueue(String regionName,
      String asyncEventQueueId, String partitionName, final List<String> allPartitions,
      boolean offHeap) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      AttributesFactory fact = new AttributesFactory();

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(16);
      pfact.addFixedPartitionAttributes(
          FixedPartitionAttributes.createFixedPartition(partitionName, true));
      pfact.setPartitionResolver(new MyFixedPartitionResolver(allPartitions));
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create()).addAsyncEventQueueId(asyncEventQueueId)
          .create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createColocatedPartitionedRegionWithAsyncEventQueue(String regionName,
      String asyncEventQueueId, Integer totalNumBuckets, String colocatedWith) {

    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
    try {
      AttributesFactory fact = new AttributesFactory();

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setColocatedWith(colocatedWith);
      fact.setPartitionAttributes(pfact.create());
      Region r = cache.createRegionFactory(fact.create()).addAsyncEventQueueId(asyncEventQueueId)
          .create(regionName);
      assertNotNull(r);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createPartitionedRegionWithCacheLoaderAndAsyncQueue(String regionName,
      String asyncEventQueueId) {

    AttributesFactory fact = new AttributesFactory();

    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(16);
    fact.setPartitionAttributes(pfact.create());
    // set the CacheLoader implementation
    fact.setCacheLoader(new MyCacheLoader());
    Region r = cache.createRegionFactory(fact.create()).addAsyncEventQueueId(asyncEventQueueId)
        .create(regionName);
    assertNotNull(r);
  }

  /**
   * Create PartitionedRegion with 1 redundant copy
   */
  public static void createPRWithRedundantCopyWithAsyncEventQueue(String regionName,
      String asyncEventQueueId, Boolean offHeap) throws InterruptedException {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());


    CountDownLatch recoveryDone = new CountDownLatch(2);

    ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
      @Override
      public void recoveryFinished(Region region) {
        recoveryDone.countDown();
      }
    };
    InternalResourceManager.setResourceObserver(observer);


    try {
      AttributesFactory fact = new AttributesFactory();

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(16);
      pfact.setRedundantCopies(1);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create()).addAsyncEventQueueId(asyncEventQueueId)
          .create(regionName);
      assertNotNull(r);
      recoveryDone.await();
    } finally {
      exp.remove();
    }
  }

  public static void createPartitionedRegionAccessorWithAsyncEventQueue(String regionName,
      String asyncEventQueueId) {
    AttributesFactory fact = new AttributesFactory();
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(16);
    pfact.setLocalMaxMemory(0);
    fact.setPartitionAttributes(pfact.create());
    Region r = cache.createRegionFactory(fact.create()).addAsyncEventQueueId(asyncEventQueueId)
        .create(regionName);
    // fact.create()).create(regionName);
    assertNotNull(r);
  }

  protected static void createCache(Integer locPort) {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public static void createCacheWithoutLocator(Integer mCastPort) {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "" + mCastPort);
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public static void checkAsyncEventQueueStats(String queueId, final int queueSize,
      int secondaryQueueSize, final int eventsReceived, final int eventsQueued,
      final int eventsDistributed) {
    Set<AsyncEventQueue> asyncQueues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    boolean isParallel = false;
    for (AsyncEventQueue q : asyncQueues) {
      isParallel = q.isParallel();
      if (q.getId().equals(queueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl) queue).getStatistics();
    await()
        .untilAsserted(
            () -> assertEquals("Expected queue entries: " + queueSize + " but actual entries: "
                + statistics.getEventQueueSize(), queueSize, statistics.getEventQueueSize()));
    if (isParallel) {
      await().untilAsserted(() -> {
        assertEquals(
            "Expected events in the secondary queue is " + secondaryQueueSize + ", but actual is "
                + statistics.getSecondaryEventQueueSize(),
            secondaryQueueSize, statistics.getSecondaryEventQueueSize());
      });
    } else {
      // for serial queue, evenvSecondaryQueueSize is not used
      assertEquals(0, statistics.getSecondaryEventQueueSize());
    }
    assertEquals(queueSize, statistics.getEventQueueSize());
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsQueued, statistics.getEventsQueued());
    assert (statistics.getEventsDistributed() >= eventsDistributed);
  }

  public static void checkAsyncEventQueueConflatedStats(String asyncEventQueueId,
      final int eventsConflated) {
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : queues) {
      if (q.getId().equals(asyncEventQueueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl) queue).getStatistics();
    assertEquals(eventsConflated, statistics.getEventsNotQueuedConflated());
  }

  public static void checkAsyncEventQueueStats_Failover(String asyncEventQueueId,
      final int eventsReceived) {
    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : asyncEventQueues) {
      if (q.getId().equals(asyncEventQueueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl) queue).getStatistics();

    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsReceived,
        (statistics.getEventsQueued() + statistics.getUnprocessedTokensAddedByPrimary()
            + statistics.getUnprocessedEventsRemovedByPrimary()));
  }

  public static void checkAsyncEventQueueBatchStats(String asyncQueueId, final int batches) {
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : queues) {
      if (q.getId().equals(asyncQueueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl) queue).getStatistics();
    assert (statistics.getBatchesDistributed() >= batches);
    assertEquals(0, statistics.getBatchesRedistributed());
  }

  public static void checkAsyncEventQueueUnprocessedStats(String asyncQueueId, int events) {
    Set<AsyncEventQueue> asyncQueues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : asyncQueues) {
      if (q.getId().equals(asyncQueueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl) queue).getStatistics();
    assertEquals(events, (statistics.getUnprocessedEventsAddedBySecondary()
        + statistics.getUnprocessedTokensRemovedBySecondary()));
    assertEquals(events, (statistics.getUnprocessedEventsRemovedByPrimary()
        + statistics.getUnprocessedTokensAddedByPrimary()));
  }

  public static void setRemoveFromQueueOnException(String senderId, boolean removeFromQueue) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertNotNull(sender);
    ((AbstractGatewaySender) sender).setRemoveFromQueueOnException(removeFromQueue);
  }

  public static void unsetRemoveFromQueueOnException(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertNotNull(sender);
    ((AbstractGatewaySender) sender).setRemoveFromQueueOnException(false);
  }

  public static void waitForSenderToBecomePrimary(String senderId) {
    Set<GatewaySender> senders = ((GemFireCacheImpl) cache).getAllGatewaySenders();
    final GatewaySender sender = getGatewaySenderById(senders, senderId);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (sender != null && ((AbstractGatewaySender) sender).isPrimary()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected sender primary state to be true but is false";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
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

  public static void createSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManulaStart) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
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
        gateway.setManualStart(isManulaStart);
        // set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
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
        gateway.setBatchConflationEnabled(isConflation);
        gateway.create(dsName, remoteDsId);

      } else {
        GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
        gateway.setMaximumQueueMemory(maxMemory);
        gateway.setBatchSize(batchSize);
        gateway.setManualStart(isManulaStart);
        // set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (filter != null) {
          eventFilter = filter;
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
    } finally {
      exln.remove();
    }
  }

  public static int createReceiver(int locPort) {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");

    InternalDistributedSystem ds = test.getSystem(props);
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
      fail("Test " + test.getName() + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }

  public static String makePath(String[] strings) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strings.length; i++) {
      sb.append(strings[i]);
      sb.append(File.separator);
    }
    return sb.toString();
  }

  /**
   * Do a rebalance and verify balance was improved. If evictionPercentage > 0 (the default) then we
   * have heapLRU and this can cause simulate and rebalance results to differ if eviction kicks in
   * between. (See BUG 44899).
   */
  public static void doRebalance() {
    ResourceManager resMan = cache.getResourceManager();
    boolean heapEviction = (resMan.getEvictionHeapPercentage() > 0);
    RebalanceFactory factory = resMan.createRebalanceFactory();
    try {
      RebalanceResults simulateResults = null;
      if (!heapEviction) {
        LogWriterUtils.getLogWriter().info("Calling rebalance simulate");
        RebalanceOperation simulateOp = factory.simulate();
        simulateResults = simulateOp.getResults();
      }

      LogWriterUtils.getLogWriter().info("Starting rebalancing");
      RebalanceOperation rebalanceOp = factory.start();
      RebalanceResults rebalanceResults = rebalanceOp.getResults();

    } catch (InterruptedException e) {
      Assert.fail("Interrupted", e);
    }
  }

  public static void doPuts(String regionName, int numPuts) {
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(InterruptedException.class.getName());
    IgnoredException exp2 =
        IgnoredException.addIgnoredException(GatewaySenderException.class.getName());
    try {
      Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, i);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
    // for (long i = 0; i < numPuts; i++) {
    // r.destroy(i);
    // }
  }

  public static void doHeavyPuts(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numPuts; i++) {
      r.put(i, new byte[1024 * 1024]);
    }
  }

  /**
   * To be used for CacheLoader related tests
   */
  public static void doGets(String regionName, int numGets) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numGets; i++) {
      r.get(i);
    }
  }

  public static void doPutsFrom(String regionName, int from, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = from; i < numPuts; i++) {
      r.put(i, i);
    }
  }

  public static void doPutAll(String regionName, int numPuts, int size) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numPuts; i++) {
      Map putAllMap = new HashMap();
      for (long j = 0; j < size; j++) {
        putAllMap.put((size * i) + j, i);
      }
      r.putAll(putAllMap, "putAllCallback");
      putAllMap.clear();
    }
  }

  public static void putGivenKeyValue(String regionName, Map keyValues) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (Object key : keyValues.keySet()) {
      r.put(key, keyValues.get(key));
    }
  }

  public static void doNextPuts(String regionName, int start, int numPuts) {
    // waitForSitesToUpdate();
    IgnoredException exp =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {
      Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = start; i < numPuts; i++) {
        r.put(i, i);
      }
    } finally {
      exp.remove();
    }
  }

  public static void validateRegionSize(String regionName, final int regionSize) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {

      final Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          if (r.keySet().size() == regionSize) {
            return true;
          }
          return false;
        }

        public String description() {
          return "Expected region entries: " + regionSize + " but actual entries: "
              + r.keySet().size() + " present region keyset " + r.keySet();
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  /**
   * Validate whether all the attributes set on AsyncEventQueueFactory are set on the sender
   * underneath the AsyncEventQueue.
   */
  public static void validateAsyncEventQueueAttributes(String asyncChannelId, int maxQueueMemory,
      int batchSize, int batchTimeInterval, boolean isPersistent, String diskStoreName,
      boolean isDiskSynchronous, boolean batchConflationEnabled) {

    AsyncEventQueue theChannel = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncChannelId.equals(asyncChannel.getId())) {
        theChannel = asyncChannel;
      }
    }

    GatewaySender theSender = ((AsyncEventQueueImpl) theChannel).getSender();
    assertEquals("maxQueueMemory", maxQueueMemory, theSender.getMaximumQueueMemory());
    assertEquals("batchSize", batchSize, theSender.getBatchSize());
    assertEquals("batchTimeInterval", batchTimeInterval, theSender.getBatchTimeInterval());
    assertEquals("isPersistent", isPersistent, theSender.isPersistenceEnabled());
    assertEquals("diskStoreName", diskStoreName, theSender.getDiskStoreName());
    assertEquals("isDiskSynchronous", isDiskSynchronous, theSender.isDiskSynchronous());
    assertEquals("batchConflation", batchConflationEnabled, theSender.isBatchConflationEnabled());
  }

  /**
   * Validate whether all the attributes set on AsyncEventQueueFactory are set on the sender
   * underneath the AsyncEventQueue.
   */
  public static void validateConcurrentAsyncEventQueueAttributes(String asyncChannelId,
      int maxQueueMemory, int batchSize, int batchTimeInterval, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, boolean batchConflationEnabled,
      int dispatcherThreads, OrderPolicy policy) {

    AsyncEventQueue theChannel = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncChannelId.equals(asyncChannel.getId())) {
        theChannel = asyncChannel;
      }
    }

    GatewaySender theSender = ((AsyncEventQueueImpl) theChannel).getSender();
    assertEquals("maxQueueMemory", maxQueueMemory, theSender.getMaximumQueueMemory());
    assertEquals("batchSize", batchSize, theSender.getBatchSize());
    assertEquals("batchTimeInterval", batchTimeInterval, theSender.getBatchTimeInterval());
    assertEquals("isPersistent", isPersistent, theSender.isPersistenceEnabled());
    assertEquals("diskStoreName", diskStoreName, theSender.getDiskStoreName());
    assertEquals("isDiskSynchronous", isDiskSynchronous, theSender.isDiskSynchronous());
    assertEquals("batchConflation", batchConflationEnabled, theSender.isBatchConflationEnabled());
    assertEquals("dispatcherThreads", dispatcherThreads, theSender.getDispatcherThreads());
    assertEquals("orderPolicy", policy, theSender.getOrderPolicy());
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
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (eventsMap.size() == expectedSize) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected map entries: " + expectedSize + " but actual entries: " + eventsMap.size();
      }
    };
    GeodeAwaitility.await().untilAsserted(wc); // TODO:Yogs
  }

  public static void validateAsyncEventForOperationDetail(String asyncQueueId,
      final int expectedSize, boolean isLoad, boolean isPutAll) {

    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final Map eventsMap = ((MyAsyncEventListener_CacheLoader) theListener).getEventsMap();
    assertNotNull(eventsMap);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (eventsMap.size() == expectedSize) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected map entries: " + expectedSize + " but actual entries: " + eventsMap.size();
      }
    };
    GeodeAwaitility.await().untilAsserted(wc); // TODO:Yogs
    Collection values = eventsMap.values();
    Iterator itr = values.iterator();
    while (itr.hasNext()) {
      AsyncEvent asyncEvent = (AsyncEvent) itr.next();
      if (isLoad)
        assertTrue(asyncEvent.getOperation().isLoad());
      if (isPutAll)
        assertTrue(asyncEvent.getOperation().isPutAll());
    }
  }

  public static void validateCustomAsyncEventListener(String asyncQueueId, final int expectedSize) {
    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final Map eventsMap = ((CustomAsyncEventListener) theListener).getEventsMap();
    assertNotNull(eventsMap);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (eventsMap.size() == expectedSize) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected map entries: " + expectedSize + " but actual entries: " + eventsMap.size();
      }
    };
    GeodeAwaitility.await().untilAsserted(wc); // TODO:Yogs

    Iterator<AsyncEvent> itr = eventsMap.values().iterator();
    while (itr.hasNext()) {
      AsyncEvent event = itr.next();
      assertTrue("possibleDuplicate should be true for event: " + event,
          event.getPossibleDuplicate());
    }
  }

  public static void waitForAsyncQueueToGetEmpty(String asyncQueueId) {
    AsyncEventQueue theAsyncEventQueue = cache.getAsyncEventQueue(asyncQueueId);
    final GatewaySender sender = ((AsyncEventQueueImpl) theAsyncEventQueue).getSender();

    if (sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();

      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          int size = 0;
          for (RegionQueue q : queues) {
            size += q.size();
          }
          if (size == 0) {
            return true;
          }
          return false;
        }

        public String description() {
          int size = 0;
          for (RegionQueue q : queues) {
            size += q.size();
          }
          return "Expected queue size to be : " + 0 + " but actual entries: " + size;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

    } else {
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
          int size = 0;
          for (RegionQueue q : queues) {
            size += q.size();
          }
          if (size == 0) {
            return true;
          }
          return false;
        }

        public String description() {
          Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
          int size = 0;
          for (RegionQueue q : queues) {
            size += q.size();
          }
          return "Expected queue size to be : " + 0 + " but actual entries: " + size;
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);
    }
  }

  public static void verifyAsyncEventListenerForPossibleDuplicates(String asyncEventQueueId,
      Set<Integer> bucketIds, int batchSize) {
    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final Map<Integer, List<GatewaySenderEventImpl>> bucketToEventsMap =
        ((MyAsyncEventListener2) theListener).getBucketToEventsMap();
    assertNotNull(bucketToEventsMap);
    assertTrue(bucketIds.size() > 1);

    for (int bucketId : bucketIds) {
      List<GatewaySenderEventImpl> eventsForBucket = bucketToEventsMap.get(bucketId);
      LogWriterUtils.getLogWriter()
          .info("Events for bucket: " + bucketId + " is " + eventsForBucket);
      assertNotNull(eventsForBucket);
      for (int i = 0; i < batchSize; i++) {
        GatewaySenderEventImpl senderEvent = eventsForBucket.get(i);
        assertTrue(senderEvent.getPossibleDuplicate());
      }
    }
  }

  public static void verifySubstitutionFilterInvocations(String asyncEventQueueId,
      int expectedNumInvocations) {
    AsyncEventQueue queue = cache.getAsyncEventQueue(asyncEventQueueId);
    assertNotNull(queue);

    // Verify the GatewayEventSubstitutionFilter has been invoked the appropriate number of times
    MyGatewayEventSubstitutionFilter filter =
        (MyGatewayEventSubstitutionFilter) queue.getGatewayEventSubstitutionFilter();
    assertNotNull(filter);
    assertEquals(expectedNumInvocations, filter.getNumInvocations());

    // Verify the AsyncEventListener has received the substituted values
    MyAsyncEventListener listener = (MyAsyncEventListener) queue.getAsyncEventListener();
    final Map eventsMap = listener.getEventsMap();
    assertNotNull(eventsMap);
    assertEquals(expectedNumInvocations, eventsMap.size());

    for (Iterator i = eventsMap.entrySet().iterator(); i.hasNext();) {
      Map.Entry<Integer, String> entry = (Map.Entry<Integer, String>) i.next();
      assertEquals(MyGatewayEventSubstitutionFilter.SUBSTITUTION_PREFIX + entry.getKey(),
          entry.getValue());
    }
  }


  public static void verifySubstitutionFilterToDataInvocations(String asyncEventQueueId,
      int expectedToDataInvoations) {
    AsyncEventQueue queue = cache.getAsyncEventQueue(asyncEventQueueId);
    assertNotNull(queue);

    // Verify the GatewayEventSubstitutionFilter has been invoked the appropriate number of times
    SizeableGatewayEventSubstitutionFilter filter =
        (SizeableGatewayEventSubstitutionFilter) queue.getGatewayEventSubstitutionFilter();
    assertNotNull(filter);
    assertEquals(expectedToDataInvoations, filter.getNumToDataInvocations());
  }

  public static AsyncEventListener getAsyncEventListener(String asyncEventQueueId) {
    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncQueue.getId())) {
        return asyncQueue.getAsyncEventListener();
      }
    }
    return null;
  }

  public static int getAsyncEventListenerMapSize(String asyncEventQueueId) {
    AsyncEventListener theListener = getAsyncEventListener(asyncEventQueueId);

    final Map eventsMap = ((MyAsyncEventListener) theListener).getEventsMap();
    assertNotNull(eventsMap);
    LogWriterUtils.getLogWriter().info("The events map size is " + eventsMap.size());
    return eventsMap.size();
  }

  public static int getAsyncEventQueueSize(String asyncEventQueueId) {
    AsyncEventQueue theQueue = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncQueue.getId())) {
        theQueue = asyncQueue;
      }
    }
    assertNotNull(theQueue);
    return theQueue.size();
  }

  public static String getRegionFullPath(String regionName) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    return r.getFullPath();
  }

  public static Set<Integer> getAllPrimaryBucketsOnTheNode(String regionName) {
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    return region.getDataStore().getAllLocalPrimaryBucketIds();
  }

  public static void addCacheListenerAndCloseCache(String regionName) {
    final Region region = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(region);
    CacheListenerAdapter cl = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if ((Long) event.getKey() == 900) {
          cache.getLogger().fine(" Gateway sender is killed by a test");
          cache.close();
          cache.getDistributedSystem().disconnect();
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
        LogWriterUtils.getLogWriter().info("Gateway sender is killed by a test");
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

  public static Boolean killAsyncEventQueue(String asyncQueueId) {
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueueImpl queue = null;
    for (AsyncEventQueue q : queues) {
      if (q.getId().equals(asyncQueueId)) {
        queue = (AsyncEventQueueImpl) q;
        break;
      }
    }
    if (queue.isPrimary()) {
      LogWriterUtils.getLogWriter().info("AsyncEventQueue is killed by a test");
      cache.getDistributedSystem().disconnect();
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  public static void killSender() {
    LogWriterUtils.getLogWriter().info("Gateway sender is going to be killed by a test");
    cache.close();
    cache.getDistributedSystem().disconnect();
    LogWriterUtils.getLogWriter().info("Gateway sender is killed by a test");
  }

  public static class MyLocatorCallback extends LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();

    private final Set removedLocators = new HashSet();

    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    public boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }

    public boolean waitForRemove(InetSocketAddress locator, long time) throws InterruptedException {
      return waitFor(removedLocators, locator, time);
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

    public synchronized Set getRemoved() {
      return new HashSet(removedLocators);
    }
  }

  @Override
  public final void postTearDown() throws Exception {
    cleanupVM();
    vm0.invoke(() -> AsyncEventQueueTestBase.cleanupVM());
    vm1.invoke(() -> AsyncEventQueueTestBase.cleanupVM());
    vm2.invoke(() -> AsyncEventQueueTestBase.cleanupVM());
    vm3.invoke(() -> AsyncEventQueueTestBase.cleanupVM());
    vm4.invoke(() -> AsyncEventQueueTestBase.cleanupVM());
  }

  public static void cleanupVM() throws IOException {
    closeCache();
    JUnit4DistributedTestCase.cleanDiskDirs();
  }

  public static void closeCache() throws IOException {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      cache = null;
    } else {
      AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
      if (test.isConnectedToDS()) {
        test.getSystem().disconnect();
      }
    }
  }

  public static void shutdownLocator() {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
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
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "300m");
    return props;
  }

  /**
   * Returns true if the test should create off-heap regions. OffHeap tests should over-ride this
   * method and return false.
   */
  public boolean isOffHeap() {
    return false;
  }

  private static class MyFixedPartitionResolver implements FixedPartitionResolver {

    private final List<String> allPartitions;

    public MyFixedPartitionResolver(final List<String> allPartitions) {
      this.allPartitions = allPartitions;
    }

    @Override
    public String getPartitionName(final EntryOperation opDetails,
        @Deprecated final Set targetPartitions) {
      int hash = Math.abs(opDetails.getKey().hashCode() % allPartitions.size());
      return allPartitions.get(hash);
    }

    @Override
    public Object getRoutingObject(final EntryOperation opDetails) {
      return opDetails.getKey();
    }

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public void close() {

    }
  }
}


class MyAsyncEventListener_CacheLoader implements AsyncEventListener {
  private final Map eventsMap;

  public MyAsyncEventListener_CacheLoader() {
    this.eventsMap = new ConcurrentHashMap();
  }

  public boolean processEvents(List<AsyncEvent> events) {
    for (AsyncEvent event : events) {
      this.eventsMap.put(event.getKey(), event);
    }
    return true;
  }

  public Map getEventsMap() {
    return eventsMap;
  }

  public void close() {}
}


class MyCacheLoader implements CacheLoader, Declarable {

  public Object load(LoaderHelper helper) {
    Long key = (Long) helper.getKey();
    return "LoadedValue" + "_" + key;
  }

  public void close() {}

  public void init(Properties props) {}

}


class SizeableGatewayEventSubstitutionFilter implements GatewayEventSubstitutionFilter, Declarable {

  private AtomicInteger numToDataInvocations = new AtomicInteger();

  protected static final String SUBSTITUTION_PREFIX = "substituted_";

  public Object getSubstituteValue(EntryEvent event) {
    return new GatewayEventSubstituteObject(this, SUBSTITUTION_PREFIX + event.getKey());
  }

  public void close() {}

  public void init(Properties properties) {}

  protected void incNumToDataInvocations() {
    this.numToDataInvocations.incrementAndGet();
  }

  protected int getNumToDataInvocations() {
    return this.numToDataInvocations.get();
  }
}


class GatewayEventSubstituteObject implements DataSerializable, Sizeable {

  private String id;

  private SizeableGatewayEventSubstitutionFilter filter;

  public GatewayEventSubstituteObject(SizeableGatewayEventSubstitutionFilter filter, String id) {
    this.filter = filter;
    this.id = id;
  }

  public String getId() {
    return this.id;
  }

  public void toData(DataOutput out) throws IOException {
    this.filter.incNumToDataInvocations();
    DataSerializer.writeString(this.id, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = DataSerializer.readString(in);
  }

  public int getSizeInBytes() {
    return 0;
  }

  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[").append("id=")
        .append(this.id).append("]").toString();
  }
}


class MyGatewayEventSubstitutionFilter implements GatewayEventSubstitutionFilter, Declarable {

  private AtomicInteger numInvocations = new AtomicInteger();

  protected static final String SUBSTITUTION_PREFIX = "substituted_";

  public Object getSubstituteValue(EntryEvent event) {
    this.numInvocations.incrementAndGet();
    return SUBSTITUTION_PREFIX + event.getKey();
  }

  public void close() {}

  public void init(Properties properties) {}

  protected int getNumInvocations() {
    return this.numInvocations.get();
  }
}
