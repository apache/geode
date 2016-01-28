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
package com.gemstone.gemfire.internal.cache.wan;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.CustomerIDPartitionResolver;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.Shipment;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import com.gemstone.gemfire.pdx.SimpleClass;
import com.gemstone.gemfire.pdx.SimpleClass1;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;
import com.jayway.awaitility.Awaitility;

import junit.framework.Assert;

public class WANTestBase extends DistributedTestCase{

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
  
  protected static List<QueueListener> gatewayListeners;
  
  protected static AsyncEventListener eventListener1 ;
  protected static AsyncEventListener eventListener2 ;

  private static final long MAX_WAIT = 10000;
  
  protected static GatewayEventFilter eventFilter;
  
  protected static boolean destroyFlag = false;
  
  protected static List<Integer> dispatcherThreads = 
	  new ArrayList<Integer>(Arrays.asList(1, 3, 5));
  //this will be set for each test method run with one of the values from above list
  protected static int numDispatcherThreadsForTheRun = 1;
  
  public WANTestBase(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0); 
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3); 
    vm4 = host.getVM(4);
    vm5 = host.getVM(5);
    vm6 = host.getVM(6); 
    vm7 = host.getVM(7);
    //Need to set the test name after the VMs are created
    super.setUp();
    //this is done to vary the number of dispatchers for sender 
    //during every test method run
    shuffleNumDispatcherThreads();
    invokeInEveryVM(WANTestBase.class,"setNumDispatcherThreadsForTheRun",
    	new Object[]{dispatcherThreads.get(0)});
    addExpectedException("Connection refused");
    addExpectedException("Software caused connection abort");
    addExpectedException("Connection reset");
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

  public static void createLocator(int dsId, int port, Set<String> localLocatorsList, Set<String> remoteLocatorsList){
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    StringBuffer localLocatorBuffer = new StringBuffer(localLocatorsList.toString());
    localLocatorBuffer.deleteCharAt(0);
    localLocatorBuffer.deleteCharAt(localLocatorBuffer.lastIndexOf("]"));
    String localLocator = localLocatorBuffer.toString();
    localLocator = localLocator.replace(" ", ""); 
    
    props.setProperty(DistributionConfig.LOCATORS_NAME, localLocator);
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    StringBuffer remoteLocatorBuffer = new StringBuffer(remoteLocatorsList.toString());
    remoteLocatorBuffer.deleteCharAt(0);
    remoteLocatorBuffer.deleteCharAt(remoteLocatorBuffer.lastIndexOf("]"));
    String remoteLocator = remoteLocatorBuffer.toString();
    remoteLocator = remoteLocator.replace(" ", ""); 
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, remoteLocator);
    test.getSystem(props);
  }
  
  public static Integer createFirstLocatorWithDSId(int dsId) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
    return port;
  }
  
  public static Integer createFirstPeerLocator(int dsId) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=false,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
    return port;
  }
  
  public static Integer createSecondLocator(int dsId, int locatorPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locatorPort + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
    return port;
  }

  public static Integer createSecondPeerLocator(int dsId, int locatorPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locatorPort + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=false,peer=true,hostname-for-clients=localhost");
    test.getSystem(props);
    return port;
  }
  
  public static Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    test.getSystem(props);
    return port;
  }
  
  public static void bringBackLocatorOnOldPort(int dsId, int remoteLocPort, int oldPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + oldPort + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + oldPort + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    test.getSystem(props);
    return;
  }
  
  
  public static Integer createFirstRemotePeerLocator(int dsId, int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=false,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    test.getSystem(props);
    return port;
  }
  
  public static Integer createSecondRemoteLocator(int dsId, int localPort,
      int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + localPort + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    test.getSystem(props);
    return port;
  }
  
  public static Integer createSecondRemotePeerLocator(int dsId, int localPort,
      int remoteLocPort) {
    stopOldLocator();
    WANTestBase test = new WANTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + localPort + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=false,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    test.getSystem(props);
    return port;
  }
  
  public static void createReplicatedRegion(String regionName, String senderIds, Boolean offHeap){
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(InterruptedException.class
        .getName());
    ExpectedException exp2 = addExpectedException(GatewaySenderException.class
        .getName());
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
      fact.setDataPolicy(DataPolicy.REPLICATE);
      fact.setScope(Scope.DISTRIBUTED_ACK);
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create()).create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
      exp1.remove();
      exp2.remove();
    }
  }

  public static void createNormalRegion(String regionName, String senderIds){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    fact.setDataPolicy(DataPolicy.NORMAL);
    fact.setScope(Scope.DISTRIBUTED_ACK);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }
  
//  public static void createReplicatedRegion_PDX(String regionName, String senderId, DataPolicy policy, InterestPolicy intPolicy){
//    AttributesFactory fact = new AttributesFactory();
//    if(senderId!= null){
//      StringTokenizer tokenizer = new StringTokenizer(senderId, ",");
//      while (tokenizer.hasMoreTokens()){
//        String sender = tokenizer.nextToken();
//        //fact.addSerialGatewaySenderId(sender);
//      }
//    }
//    fact.setDataPolicy(policy);
//    SubscriptionAttributes subAttr = new SubscriptionAttributes(intPolicy);
//    fact.setSubscriptionAttributes(subAttr);
//    fact.setScope(Scope.DISTRIBUTED_ACK);
//    Region r = cache.createRegionFactory(fact.create()).create(regionName);
//    assertNotNull(r);
//    assertTrue(r.size() == 0);
//  }
  
  public static void createPersistentReplicatedRegion(String regionName, String senderIds, Boolean offHeap){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    fact.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    fact.setOffHeap(offHeap);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }
  
//  public static void createReplicatedRegionWithParallelSenderId(String regionName, String senderId){
//    AttributesFactory fact = new AttributesFactory();
//    if(senderId!= null){
//      StringTokenizer tokenizer = new StringTokenizer(senderId, ",");
//      while (tokenizer.hasMoreTokens()){
//        String sender = tokenizer.nextToken();
//        //fact.addParallelGatewaySenderId(sender);
//      }
//    }
//    fact.setDataPolicy(DataPolicy.REPLICATE);
//    Region r = cache.createRegionFactory(fact.create()).create(regionName);
//    assertNotNull(r);
//  }
  
//  public static void createReplicatedRegion(String regionName){
//    AttributesFactory fact = new AttributesFactory();
//    fact.setDataPolicy(DataPolicy.REPLICATE);
//    Region r = cache.createRegionFactory(fact.create()).create(regionName);
//    assertNotNull(r);
//  }
  
  public static void createReplicatedRegionWithAsyncEventQueue(
      String regionName, String asyncQueueIds, Boolean offHeap) {
    ExpectedException exp1 = addExpectedException(ForceReattemptException.class
        .getName());
    try {
      AttributesFactory fact = new AttributesFactory();
      if (asyncQueueIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(asyncQueueIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String asyncQueueId = tokenizer.nextToken();
          fact.addAsyncEventQueueId(asyncQueueId);
        }
      }
      fact.setDataPolicy(DataPolicy.REPLICATE);
      fact.setOffHeap(offHeap);
      RegionFactory regionFactory = cache.createRegionFactory(fact.create());
      Region r = regionFactory.create(regionName);
      assertNotNull(r);
    }
    finally {
      exp1.remove();
    }
  }
  
  public static void createPersistentReplicatedRegionWithAsyncEventQueue(
      String regionName, String asyncQueueIds) {
        
    AttributesFactory fact = new AttributesFactory();
    if(asyncQueueIds != null){
      StringTokenizer tokenizer = new StringTokenizer(asyncQueueIds, ",");
      while (tokenizer.hasMoreTokens()){
        String asyncQueueId = tokenizer.nextToken();
        fact.addAsyncEventQueueId(asyncQueueId);
      }
    }
    fact.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    RegionFactory regionFactory = cache.createRegionFactory(fact.create());
    Region r = regionFactory.create(regionName);
    assertNotNull(r);
  }
  
  
  
  public static void createReplicatedRegionWithSenderAndAsyncEventQueue(
      String regionName, String senderIds, String asyncChannelId, Boolean offHeap) {
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
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
      fact.setScope(Scope.DISTRIBUTED_ACK);
      fact.setOffHeap(offHeap);
      RegionFactory regionFactory = cache.createRegionFactory(fact.create());
      regionFactory.addAsyncEventQueueId(asyncChannelId);
      Region r = regionFactory.create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
    }
  }
  
  public static void createReplicatedRegion(String regionName, String senderIds, Scope scope, DataPolicy policy, Boolean offHeap){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    fact.setDataPolicy(policy);
    fact.setScope(scope);
    fact.setOffHeap(offHeap);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }
  
  public static void createAsyncEventQueue(
      String asyncChannelId, boolean isParallel, 
      Integer maxMemory, Integer batchSize, boolean isConflation, 
      boolean isPersistent, String diskStoreName, boolean isDiskSynchronous) {
    
    if (diskStoreName != null) {
      File directory = new File(asyncChannelId + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      File[] dirs1 = new File[] { directory };
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      DiskStore ds = dsf.create(diskStoreName);
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
    //set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
  }
  
  public static void createAsyncEventQueueWithListener2(String asyncChannelId,
      boolean isParallel, Integer maxMemory, Integer batchSize,
      boolean isPersistent, String diskStoreName) {
    
    if (diskStoreName != null) {
      File directory = new File(asyncChannelId + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      File[] dirs1 = new File[] { directory };
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      DiskStore ds = dsf.create(diskStoreName);
    }
    
    AsyncEventListener asyncEventListener = new MyAsyncEventListener2();

    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setPersistent(isPersistent);
    factory.setDiskStoreName(diskStoreName);
    factory.setMaximumQueueMemory(maxMemory);
    factory.setParallel(isParallel);
    //set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId,
        asyncEventListener);
  }
  
  public static void createAsyncEventQueue(
    String asyncChannelId, boolean isParallel, Integer maxMemory, 
    Integer batchSize, boolean isConflation, boolean isPersistent, 
    String diskStoreName, boolean isDiskSynchronous, String asyncListenerClass) throws Exception {
	    
	if (diskStoreName != null) {
	  File directory = new File(asyncChannelId + "_disk_"
		+ System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
	  directory.mkdir();
	  File[] dirs1 = new File[] { directory };
	  DiskStoreFactory dsf = cache.createDiskStoreFactory();
	  dsf.setDiskDirs(dirs1);
	  DiskStore ds = dsf.create(diskStoreName);
	}
	
	String packagePrefix = "com.gemstone.gemfire.internal.cache.wan.";
	String className = packagePrefix + asyncListenerClass;
	AsyncEventListener asyncEventListener = null;
	try {
		Class clazz = Class.forName(className);
		asyncEventListener = (AsyncEventListener) clazz.newInstance();
	} catch (ClassNotFoundException e) {
	  throw e;
	} catch (InstantiationException e) {
	  throw e;
	} catch (IllegalAccessException e) {
	  throw e;
	}
	    
	AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
	factory.setBatchSize(batchSize);
	factory.setPersistent(isPersistent);
	factory.setDiskStoreName(diskStoreName);
	factory.setDiskSynchronous(isDiskSynchronous);
	factory.setBatchConflationEnabled(isConflation);
	factory.setMaximumQueueMemory(maxMemory);
	factory.setParallel(isParallel);
	//set dispatcher threads
	factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
	AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
  }
  
  public static void createAsyncEventQueueWithCustomListener(
      String asyncChannelId, boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous) {
    createAsyncEventQueueWithCustomListener(asyncChannelId, isParallel, maxMemory, batchSize,
        isConflation, isPersistent, diskStoreName, isDiskSynchronous, GatewaySender.DEFAULT_DISPATCHER_THREADS);
  }
  
  public static void createAsyncEventQueueWithCustomListener(
      String asyncChannelId, boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous, int nDispatchers) {

    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());

    try {
      if (diskStoreName != null) {
        File directory = new File(asyncChannelId + "_disk_"
            + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
        directory.mkdir();
        File[] dirs1 = new File[] { directory };
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(dirs1);
        DiskStore ds = dsf.create(diskStoreName);
      }

      AsyncEventListener asyncEventListener = new CustomAsyncEventListener();

      AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
      factory.setBatchSize(batchSize);
      factory.setPersistent(isPersistent);
      factory.setDiskStoreName(diskStoreName);
      factory.setMaximumQueueMemory(maxMemory);
      factory.setParallel(isParallel);
      factory.setDispatcherThreads(nDispatchers);
      AsyncEventQueue asyncChannel = factory.create(asyncChannelId,
          asyncEventListener);
    } finally {
      exp.remove();
    }
  }

  public static void createConcurrentAsyncEventQueue(
      String asyncChannelId, boolean isParallel, 
      Integer maxMemory, Integer batchSize, boolean isConflation, 
      boolean isPersistent, String diskStoreName, boolean isDiskSynchronous,
      int dispatcherThreads, OrderPolicy policy) {
    
    if (diskStoreName != null) {
      File directory = new File(asyncChannelId + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      File[] dirs1 = new File[] { directory };
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      DiskStore ds = dsf.create(diskStoreName);
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
    factory.setDispatcherThreads(dispatcherThreads);
    factory.setOrderPolicy(policy);
    AsyncEventQueue asyncChannel = factory.create(asyncChannelId, asyncEventListener);
  }
  
  
  public static String createAsyncEventQueueWithDiskStore(
      String asyncChannelId, boolean isParallel, 
      Integer maxMemory, Integer batchSize, 
      boolean isPersistent, String diskStoreName) {
    
    AsyncEventListener asyncEventListener = new MyAsyncEventListener();
    
    File persistentDirectory = null;
    if (diskStoreName == null) {
      persistentDirectory = new File(asyncChannelId + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    } else {
      persistentDirectory = new File(diskStoreName); 
    }
    getLogWriter().info("The ds is : " + persistentDirectory.getName());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File [] dirs1 = new File[] {persistentDirectory};
    
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setParallel(isParallel);
    if (isPersistent) {
      factory.setPersistent(isPersistent);
      factory.setDiskStoreName(dsf.setDiskDirs(dirs1).create(asyncChannelId).getName());
    }
    factory.setMaximumQueueMemory(maxMemory);
    //set dispatcher threads
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
    
    ((AsyncEventQueueImpl)theChannel).getSender().pause();
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
    
    ((AsyncEventQueueImpl)theChannel).getSender().pause();
    
    
    ((AbstractGatewaySender)((AsyncEventQueueImpl)theChannel).getSender()).getEventProcessor().waitForDispatcherToPause();
  }
  
 public static void resumeAsyncEventQueue(String asyncQueueId) {
    AsyncEventQueue theQueue = null;
    
    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theQueue = asyncChannel;
      }
    }
    
    ((AsyncEventQueueImpl)theQueue).getSender().resume();
  }
  
  
  public static void checkAsyncEventQueueSize(String asyncQueueId, int numQueueEntries) {
    AsyncEventQueue theAsyncEventQueue = null;
    
    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theAsyncEventQueue = asyncChannel;
      }
    }
    
    GatewaySender sender = ((AsyncEventQueueImpl)theAsyncEventQueue).getSender();
    
    if (sender.isParallel()) {
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      assertEquals(numQueueEntries,
          queues.toArray(new RegionQueue[queues.size()])[0].getRegion().size());
    } else {
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }
  
  /**
   * This method verifies the queue size of a ParallelGatewaySender. For
   * ParallelGatewaySender conflation happens in a separate thread, hence test
   * code needs to wait for some time for expected result
   * 
   * @param asyncQueueId
   *          Async Queue ID
   * @param numQueueEntries
   *          expected number of Queue entries
   * @throws Exception
   */
  public static void waitForAsyncEventQueueSize(String asyncQueueId,
      final int numQueueEntries) throws Exception {
    AsyncEventQueue theAsyncEventQueue = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theAsyncEventQueue = asyncChannel;
      }
    }

    GatewaySender sender = ((AsyncEventQueueImpl) theAsyncEventQueue)
        .getSender();

    if (sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender) sender)
          .getQueues();

      waitForCriterion(new WaitCriterion() {

        public String description() {
          return "Waiting for EventQueue size to be " + numQueueEntries;
        }

        public boolean done() {
          boolean done = numQueueEntries == queues
              .toArray(new RegionQueue[queues.size()])[0].getRegion().size();
          return done;
        }

      }, MAX_WAIT, 500, true);

    } else {
      throw new Exception(
          "This method should be used for only ParallelGatewaySender,SerialGatewaySender should use checkAsyncEventQueueSize() method instead");

    }
  }
  
  public static void createPartitionedRegion(String regionName, String senderIds, Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap){
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(PartitionOfflineException.class
        .getName());
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
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create()).create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
      exp1.remove();
    }
  }
  
  // TODO:OFFHEAP: add offheap flavor
  public static void createPartitionedRegionWithPersistence(String regionName,
      String senderIds, Integer redundantCopies, Integer totalNumBuckets) {
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(PartitionOfflineException.class
        .getName());
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
      fact.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      Region r = cache.createRegionFactory(fact.create()).create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
      exp1.remove();
    }
  }
  public static void createColocatedPartitionedRegion(String regionName,
	      String senderIds, Integer redundantCopies, Integer totalNumBuckets, String colocatedWith) {
	ExpectedException exp = addExpectedException(ForceReattemptException.class
		.getName());
	ExpectedException exp1 = addExpectedException(PartitionOfflineException.class
		.getName());
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
	  pfact.setColocatedWith(colocatedWith);
	  fact.setPartitionAttributes(pfact.create());
	  Region r = cache.createRegionFactory(fact.create()).create(regionName);
	  assertNotNull(r);
	}
	finally {
		exp.remove();
		exp1.remove();
	}
  }
  
  public static void addSenderThroughAttributesMutator(String regionName,
      String senderIds){
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    AttributesMutator mutator = r.getAttributesMutator();
    mutator.addGatewaySenderId(senderIds);
  }
  
  public static void addAsyncEventQueueThroughAttributesMutator(
      String regionName, String queueId) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    AttributesMutator mutator = r.getAttributesMutator();
    mutator.addAsyncEventQueueId(queueId);
  }
  
  public static void createPartitionedRegionWithAsyncEventQueue(
      String regionName, String asyncEventQueueId, Boolean offHeap) {
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(PartitionOfflineException.class
        .getName());
    try {
      AttributesFactory fact = new AttributesFactory();

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(16);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create())
          .addAsyncEventQueueId(asyncEventQueueId).create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
      exp1.remove();
    }
  }
  
  public static void createColocatedPartitionedRegionWithAsyncEventQueue(
    String regionName, String asyncEventQueueId, Integer totalNumBuckets, String colocatedWith) {
	
	ExpectedException exp = addExpectedException(ForceReattemptException.class
	  .getName());
	ExpectedException exp1 = addExpectedException(PartitionOfflineException.class
	  .getName());
	try {
	  AttributesFactory fact = new AttributesFactory();

	  PartitionAttributesFactory pfact = new PartitionAttributesFactory();
	  pfact.setTotalNumBuckets(totalNumBuckets);
	  pfact.setColocatedWith(colocatedWith);
	  fact.setPartitionAttributes(pfact.create());
	  Region r = cache.createRegionFactory(fact.create())
	    .addAsyncEventQueueId(asyncEventQueueId).create(regionName);
	  assertNotNull(r);
	}
	finally {
	  exp.remove();
	  exp1.remove();
	}
  }
  
  public static void createPersistentPartitionedRegionWithAsyncEventQueue(
      String regionName, String asyncEventQueueId) {
    AttributesFactory fact = new AttributesFactory();

    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    fact.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    pfact.setTotalNumBuckets(16);
    fact.setPartitionAttributes(pfact.create());
    if (asyncEventQueueId != null) {
      StringTokenizer tokenizer = new StringTokenizer(asyncEventQueueId, ",");
      while (tokenizer.hasMoreTokens()) {
        String asyncId = tokenizer.nextToken();
        fact.addAsyncEventQueueId(asyncId);
      }
    }
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }
  
  /**
   * Create PartitionedRegion with 1 redundant copy
   */
  public static void createPRWithRedundantCopyWithAsyncEventQueue(
      String regionName, String asyncEventQueueId, Boolean offHeap) {
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());

    try {
      AttributesFactory fact = new AttributesFactory();

      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(16);
      pfact.setRedundantCopies(1);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create())
          .addAsyncEventQueueId(asyncEventQueueId).create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
    }
  }
  
  public static void createPartitionedRegionAccessorWithAsyncEventQueue(
      String regionName, String asyncEventQueueId) {
    AttributesFactory fact = new AttributesFactory();
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(16);
    pfact.setLocalMaxMemory(0);
    fact.setPartitionAttributes(pfact.create());
    Region r = cache.createRegionFactory(
    fact.create()).addAsyncEventQueueId(
    asyncEventQueueId).create(regionName);
    //fact.create()).create(regionName);
    assertNotNull(r);
  }
  
  public static void createPartitionedRegionAsAccessor(
      String regionName, String senderIds, Integer redundantCopies, Integer totalNumBuckets){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    pfact.setLocalMaxMemory(0);
    fact.setPartitionAttributes(pfact.create());
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }
  
  public static void createPartitionedRegionWithSerialParallelSenderIds(String regionName, String serialSenderIds, String parallelSenderIds, String colocatedWith, Boolean offHeap){
    AttributesFactory fact = new AttributesFactory();
    if (serialSenderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(serialSenderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
        // GatewaySender sender = cache.getGatewaySender(senderId);
        // assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    if (parallelSenderIds != null) {
      StringTokenizer tokenizer = new StringTokenizer(parallelSenderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setColocatedWith(colocatedWith);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
  }
  
  public static void createPersistentPartitionedRegion(
      String regionName, 
      String senderIds, 
      Integer redundantCopies, 
      Integer totalNumBuckets,
      Boolean offHeap){
    
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(PartitionOfflineException.class
        .getName());
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
      fact.setPartitionAttributes(pfact.create());
      fact.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      fact.setOffHeap(offHeap);
      Region r = cache.createRegionFactory(fact.create()).create(regionName);
      assertNotNull(r);
    }
    finally {
      exp.remove();
      exp1.remove();
    }
  }
  
  public static void createCustomerOrderShipmentPartitionedRegion(
      String regionName, String senderIds, Integer redundantCopies,
      Integer totalNumBuckets, Boolean offHeap) {
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
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

      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      // creating colocated Regions
      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies)
          .setTotalNumBuckets(totalNumBuckets)
          .setPartitionResolver(
              new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      customerRegion = (PartitionedRegion)cache.createRegionFactory(
          fact.create()).create(customerRegionName);
      assertNotNull(customerRegion);
      getLogWriter().info(
          "Partitioned Region CUSTOMER created Successfully :"
              + customerRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies)
          .setTotalNumBuckets(totalNumBuckets)
          .setColocatedWith(customerRegionName)
          .setPartitionResolver(
              new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact = new AttributesFactory();
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          // GatewaySender sender = cache.getGatewaySender(senderId);
          // assertNotNull(sender);
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      orderRegion = (PartitionedRegion)cache.createRegionFactory(fact.create())
          .create(orderRegionName);
      assertNotNull(orderRegion);
      getLogWriter().info(
          "Partitioned Region ORDER created Successfully :"
              + orderRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies)
          .setTotalNumBuckets(totalNumBuckets)
          .setColocatedWith(orderRegionName)
          .setPartitionResolver(
              new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact = new AttributesFactory();
      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          // GatewaySender sender = cache.getGatewaySender(senderId);
          // assertNotNull(sender);
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      shipmentRegion = (PartitionedRegion)cache.createRegionFactory(
          fact.create()).create(shipmentRegionName);
      assertNotNull(shipmentRegion);
      getLogWriter().info(
          "Partitioned Region SHIPMENT created Successfully :"
              + shipmentRegion.toString());
    }
    finally {
      exp.remove();
    }
  }
  
  public static void createColocatedPartitionedRegions(String regionName, String senderIds, Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
    
    pfact.setColocatedWith(r.getName());
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r1 = cache.createRegionFactory(fact.create()).create(regionName+"_child1");
    assertNotNull(r1);    
    
    Region r2 = cache.createRegionFactory(fact.create()).create(regionName+"_child2");
    assertNotNull(r2);
  }
  
  public static void createColocatedPartitionedRegions2 (String regionName, String senderIds, Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap){
    AttributesFactory fact = new AttributesFactory();
    if(senderIds!= null){
      StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()){
        String senderId = tokenizer.nextToken();
//        GatewaySender sender = cache.getGatewaySender(senderId);
//        assertNotNull(sender);
        fact.addGatewaySenderId(senderId);
      }
    }
    PartitionAttributesFactory pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r = cache.createRegionFactory(fact.create()).create(regionName);
    assertNotNull(r);
    
    
    fact = new AttributesFactory();
    pfact.setColocatedWith(r.getName());
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region r1 = cache.createRegionFactory(fact.create()).create(regionName+"_child1");
    assertNotNull(r1);    
    
    Region r2 = cache.createRegionFactory(fact.create()).create(regionName+"_child2");
    assertNotNull(r2);
  }
  
  public static void createCache(Integer locPort){
    createCache(false, locPort);
  }
  public static void createManagementCache(Integer locPort){
    createCache(true, locPort);
  }
  
  public static void createCacheConserveSockets(Boolean conserveSockets,Integer locPort){
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    props.setProperty(DistributionConfig.CONSERVE_SOCKETS_NAME, conserveSockets.toString());   
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }
  
  protected static void createCache(boolean management, Integer locPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    if (management) {
      props.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
      props.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "false");
      props.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, "0");
      props.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME, "0");
    }
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);    
  }
  
  protected static void createCacheWithSSL(Integer locPort) {
    WANTestBase test = new WANTestBase(testName);

    boolean gatewaySslenabled = true;
    String  gatewaySslprotocols = "any";
    String  gatewaySslciphers = "any";
    boolean gatewaySslRequireAuth = true;
    
    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_ENABLED_NAME, String.valueOf(gatewaySslenabled));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_PROTOCOLS_NAME, gatewaySslprotocols);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_CIPHERS_NAME, gatewaySslciphers);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(gatewaySslRequireAuth));
    
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_KEYSTORE_TYPE_NAME, "jks");
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_KEYSTORE_NAME, 
        TestUtil.getResourcePath(WANTestBase.class, "/com/gemstone/gemfire/cache/client/internal/client.keystore"));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_KEYSTORE_PASSWORD_NAME, "password");
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_TRUSTSTORE_NAME, 
        TestUtil.getResourcePath(WANTestBase.class, "/com/gemstone/gemfire/cache/client/internal/client.truststore"));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME, "password");
    
    gemFireProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    gemFireProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    
    getLogWriter().info("Starting cache ds with following properties \n" + gemFireProps);
    
    InternalDistributedSystem ds = test.getSystem(gemFireProps);
    cache = CacheFactory.create(ds);    
  }
  
  public static void createCache_PDX(Integer locPort){
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    CacheConfig cacheConfig = new CacheConfig();
    cacheConfig.setPdxPersistent(true);
    cacheConfig.setPdxDiskStore("PDX_TEST");
    cache = GemFireCacheImpl.create(ds, false, cacheConfig);
    
    File pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File [] dirs1 = new File[] {pdxDir};
    DiskStore store = dsf.setDiskDirs(dirs1).setMaxOplogSize(1).create("PDX_TEST");
  }
  
  public static void createCache(Integer locPort1, Integer locPort2){
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort1
        + "],localhost[" + locPort2 + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }
  
  public static void createCacheWithoutLocator(Integer mCastPort){
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, ""+mCastPort);
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }
  
  /**
   * Method that creates a bridge server
   * 
   * @return    Integer   Port on which the server is started.
   */
  public static Integer createCacheServer() {
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return new Integer(server1.getPort());
  }
  
  /**
   * Returns a Map that contains the count for number of bridge server and number
   * of Receivers.
   * 
   * @return    Map
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
  
  public static void startSender(String senderId) {
    final ExpectedException exln = addExpectedException("Could not connect");

    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(InterruptedException.class
        .getName());
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
  
  public static void enableConflation(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    sender.test_setBatchConflationEnabled(true);
  }
  
  public static void startAsyncEventQueue(String senderId) {
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueue q = null;
    for (AsyncEventQueue s : queues) {
      if (s.getId().equals(senderId)) {
        q = s;
        break;
      }
    }
    //merge42180: There is no start method on AsyncEventQueue. Cheetah has this method. Yet the code for AsyncEvnt Queue is not properly merged from cheetah to cedar
    //q.start();
  }
  
  /*
  public static Map getSenderToReceiverConnectionInfo(String senderId){	
	  Set<GatewaySender> senders = cache.getGatewaySenders();
	  GatewaySender sender = null;
	  for(GatewaySender s : senders){
		  if(s.getId().equals(senderId)){
			  sender = s;
			  break;
	      }
	  }
	  Map connectionInfo = null;
	  if (!sender.isParallel() && ((AbstractGatewaySender) sender).isPrimary()) {
		  connectionInfo = new HashMap();
		  GatewaySenderEventDispatcher dispatcher = 
			  ((AbstractGatewaySender)sender).getEventProcessor().getDispatcher();
		  if (dispatcher instanceof GatewaySenderEventRemoteDispatcher) {
			  ServerLocation serverLocation = 
				  ((GatewaySenderEventRemoteDispatcher) dispatcher).getConnection().getServer();
			  connectionInfo.put("serverHost", serverLocation.getHostName());
			  connectionInfo.put("serverPort", serverLocation.getPort());
			  
		  }
	  }
	  return connectionInfo;
  }
  */
  public static List<Integer> getSenderStats(String senderId, final int expectedQueueSize){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender).getStatistics();
    if (expectedQueueSize != -1) {
      final RegionQueue regionQueue;
      regionQueue = sender.getQueues().toArray(
          new RegionQueue[1])[0];
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          if (regionQueue.size() == expectedQueueSize) {
            return true;
          }
          return false;
        }

        public String description() {
          return "Expected queue entries: " + expectedQueueSize
              + " but actual entries: " + regionQueue.size();
        }
      };
      DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
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
    return stats;
  }
      
  public static void checkQueueStats(String senderId, final int queueSize,
      final int eventsReceived, final int eventsQueued,
      final int eventsDistributed) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender).getStatistics();
    assertEquals(queueSize, statistics.getEventQueueSize());
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsQueued, statistics.getEventsQueued());
    assert(statistics.getEventsDistributed() >= eventsDistributed);
  }

  public static void checkAsyncEventQueueStats(String queueId, final int queueSize,
      final int eventsReceived, final int eventsQueued,
      final int eventsDistributed) {
    Set<AsyncEventQueue> asyncQueues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : asyncQueues) {
      if (q.getId().equals(queueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl)queue).getStatistics();
    assertEquals(queueSize, statistics.getEventQueueSize()); 
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsQueued, statistics.getEventsQueued());
    assert(statistics.getEventsDistributed() >= eventsDistributed);
  }
  
  public static void checkGatewayReceiverStats(int processBatches,
      int eventsReceived, int creates) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = (GatewayReceiver)gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl)receiver.getServer())
        .getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats)stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertEquals(eventsReceived, gatewayReceiverStats.getEventsReceived());
    assertEquals(creates, gatewayReceiverStats.getCreateRequest());
  }

  public static void checkMinimumGatewayReceiverStats(int processBatches,
      int eventsReceived) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = (GatewayReceiver)gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl)receiver.getServer())
        .getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats)stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertTrue(gatewayReceiverStats.getEventsReceived()>= eventsReceived);
  }
  
  public static void checkExcepitonStats(int exceptionsOccured) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = (GatewayReceiver)gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl)receiver.getServer())
        .getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats)stats;
    if (exceptionsOccured == 0) {
      assertEquals(exceptionsOccured, gatewayReceiverStats
          .getExceptionsOccured());
    }
    else {
      assertTrue(gatewayReceiverStats.getExceptionsOccured() >= exceptionsOccured);
    }
  }

  public static void checkGatewayReceiverStatsHA(int processBatches,
      int eventsReceived, int creates) {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = (GatewayReceiver)gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl)receiver.getServer())
        .getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    GatewayReceiverStats gatewayReceiverStats = (GatewayReceiverStats)stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertTrue(gatewayReceiverStats.getEventsReceived() >= eventsReceived);
    assertTrue(gatewayReceiverStats.getCreateRequest() >= creates);
  }
  
  public static void checkEventFilteredStats(String senderId, final int eventsFiltered) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender).getStatistics();
    assertEquals(eventsFiltered, statistics.getEventsFiltered());
  }
  
  public static void checkConflatedStats(String senderId, final int eventsConflated) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender).getStatistics();
    assertEquals(eventsConflated, statistics.getEventsNotQueuedConflated());
  }
  
  public static void checkAsyncEventQueueConflatedStats(
      String asyncEventQueueId, final int eventsConflated) {
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : queues) {
      if (q.getId().equals(asyncEventQueueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl)queue)
        .getStatistics();
    assertEquals(eventsConflated, statistics.getEventsNotQueuedConflated());
  }
  
  public static void checkStats_Failover(String senderId,
      final int eventsReceived) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender)
        .getStatistics();

    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsReceived, (statistics.getEventsQueued()
        + statistics.getUnprocessedTokensAddedByPrimary() + statistics
        .getUnprocessedEventsRemovedByPrimary()));
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
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl) queue)
        .getStatistics();

    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsReceived, (statistics.getEventsQueued()
        + statistics.getUnprocessedTokensAddedByPrimary() + statistics
        .getUnprocessedEventsRemovedByPrimary()));
  }

  public static void checkBatchStats(String senderId, final int batches) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender)
        .getStatistics();
    assert (statistics.getBatchesDistributed() >= batches);
    assertEquals(0, statistics.getBatchesRedistributed());
  }
  
  public static void checkAsyncEventQueueBatchStats(String asyncQueueId,
      final int batches) {
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueue queue = null;
    for (AsyncEventQueue q : queues) {
      if (q.getId().equals(asyncQueueId)) {
        queue = q;
        break;
      }
    }
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl)queue)
        .getStatistics();
    assert (statistics.getBatchesDistributed() >= batches);
    assertEquals(0, statistics.getBatchesRedistributed());
  }

  public static void checkBatchStats(String senderId,
      final boolean batchesDistributed, final boolean bathcesRedistributed) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender)
        .getStatistics();
    assertEquals(batchesDistributed, (statistics.getBatchesDistributed() > 0));
    assertEquals(bathcesRedistributed,
        (statistics.getBatchesRedistributed() > 0));
  }

  public static void checkUnProcessedStats(String senderId, int events) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    final GatewaySenderStats statistics = ((AbstractGatewaySender)sender)
        .getStatistics();
    assertEquals(events,
        (statistics.getUnprocessedEventsAddedBySecondary() + statistics
            .getUnprocessedTokensRemovedBySecondary()));
    assertEquals(events,
        (statistics.getUnprocessedEventsRemovedByPrimary() + statistics
            .getUnprocessedTokensAddedByPrimary()));
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
    final AsyncEventQueueStats statistics = ((AsyncEventQueueImpl)queue).getStatistics();
    assertEquals(events,
        (statistics.getUnprocessedEventsAddedBySecondary() + statistics
            .getUnprocessedTokensRemovedBySecondary()));
    assertEquals(events,
        (statistics.getUnprocessedEventsRemovedByPrimary() + statistics
            .getUnprocessedTokensAddedByPrimary()));
  }
  
  
  public static void setRemoveFromQueueOnException(String senderId, boolean removeFromQueue){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    assertNotNull(sender);
    ((AbstractGatewaySender)sender).setRemoveFromQueueOnException(removeFromQueue);
  }
  
  public static void unsetRemoveFromQueueOnException(String senderId){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    assertNotNull(sender);
    ((AbstractGatewaySender)sender).setRemoveFromQueueOnException(false);
  }
  
  public static void waitForSenderRunningState(String senderId){
    final ExpectedException exln = addExpectedException("Could not connect");
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      final GatewaySender sender = getGatewaySenderById(senders, senderId);

      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          if (sender != null && sender.isRunning()) {
            return true;
          }
          return false;
        }

        public String description() {
          return "Expected sender isRunning state to be true but is false";
        }
      };
      DistributedTestCase.waitForCriterion(wc, 300000, 500, true);
    } finally {
      exln.remove();
    }
  }
  
  public static void waitForSenderToBecomePrimary(String senderId){
    Set<GatewaySender> senders = ((GemFireCacheImpl)cache).getAllGatewaySenders();
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
    DistributedTestCase.waitForCriterion(wc, 10000, 1000, true); 
  }
  
  private static GatewaySender getGatewaySenderById(Set<GatewaySender> senders, String senderId) {
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        return s;
      }
    }
    //if none of the senders matches with the supplied senderid, return null
    return null;
  }
  
  public static HashMap checkQueue(){
    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener1.createList);
    listenerAttrs.put("Update", listener1.updateList);
    listenerAttrs.put("Destroy", listener1.destroyList);
    return listenerAttrs;
  }
  
  public static void checkQueueOnSecondary (final Map primaryUpdatesMap){
    final HashMap secondaryUpdatesMap = new HashMap();
    secondaryUpdatesMap.put("Create", listener1.createList);
    secondaryUpdatesMap.put("Update", listener1.updateList);
    secondaryUpdatesMap.put("Destroy", listener1.destroyList);
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        secondaryUpdatesMap.put("Create", listener1.createList);
        secondaryUpdatesMap.put("Update", listener1.updateList);
        secondaryUpdatesMap.put("Destroy", listener1.destroyList);
        if (secondaryUpdatesMap.equals(primaryUpdatesMap)) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected seconadry map to be " + primaryUpdatesMap + " but it is " + secondaryUpdatesMap;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 300000, 500, true); 
  }
  
  public static HashMap checkQueue2(){
    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener2.createList);
    listenerAttrs.put("Update", listener2.updateList);
    listenerAttrs.put("Destroy", listener2.destroyList);
    return listenerAttrs;
  }
  
  public static HashMap checkPR(String regionName){
    PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);
    QueueListener listener = (QueueListener)region.getCacheListener();
    
    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener.createList);
    listenerAttrs.put("Update", listener.updateList);
    listenerAttrs.put("Destroy", listener.destroyList);
    return listenerAttrs;
  }
  
  public static HashMap checkBR(String regionName, int numBuckets){
    PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);
    HashMap listenerAttrs = new HashMap();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      QueueListener listener = (QueueListener)br.getCacheListener();
      listenerAttrs.put("Create"+i, listener.createList);
      listenerAttrs.put("Update"+i, listener.updateList);
      listenerAttrs.put("Destroy"+i, listener.destroyList);
    }
    return listenerAttrs;
  }
  
  public static HashMap checkQueue_PR(String senderId){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    
    RegionQueue parallelQueue = (RegionQueue)((AbstractGatewaySender)sender)
    .getQueues().toArray(new RegionQueue[1])[0];
    
    PartitionedRegion region = (PartitionedRegion)parallelQueue.getRegion();
    QueueListener listener = (QueueListener)region.getCacheListener();
    
    HashMap listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener.createList);
    listenerAttrs.put("Update", listener.updateList);
    listenerAttrs.put("Destroy", listener.destroyList);
    return listenerAttrs;
  }
  
  public static HashMap checkQueue_BR(String senderId, int numBuckets){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    RegionQueue parallelQueue = (RegionQueue)((AbstractGatewaySender)sender)
    .getQueues().toArray(new RegionQueue[1])[0];
    
    PartitionedRegion region = (PartitionedRegion)parallelQueue.getRegion();
    HashMap listenerAttrs = new HashMap();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      if (br != null) {
        QueueListener listener = (QueueListener)br.getCacheListener();
        if (listener != null) {
          listenerAttrs.put("Create"+i, listener.createList);
          listenerAttrs.put("Update"+i, listener.updateList);
          listenerAttrs.put("Destroy"+i, listener.destroyList);
        }
      }
    }
    return listenerAttrs;
  }

  public static void addListenerOnBucketRegion(String regionName, int numBuckets) {
    WANTestBase test = new WANTestBase(testName);
    test.addCacheListenerOnBucketRegion(regionName, numBuckets);
  }
  
  private void addCacheListenerOnBucketRegion(String regionName, int numBuckets){
    PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      AttributesMutator mutator = br.getAttributesMutator();
      listener1 = new QueueListener();
      mutator.addCacheListener(listener1);
    }
  }
  
  public static void addListenerOnQueueBucketRegion(String senderId, int numBuckets) {
    WANTestBase test = new WANTestBase(testName);
    test.addCacheListenerOnQueueBucketRegion(senderId, numBuckets);
  }
  
  private void addCacheListenerOnQueueBucketRegion(String senderId, int numBuckets){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    RegionQueue parallelQueue = (RegionQueue)((AbstractGatewaySender)sender)
    .getQueues().toArray(new RegionQueue[1])[0];
    
    PartitionedRegion region = (PartitionedRegion)parallelQueue.getRegion();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion br = region.getBucketRegion(i);
      if (br != null) {
        AttributesMutator mutator = br.getAttributesMutator();
        CacheListener listener = new QueueListener();
        mutator.addCacheListener(listener);
      }
    }
    
  }
  
  public static void addQueueListener(String senderId, boolean isParallel){
    WANTestBase test = new WANTestBase(testName);
    test.addCacheQueueListener(senderId, isParallel);
  }
  
  public static void addSecondQueueListener(String senderId, boolean isParallel){
    WANTestBase test = new WANTestBase(testName);
    test.addSecondCacheQueueListener(senderId, isParallel);
  }
  
  public static void addListenerOnRegion(String regionName){
    WANTestBase test = new WANTestBase(testName);
    test.addCacheListenerOnRegion(regionName);
  }
  private void addCacheListenerOnRegion(String regionName){
    Region region = cache.getRegion(regionName);
    AttributesMutator mutator = region.getAttributesMutator();
    listener1 = new QueueListener();
    mutator.addCacheListener(listener1);
  }
  
  private void addCacheQueueListener(String senderId, boolean isParallel) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    listener1 = new QueueListener();
    if (!isParallel) {
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      for(RegionQueue q: queues) {
        q.addCacheListener(listener1);
      }
    }
    else {
      RegionQueue parallelQueue = (RegionQueue)((AbstractGatewaySender)sender)
      .getQueues().toArray(new RegionQueue[1])[0];
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
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      for(RegionQueue q: queues) {
        q.addCacheListener(listener2);
      }
    }
    else {
    	RegionQueue parallelQueue = (RegionQueue)((AbstractGatewaySender)sender)
      .getQueues().toArray(new RegionQueue[1])[0];
      parallelQueue.addCacheListener(listener2);
    }
  }
  
  public static void pauseSender(String senderId) {
    final ExpectedException exln = addExpectedException("Could not connect");
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
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
    }
    finally {
      exp.remove();
      exln.remove();
    }
  }
      
  public static void pauseSenderAndWaitForDispatcherToPause(String senderId) {
    final ExpectedException exln = addExpectedException("Could not connect");
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
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
      ((AbstractGatewaySender)sender).getEventProcessor().waitForDispatcherToPause();
    } finally {
      exp.remove();
      exln.remove();
    }    
  }
  
  public static void resumeSender(String senderId) {
    final ExpectedException exln = addExpectedException("Could not connect");
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
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
    }
    finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void stopSender(String senderId) {
    final ExpectedException exln = addExpectedException("Could not connect");
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.stop();
    }
    finally {
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
  
  public static void createSender(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManulaStart) {
    final ExpectedException exln = addExpectedException("Could not connect");
    try {
      File persistentDirectory = new File(dsName + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] { persistentDirectory };
      if (isParallel) {
        InternalGatewaySenderFactory gateway = (InternalGatewaySenderFactory)cache.createGatewaySenderFactory();
        gateway.setParallel(true);
        gateway.setMaximumQueueMemory(maxMemory);
        gateway.setBatchSize(batchSize);
        gateway.setManualStart(isManulaStart);
        //set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (filter != null) {
          eventFilter = filter;
          gateway.addGatewayEventFilter(filter);
        }
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
              .getName());
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
        //set dispatcher threads
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
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
              .getName());
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
  
  public static void createSenderWithMultipleDispatchers(String dsName, int remoteDsId,
	boolean isParallel, Integer maxMemory,
	Integer batchSize, boolean isConflation, boolean isPersistent,
	GatewayEventFilter filter, boolean isManulaStart, int numDispatchers, OrderPolicy orderPolicy) {
	  final ExpectedException exln = addExpectedException("Could not connect");
	  try {
		File persistentDirectory = new File(dsName + "_disk_"
			+ System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
		persistentDirectory.mkdir();
		DiskStoreFactory dsf = cache.createDiskStoreFactory();
		File[] dirs1 = new File[] { persistentDirectory };
		if (isParallel) {
		  GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
	      gateway.setParallel(true);
	      gateway.setMaximumQueueMemory(maxMemory);
	      gateway.setBatchSize(batchSize);
	      gateway.setManualStart(isManulaStart);
	      ((InternalGatewaySenderFactory) gateway)
	      .setLocatorDiscoveryCallback(new MyLocatorCallback());
	      if (filter != null) {
	    	eventFilter = filter;
	        gateway.addGatewayEventFilter(filter);
	      }
	      if (isPersistent) {
	    	gateway.setPersistenceEnabled(true);
	        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
	        	.getName());
	      } else {
	    	DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
	    	gateway.setDiskStoreName(store.getName());
	      }
	      gateway.setBatchConflationEnabled(isConflation);
	      gateway.setDispatcherThreads(numDispatchers);
	      gateway.setOrderPolicy(orderPolicy);
	      gateway.create(dsName, remoteDsId);

		} else {
		  GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
		  gateway.setMaximumQueueMemory(maxMemory);
		  gateway.setBatchSize(batchSize);
		  gateway.setManualStart(isManulaStart);
		  ((InternalGatewaySenderFactory) gateway)
		  .setLocatorDiscoveryCallback(new MyLocatorCallback());
		  if (filter != null) {
			eventFilter = filter;
			gateway.addGatewayEventFilter(filter);
		  }
		  gateway.setBatchConflationEnabled(isConflation);
		  if (isPersistent) {
			gateway.setPersistenceEnabled(true);
			gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
				.getName());
		  } else {
			DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
			gateway.setDiskStoreName(store.getName());
		  }
		  gateway.setDispatcherThreads(numDispatchers);
		  gateway.setOrderPolicy(orderPolicy);
		  gateway.create(dsName, remoteDsId);
		}
	  } finally {
		exln.remove();
	  }
  }
  
  public static void createSenderWithoutDiskStore(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManulaStart) {
    
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      //set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);
  }
  
  public static void createConcurrentSender(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize,
      boolean isConflation, boolean isPersistent, GatewayEventFilter filter,
      boolean isManualStart, int concurrencyLevel, OrderPolicy policy) {

    File persistentDirectory = new File(dsName + "_disk_"
        + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] { persistentDirectory };

    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.setDispatcherThreads(concurrencyLevel);
      gateway.setOrderPolicy(policy);
      gateway.create(dsName, remoteDsId);
    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setDispatcherThreads(concurrencyLevel);
      gateway.setOrderPolicy(policy);
      gateway.create(dsName, remoteDsId);
    }
  }
  
//  public static void createSender_PDX(String dsName, int remoteDsId,
//      boolean isParallel, Integer maxMemory,
//      Integer batchSize, boolean isConflation, boolean isPersistent,
//      GatewayEventFilter filter, boolean isManulaStart) {
//    File persistentDirectory = new File(dsName +"_disk_"+System.currentTimeMillis()+"_" + VM.getCurrentVMNum());
//    persistentDirectory.mkdir();
//    
//    File [] dirs1 = new File[] {persistentDirectory};
//    
//    if(isParallel) {
//      ParallelGatewaySenderFactory gateway = cache.createParallelGatewaySenderFactory();
//      gateway.setMaximumQueueMemory(maxMemory);
//      gateway.setBatchSize(batchSize);
//      ((ParallelGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
//      if (filter != null) {
//        gateway.addGatewayEventFilter(filter);
//      }
//      if(isPersistent) {
//        gateway.setPersistenceEnabled(true);
//        DiskStoreFactory dsf = cache.createDiskStoreFactory();
//        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
//      }
//      gateway.setBatchConflationEnabled(isConflation);
//      gateway.create(dsName, remoteDsId);
//      
//    }else {
//      SerialGatewaySenderFactory gateway = cache.createSerialGatewaySenderFactory();
//      gateway.setMaximumQueueMemory(maxMemory);
//      gateway.setBatchSize(batchSize);
//      gateway.setManualStart(isManulaStart);
//      ((SerialGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
//      if (filter != null) {
//        gateway.addGatewayEventFilter(filter);
//      }
//      gateway.setBatchConflationEnabled(isConflation);
//      if(isPersistent) {
//        gateway.setPersistenceEnabled(true);
//        DiskStoreFactory dsf = cache.createDiskStoreFactory();
//        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
//      }
//      gateway.create(dsName, remoteDsId);
//    }
//  }
  public static void createSenderForValidations(String dsName, int remoteDsId,
      boolean isParallel, Integer alertThreshold,
      boolean isConflation, boolean isPersistent,
      List<GatewayEventFilter> eventfilters,
      List<GatewayTransportFilter> tranportFilters, boolean isManulaStart,
      boolean isDiskSync) {
    ExpectedException exp1 = addExpectedException(RegionDestroyedException.class
        .getName());
    try {
      File persistentDirectory = new File(dsName + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] { persistentDirectory };

      if (isParallel) {
        GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
        gateway.setParallel(true);
        gateway.setAlertThreshold(alertThreshold);
        ((InternalGatewaySenderFactory)gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (eventfilters != null) {
          for (GatewayEventFilter filter : eventfilters) {
            gateway.addGatewayEventFilter(filter);
          }
        }
        if (tranportFilters != null) {
          for (GatewayTransportFilter filter : tranportFilters) {
            gateway.addGatewayTransportFilter(filter);
          }
        }
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1)
              .create(dsName + "_Parallel").getName());
        }
        else {
          DiskStore store = dsf.setDiskDirs(dirs1).create(dsName + "_Parallel");
          gateway.setDiskStoreName(store.getName());
        }
        gateway.setDiskSynchronous(isDiskSync);
        gateway.setBatchConflationEnabled(isConflation);
        gateway.setManualStart(isManulaStart);
        //set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        gateway.create(dsName, remoteDsId);

      }
      else {
        GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
        gateway.setAlertThreshold(alertThreshold);
        gateway.setManualStart(isManulaStart);
        //set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        ((InternalGatewaySenderFactory)gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (eventfilters != null) {
          for (GatewayEventFilter filter : eventfilters) {
            gateway.addGatewayEventFilter(filter);
          }
        }
        if (tranportFilters != null) {
          for (GatewayTransportFilter filter : tranportFilters) {
            gateway.addGatewayTransportFilter(filter);
          }
        }
        gateway.setBatchConflationEnabled(isConflation);
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1)
              .create(dsName + "_Serial").getName());
        }
        else {
          DiskStore store = dsf.setDiskDirs(dirs1).create(dsName + "_Serial");
          gateway.setDiskStoreName(store.getName());
        }
        gateway.setDiskSynchronous(isDiskSync);
        GatewaySender sender = gateway
            .create(dsName, remoteDsId);
      }
    }
    finally {
      exp1.remove();
    }
  }
  
  public static String createSenderWithDiskStore(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, String dsStore, boolean isManualStart) {
    File persistentDirectory = null;
    if (dsStore == null) {
      persistentDirectory = new File(dsName + "_disk_"
          + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    }
    else {
      persistentDirectory = new File(dsStore);  
    }
    getLogWriter().info("The ds is : " + persistentDirectory.getName());
    
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File [] dirs1 = new File[] {persistentDirectory};
    
    if(isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      //set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if(isPersistent) {
        gateway.setPersistenceEnabled(true);
        String dsname = dsf.setDiskDirs(dirs1).create(dsName).getName();
        gateway.setDiskStoreName(dsname);
        getLogWriter().info("The DiskStoreName is : " + dsname);
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
        getLogWriter().info("The ds is : " + store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);
      
    }else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      //set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if(isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
    return persistentDirectory.getName();
  }
  
  
  public static void createSenderWithListener(String dsName, int remoteDsName,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean attachTwoListeners, boolean isManulaStart) {
    File persistentDirectory = new File(dsName + "_disk_"
        + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] { persistentDirectory };

    if (isParallel) {
      GatewaySenderFactory gateway = cache
          .createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      //set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory)gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsName);

    } else {
      GatewaySenderFactory gateway = cache
          .createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      //set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory)gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }

      eventListener1 = new MyGatewaySenderEventListener();
      ((InternalGatewaySenderFactory)gateway).addAsyncEventListener(eventListener1);
      if (attachTwoListeners) {
        eventListener2 = new MyGatewaySenderEventListener2();
        ((InternalGatewaySenderFactory)gateway).addAsyncEventListener(eventListener2);
      }
      ((InternalGatewaySenderFactory)gateway).create(dsName);
    }
  }
  
  public static void pauseWaitCriteria(final long millisec) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return false;
      }

      public String description() {
        return "Expected to wait for " + millisec + " millisec.";
      }
    };
    DistributedTestCase.waitForCriterion(wc, millisec, 500, false); 
  }
  
  public static int createReceiver(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");

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
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }
  
  public static void createReceiverWithBindAddress(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);    
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    fact.setBindAddress("200.112.204.10");
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
      fail("Expected GatewayReciever Exception");
    }
    catch (GatewayReceiverException gRE){
      getLogWriter().fine("KBKBKB : got the GatewayReceiverException", gRE);
      assertTrue(gRE.getMessage().contains("Failed to create server socket on"));
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
  }
  public static int createReceiverWithSSL(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    boolean gatewaySslenabled = true;
    String  gatewaySslprotocols = "any";
    String  gatewaySslciphers = "any";
    boolean gatewaySslRequireAuth = true;
    
    Properties gemFireProps = new Properties();

    gemFireProps.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_ENABLED_NAME, String.valueOf(gatewaySslenabled));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_PROTOCOLS_NAME, gatewaySslprotocols);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_CIPHERS_NAME, gatewaySslciphers);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(gatewaySslRequireAuth));
    
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_KEYSTORE_TYPE_NAME, "jks");
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_KEYSTORE_NAME, 
        TestUtil.getResourcePath(WANTestBase.class, "/com/gemstone/gemfire/cache/client/internal/cacheserver.keystore"));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_KEYSTORE_PASSWORD_NAME, "password");
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_TRUSTSTORE_NAME, 
        TestUtil.getResourcePath(WANTestBase.class, "/com/gemstone/gemfire/cache/client/internal/cacheserver.truststore"));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME, "password");
    
    gemFireProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    gemFireProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");

    getLogWriter().info("Starting cache ds with following properties \n" + gemFireProps);
    
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
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }
  
  public static String makePath(String[] strings) {
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<strings.length;i++){
      sb.append(strings[i]);      
      sb.append(File.separator);
    }
    return sb.toString();
  }
  
  public static int createReceiverAfterCache(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }
  
  public static void createReceiverAndServer(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");

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
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + receiverPort);
    }
    CacheServer server = cache.addCacheServer();
    int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(serverPort);
    server.setHostnameForClients("localhost");
    //server.setGroups(new String[]{"serv"});
    try {
      server.start();
    } catch (IOException e) {
      fail("Failed to start server ", e);
    }
  }
  
  public static int createReceiverInSecuredCache(int locPort) {
	GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
	int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
	fact.setStartPort(port);
	fact.setEndPort(port);
	fact.setManualStart(true);
	GatewayReceiver receiver = fact.create();
	try {
          receiver.start();
        }
        catch (IOException e) {
          e.printStackTrace();
          fail("Failed to start GatewayRecevier on port " + port, e);
        }
	return port;
  }
  
  public static int createServer(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);    

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setHostnameForClients("localhost");
    //server.setGroups(new String[]{"serv"});
    try {
      server.start();
    } catch (IOException e) {
      fail("Failed to start server ", e);
    }
    return port;
  }
  
  public static void createClientWithLocator(int port0,String host, 
      String regionName) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    
    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0) //.setServerGroup("serv")
          .setPingInterval(250).setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1).setReadTimeout(2000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10)
          .setRetryAttempts(3).create(regionName);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    AttributesFactory factory = new AttributesFactory();
    factory.setPoolName(p.getName());
    factory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes attrs = factory.create();
    region = cache.createRegion(regionName, attrs);
    region.registerInterest("ALL_KEYS");
    assertNotNull(region);
    getLogWriter().info(
        "Distributed Region " + regionName + " created Successfully :"
            + region.toString());
  }
  
  public static int createReceiver_PDX(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    CacheConfig cacheConfig = new CacheConfig();
    File pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    cacheConfig.setPdxPersistent(true);
    cacheConfig.setPdxDiskStore("pdxStore");
    cache = GemFireCacheImpl.create(ds, false, cacheConfig);
    cache.createDiskStoreFactory()
      .setDiskDirs(new File[] { pdxDir})
      .setMaxOplogSize(1)
      .create("pdxStore");
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }
  
  public static void createReceiver2(int locPort) {
    WANTestBase test = new WANTestBase(testName);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
    }
  }

  public static void doDistTXPuts(String regionName, int numPuts) {
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
    txMgr.setDistributed(true);
    
    ExpectedException exp1 = addExpectedException(InterruptedException.class
        .getName());
    ExpectedException exp2 = addExpectedException(GatewaySenderException.class
        .getName());
    try {
      Region r = cache.getRegion(Region.SEPARATOR + regionName);
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
//    for (long i = 0; i < numPuts; i++) {
//      r.destroy(i);
//    }
  }
  
  public static void doPuts(String regionName, int numPuts) {
    ExpectedException exp1 = addExpectedException(InterruptedException.class
        .getName());
    ExpectedException exp2 = addExpectedException(GatewaySenderException.class
        .getName());
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
//    for (long i = 0; i < numPuts; i++) {
//      r.destroy(i);
//    }
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
  
  public static void doPutsAfter300(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 300; i < numPuts; i++) {
      r.put(i, i);
    }
  }
  
  public static void doPutsFrom(String regionName, int from, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = from; i < numPuts; i++) {
      r.put(i, i);
    }
  }

  public static void doDestroys(String regionName, int keyNum) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < keyNum; i++) {
      r.destroy(i);
    }
  }
  
  public static void doPutAll(String regionName, int numPuts, int size) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numPuts; i++) {
      Map putAllMap = new HashMap();
      for(long j =0; j< size; j++) {
        putAllMap.put((size* i)+j,i);
      }
      r.putAll(putAllMap, "putAllCallback");
      putAllMap.clear();
    }
  }
  
  
  public static void doPutsWithKeyAsString(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < numPuts; i++) {
      r.put("Object_" + i, i);
    }
  }
  
  public static void putGivenKeyValue(String regionName, Map keyValues) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (Object key: keyValues.keySet()) {
      r.put(key, keyValues.get(key));
    }
  }
  
  public static void destroyRegion(String regionName) {
    destroyRegion(regionName, -1);
  }
  
  public static void destroyRegion(String regionName, final int min) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.size() > min) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Looking for min size of region to be " + min;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 30000, 5, false); 
    r.destroyRegion();
  }

  public static void destroyRegionAfterMinRegionSize(String regionName, final int min) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (destroyFlag) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Looking for min size of region to be " + min;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 30000, 5, false); 
    r.destroyRegion();
    destroyFlag = false;
  }
  
  public static void localDestroyRegion(String regionName) {
    ExpectedException exp = addExpectedException(PRLocallyDestroyedException.class
        .getName());
    try {
      Region r = cache.getRegion(Region.SEPARATOR + regionName);
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

  protected static Map putCustomerPartitionedRegion(int numPuts,
      String valueSuffix) {
    assertNotNull(cache);
    assertNotNull(customerRegion);
    Map custKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i + valueSuffix);
      try {
        customerRegion.put(custid, customer);
        assertTrue(customerRegion.containsKey(custid));
        assertEquals(customer,customerRegion.get(custid));
        custKeyValues.put(custid, customer);
      }
      catch (Exception e) {
        fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      getLogWriter().info("Customer :- { " + custid + " : " + customer + " }");
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
      Order order = new Order("OREDR" + oid);
      try {
        orderRegion.put(orderId, order);
        orderKeyValues.put(orderId, order);
        assertTrue(orderRegion.containsKey(orderId));
        assertEquals(order,orderRegion.get(orderId));

      }
      catch (Exception e) {
        fail(
            "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
    }
    return orderKeyValues;
  }
  
  public static Map putOrderPartitionedRegionUsingCustId(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Order order = new Order("OREDR" + i);
      try {
        orderRegion.put(custid, order);
        orderKeyValues.put(custid, order);
        assertTrue(orderRegion.containsKey(custid));
        assertEquals(order, orderRegion.get(custid));

      } catch (Exception e) {
        fail(
            "putOrderPartitionedRegionUsingCustId : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      getLogWriter().info("Order :- { " + custid + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map updateOrderPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 1; j++) {
        int oid = (i * 1) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid + "_update");
        try {
          orderRegion.put(orderId, order);
          orderKeyValues.put(orderId, order);
          assertTrue(orderRegion.containsKey(orderId));
          assertEquals(order,orderRegion.get(orderId));

        }
        catch (Exception e) {
          fail(
              "updateOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
    return orderKeyValues;
  }
  
  public static Map updateOrderPartitionedRegionUsingCustId(int numPuts) {
    assertNotNull(cache);
    assertNotNull(orderRegion);
    Map orderKeyValues = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      Order order =new Order("OREDR" + i + "_update");
      try {
        orderRegion.put(custid, order);
        assertTrue(orderRegion.containsKey(custid));
        assertEquals(order, orderRegion.get(custid));
        orderKeyValues.put(custid, order);
      } catch (Exception e) {
        fail(
            "updateOrderPartitionedRegionUsingCustId : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      getLogWriter().info("Order :- { " + custid + " : " + order + " }");
    }
    return orderKeyValues;
  }
  
  public static Map putShipmentPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(shipmentRegion);
    Map shipmentKeyValue = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 1; j++) {
        int oid = (i * 1) + j;
        OrderId orderId = new OrderId(oid, custid);
        for (int k = 1; k <= 1; k++) {
          int sid = (oid * 1) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          try {
            shipmentRegion.put(shipmentId, shipment);
            assertTrue(shipmentRegion.containsKey(shipmentId));
            assertEquals(shipment,shipmentRegion.get(shipmentId));
            shipmentKeyValue.put(shipmentId, shipment);
          }
          catch (Exception e) {
            fail(
                "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                e);
          }
          getLogWriter().info(
              "Shipment :- { " + shipmentId + " : " + shipment + " }");
        }
      }
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
      for (int j = 1; j <= 1; j++) {
        int oid = (i * 1) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("Order"+orderId);
        orderRegion.put(orderId, order);
        for (int k = 1; k <= 1; k++) {
          int sid = (oid * 1) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          shipmentRegion.put(shipmentId, shipment);
        }
      }
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
        fail(
            "putShipmentPartitionedRegionUsingCustId : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      getLogWriter().info("Shipment :- { " + custid + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }
  
  public static Map updateShipmentPartitionedRegion(int numPuts) {
    assertNotNull(cache);
    assertNotNull(shipmentRegion);
    Map shipmentKeyValue = new HashMap();
    for (int i = 1; i <= numPuts; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 1; j++) {
        int oid = (i * 1) + j;
        OrderId orderId = new OrderId(oid, custid);
        for (int k = 1; k <= 1; k++) {
          int sid = (oid * 1) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid + "_update");
          try {
            shipmentRegion.put(shipmentId, shipment);
            assertTrue(shipmentRegion.containsKey(shipmentId));
            assertEquals(shipment,shipmentRegion.get(shipmentId));
            shipmentKeyValue.put(shipmentId, shipment);
          }
          catch (Exception e) {
            fail(
                "updateShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                e);
          }
          getLogWriter().info(
              "Shipment :- { " + shipmentId + " : " + shipment + " }");
        }
      }
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
        fail(
            "updateShipmentPartitionedRegionUsingCustId : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      getLogWriter().info("Shipment :- { " + custid + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static void doPutsPDXSerializable(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (int i = 0; i < numPuts; i++) {
      r.put("Key_" + i, new SimpleClass(i, (byte)i));
    }
  }
  
  public static void doPutsPDXSerializable2(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (int i = 0; i < numPuts; i++) {
      r.put("Key_" + i, new SimpleClass1(false, (short) i, "" + i, i,"" +i ,""+ i,i, i));
    }
  }
  
  
  public static void doTxPuts(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    CacheTransactionManager mgr = cache.getCacheTransactionManager();

    mgr.begin();
    r.put(0, 0);
    r.put(100, 100);
    r.put(200, 200);
    mgr.commit();
  }
    
  public static void doNextPuts(String regionName, int start, int numPuts) {
    //waitForSitesToUpdate();
    ExpectedException exp = addExpectedException(CacheClosedException.class
        .getName());
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
  
  public static void checkQueueSize(String senderId, int numQueueEntries) {
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> testQueueSize(senderId, numQueueEntries));
  }
  
  public static void testQueueSize(String senderId, int numQueueEntries) {
    GatewaySender sender = null;
    for (GatewaySender s : cache.getGatewaySenders()) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    if (sender.isParallel()) {
      int totalSize = 0;
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      for( RegionQueue q : queues) {
    	  ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue)q;
        totalSize += prQ.size();
      }
      assertEquals(numQueueEntries,totalSize);
    } else {
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }
  
  /**
   * To be used only for ParallelGatewaySender.
   * @param senderId    Id of the ParallelGatewaySender
   * @param numQueueEntries     Expected number of ParallelGatewaySenderQueue entries
   */
  public static void checkPRQLocalSize(String senderId,
      final int numQueueEntries) {
    GatewaySender sender = null;
    for (GatewaySender s : cache.getGatewaySenders()) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (sender.isParallel()) {
      int totalSize = 0;
      final Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      
      WaitCriterion wc = new WaitCriterion() {
        int size = 0;
        public boolean done() {
          for (RegionQueue q : queues) {
            ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue)q;
            size += prQ.localSize();
          }
          if (size == numQueueEntries) {
            return true;
          }
          return false;
        }

        public String description() {
          return " Expected local queue entries: " + numQueueEntries
            + " but actual entries: " + size;
        }
        
      };
      
      DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
    }
  }
  
  /**
   * To be used only for ParallelGatewaySender.
   * @param senderId    Id of the ParallelGatewaySender
   * @param numQueueEntries     Expected number of ParallelGatewaySenderQueue entries
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
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueues();
      for( RegionQueue q : queues) {
        ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue)q;
        totalSize += prQ.localSize();
      }
      return totalSize;
    }
    return -1;
  }
  
  public static void doUpdates(String regionName, int numUpdates) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (int i = 0; i < numUpdates; i++) {
      String s = "K"+i; 
      r.put(i, s);
    }
  }

  public static void doUpdateOnSameKey(String regionName, int key,
      int numUpdates) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (int i = 0; i < numUpdates; i++) {
      String s = "V_" + i;
      r.put(key, s);
    }
  }
  
  public static void doRandomUpdates(String regionName, int numUpdates) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    Set<Integer> generatedKeys = new HashSet<Integer>();
    while(generatedKeys.size() != numUpdates) {
      generatedKeys.add((new Random()).nextInt(r.size()));
    }
    for (Integer i: generatedKeys) {
      String s = "K"+i; 
      r.put(i, s);
    }
  }
  
  public static void doMultiThreadedPuts(String regionName, int numPuts) {
    final AtomicInteger ai = new AtomicInteger(-1);
    final ExecutorService execService = Executors.newFixedThreadPool(5,
        new ThreadFactory() {
          AtomicInteger threadNum = new AtomicInteger();

          public Thread newThread(final Runnable r) {
            Thread result = new Thread(r, "Client Put Thread-"
                + threadNum.incrementAndGet());
            result.setDaemon(true);
            return result;
          }
        });

    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);

    List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
    for (long i = 0; i < 5; i++)
      tasks.add(new PutTask(r, ai, numPuts));

    try {
      List<Future<Object>> l = execService.invokeAll(tasks);
      for (Future<Object> f : l)
        f.get();
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    execService.shutdown();
  }

  public static void validateRegionSize(String regionName, final int regionSize) {
    ExpectedException exp = addExpectedException(ForceReattemptException.class
        .getName());
    ExpectedException exp1 = addExpectedException(CacheClosedException.class
        .getName());
    try {

      final Region r = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(r);
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          if (r.keySet().size() == regionSize) {
            return true;
          }
          return false;
        }

        public String description() {
          return "Expected region entries: " + regionSize
              + " but actual entries: " + r.keySet().size()
              + " present region keyset " + r.keySet();
        }
      };
      DistributedTestCase.waitForCriterion(wc, 240000, 500, true);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }
  
  /**
   * Validate whether all the attributes set on AsyncEventQueueFactory are set
   * on the sender underneath the AsyncEventQueue.
   */
  public static void validateAsyncEventQueueAttributes(String asyncChannelId,
      int maxQueueMemory, int batchSize, int batchTimeInterval,
      boolean isPersistent, String diskStoreName, boolean isDiskSynchronous,
      boolean batchConflationEnabled) {

    AsyncEventQueue theChannel = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncChannelId.equals(asyncChannel.getId())) {
        theChannel = asyncChannel;
      }
    }

    GatewaySender theSender = ((AsyncEventQueueImpl)theChannel).getSender();
    assertEquals("maxQueueMemory", maxQueueMemory, theSender
        .getMaximumQueueMemory());
    assertEquals("batchSize", batchSize, theSender.getBatchSize());
    assertEquals("batchTimeInterval", batchTimeInterval, theSender
        .getBatchTimeInterval());
    assertEquals("isPersistent", isPersistent, theSender.isPersistenceEnabled());
    assertEquals("diskStoreName", diskStoreName, theSender.getDiskStoreName());
    assertEquals("isDiskSynchronous", isDiskSynchronous, theSender
        .isDiskSynchronous());
    assertEquals("batchConflation", batchConflationEnabled, theSender
        .isBatchConflationEnabled());
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
        return "Expected map entries: " + expectedSize
            + " but actual entries: " + eventsMap.size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60000, 500, true); //TODO:Yogs 
  }
  
   public static void validateCustomAsyncEventListener(String asyncQueueId,
      final int expectedSize) {
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
        return "Expected map entries: " + expectedSize
            + " but actual entries: " + eventsMap.size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60000, 500, true); // TODO:Yogs
    
   Iterator<AsyncEvent> itr = eventsMap.values().iterator();
   while (itr.hasNext()) {
     AsyncEvent event = itr.next();
     assertTrue("possibleDuplicate should be true for event: " + event, event.getPossibleDuplicate());
   }
  }
  
  public static void waitForAsyncQueueToGetEmpty(String asyncQueueId) {
    AsyncEventQueue theAsyncEventQueue = null;

    Set<AsyncEventQueue> asyncEventChannels = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theAsyncEventQueue = asyncChannel;
      }
    }

    final GatewaySender sender = ((AsyncEventQueueImpl)theAsyncEventQueue)
        .getSender();

    if (sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender)sender)
          .getQueues();

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
          return "Expected queue size to be : " + 0 + " but actual entries: "
              + size;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60000, 500, true);

    } else {
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          Set<RegionQueue> queues = ((AbstractGatewaySender)sender)
              .getQueues();
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
          Set<RegionQueue> queues = ((AbstractGatewaySender)sender)
              .getQueues();
          int size = 0;
          for (RegionQueue q : queues) {
            size += q.size();
          }
          return "Expected queue size to be : " + 0 + " but actual entries: "
              + size;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60000, 500, true);
    }
  }
  
  public static void verifyAsyncEventListenerForPossibleDuplicates(
      String asyncEventQueueId, Set<Integer> bucketIds, int batchSize) {
    AsyncEventListener theListener = null;

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();
    for (AsyncEventQueue asyncQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final Map<Integer, List<GatewaySenderEventImpl>> bucketToEventsMap = ((MyAsyncEventListener2)theListener)
        .getBucketToEventsMap();
    assertNotNull(bucketToEventsMap);
    assertTrue(bucketIds.size() > 1);

    for (int bucketId : bucketIds) {
      List<GatewaySenderEventImpl> eventsForBucket = bucketToEventsMap
          .get(bucketId);
      getLogWriter().info(
          "Events for bucket: " + bucketId + " is " + eventsForBucket);
      assertNotNull(eventsForBucket);
      for (int i = 0; i < batchSize; i++) {
        GatewaySenderEventImpl senderEvent = eventsForBucket.get(i);
        assertTrue(senderEvent.getPossibleDuplicate());
      }
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

    final Map eventsMap = ((MyAsyncEventListener)theListener).getEventsMap();
    assertNotNull(eventsMap);
    getLogWriter().info("The events map size is " + eventsMap.size());
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

  
  public static void validateRegionSize_PDX(String regionName, final int regionSize) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.keySet().size() >= regionSize) {
          return true;
        }
        return false;
      }

      public String description() {
        
        return "Expected region entries: " + regionSize + " but actual entries: " + r.keySet().size() + " present region keyset " + r.keySet()  ;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 200000, 500, true); 
    for(int i = 0 ; i < regionSize; i++){
      getLogWriter().info("For Key : Key_"+i + " : Values : " + r.get("Key_" + i));
      assertEquals(new SimpleClass(i, (byte)i), r.get("Key_" + i));
    }
  }
  public static void validateRegionSize_PDX2(String regionName, final int regionSize) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.keySet().size() == regionSize) {
          return true;
        }
        return false;
      }

      public String description() {
        
        return "Expected region entries: " + regionSize + " but actual entries: " + r.keySet().size() + " present region keyset " + r.keySet()  ;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 200000, 500, true); 
    for(int i = 0 ; i < regionSize; i++){
      getLogWriter().info("For Key : Key_"+i + " : Values : " + r.get("Key_" + i));
      assertEquals(new SimpleClass1(false, (short) i, "" + i, i,"" +i ,""+ i,i, i), r.get("Key_" + i));
    }
  }
  
  public static void validateQueueSizeStat(String id, final int queueSize) {
    final AbstractGatewaySender sender = (AbstractGatewaySender)  cache.getGatewaySender(id);
    
    waitForCriterion(new WaitCriterion() {
      
      @Override
      public boolean done() {
        return sender.getEventQueueSize() == queueSize;
      }
      
      @Override
      public String description() {
        // TODO Auto-generated method stub
        return null;
      }
    }, 30000, 50, false);
    assertEquals(queueSize, sender.getEventQueueSize());
  }
  /**
   * This method is specifically written for pause and stop operations.
   * This method validates that the region size remains same for at least minimum number of verification
   * attempts and also it remains below a specified limit value. This validation will suffice for 
   * testing of pause/stop operations.  
   *  
   * @param regionName
   * @param regionSizeLimit
   */
  public static void validateRegionSizeRemainsSame(String regionName, final int regionSizeLimit) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      final int MIN_VERIFICATION_RUNS = 20;
      int sameRegionSizeCounter = 0;
      long previousSize = -1;
      public boolean done() {
        if (r.keySet().size() == previousSize) {
          sameRegionSizeCounter++;
          int s = r.keySet().size();
          //if the sameRegionSizeCounter exceeds the minimum verification runs and regionSize is below specified limit, then return true
          if (sameRegionSizeCounter >= MIN_VERIFICATION_RUNS &&  s <= regionSizeLimit) {
            return true;
          } else {
            return false;
          }
          
        } else { //current regionSize is not same as recorded previous regionSize 
          previousSize = r.keySet().size(); //update the previousSize variable with current region size
          sameRegionSizeCounter = 0;//reset the sameRegionSizeCounter
          return false;
        }
      }

      public String description() {
        return "Expected region size to remain same below a specified limit but actual region size does not remain same or exceeded the specified limit " + sameRegionSizeCounter + " :regionSize " + previousSize;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 200000, 500, true); 
  }
  
  public static String getRegionFullPath(String regionName) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    return r.getFullPath();
  }
  
  public static Integer getRegionSize(String regionName) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    return r.keySet().size();
  }
  
  public static void validateRegionContents(String regionName, final Map keyValues) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        for(Object key: keyValues.keySet()) {
          if (!r.get(key).equals(keyValues.get(key))) {
            getLogWriter().info(
                "The values are for key " + "  " + key + " " + r.get(key)
                    + " in the map " + keyValues.get(key));
            return false;
          }
        }
        return true;
      }

      public String description() {
        return "Expected region entries doesn't match";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120000, 500, true); 
  }
  
  public static void CheckContent(String regionName, final int regionSize) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    for (long i = 0; i < regionSize; i++) {
      assertEquals(i, r.get(i));
    }
  }
  
  public static void validateRegionContentsForPR(String regionName,
      final int regionSize) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.keySet().size() == regionSize) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected region entries: " + regionSize + " but actual entries: " + r.keySet().size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120000, 500, true); 
  }
  
  public static void verifyPrimaryStatus(final Boolean isPrimary) {
    final Set<GatewaySender> senders = cache.getGatewaySenders();
    assertEquals(senders.size(), 1);
    final AbstractGatewaySender sender = (AbstractGatewaySender)senders.iterator().next();
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (sender.isPrimary() == isPrimary.booleanValue()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected sender to be : " + isPrimary.booleanValue() + " but actually it is : " + sender.isPrimary();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
  }
  
  public static Boolean getPrimaryStatus(){
    Set<GatewaySender> senders = cache.getGatewaySenders();
    assertEquals(senders.size(), 1);
    final AbstractGatewaySender sender = (AbstractGatewaySender)senders.iterator().next();
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (sender.isPrimary()) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Checking Primary Status";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10000, 500, false);
    return sender.isPrimary();
  }
  
  public static Set<Integer> getAllPrimaryBucketsOnTheNode(String regionName) {
    PartitionedRegion region = (PartitionedRegion)cache.getRegion(regionName);
    return region.getDataStore().getAllLocalPrimaryBucketIds();
  }
  
  public static void doHeavyPuts(String regionName, int numPuts) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    // GatewaySender.DEFAULT_BATCH_SIZE * OBJECT_SIZE should be more than MAXIMUM_QUEUE_MEMORY
    // to guarantee overflow
    for (long i = 0; i < numPuts; i++) {
      r.put(i, new byte[1024*1024]);
    }
  }
  
  public static void addListenerAndKillPrimary(){
    Set<GatewaySender> senders = ((GemFireCacheImpl)cache).getAllGatewaySenders();
    assertEquals(senders.size(), 1);
    AbstractGatewaySender sender = (AbstractGatewaySender)senders.iterator().next();
    Region queue = cache.getRegion(Region.SEPARATOR+sender.getId()+"_SERIAL_GATEWAY_SENDER_QUEUE");
    assertNotNull(queue);
    CacheListenerAdapter cl = new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        if((Long)event.getKey() > 900){ 
          cache.getLogger().fine(" Gateway sender is killed by a test");
          cache.close();
          cache.getDistributedSystem().disconnect();
        }
      }
    };
    queue.getAttributesMutator().addCacheListener(cl);
  }
  
  public static void addCacheListenerAndDestroyRegion(String regionName){
    final Region region = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(region);
    CacheListenerAdapter cl = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if((Long)event.getKey() == 99){ 
          region.destroyRegion();
        }
      }
    };
    region.getAttributesMutator().addCacheListener(cl);
  }
  
  public static void addCacheListenerAndCloseCache(String regionName){
    final Region region = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(region);
    CacheListenerAdapter cl = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if((Long)event.getKey() == 900){ 
          cache.getLogger().fine(" Gateway sender is killed by a test");
          cache.close();
          cache.getDistributedSystem().disconnect();
        }
      }
    };
    region.getAttributesMutator().addCacheListener(cl);
  }
  
  public static Boolean killSender(String senderId){
    final ExpectedException exln = addExpectedException("Could not connect");
    ExpectedException exp = addExpectedException(CacheClosedException.class
        .getName());
    ExpectedException exp1 = addExpectedException(ForceReattemptException.class
        .getName());
    try {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    if (sender.isPrimary()) {
      getLogWriter().info("Gateway sender is killed by a test");
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
  
  public static Boolean killAsyncEventQueue(String asyncQueueId){
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    AsyncEventQueueImpl queue = null;
    for(AsyncEventQueue q : queues){
      if(q.getId().equals(asyncQueueId)){
        queue = (AsyncEventQueueImpl) q;
        break;
      }
    }
    if (queue.isPrimary()) {
      getLogWriter().info("AsyncEventQueue is killed by a test");
      cache.getDistributedSystem().disconnect();
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }
  
  public static void killSender(){
    getLogWriter().info("Gateway sender is going to be killed by a test"); 
    cache.close();
    cache.getDistributedSystem().disconnect();
    getLogWriter().info("Gateway sender is killed by a test");
  }
  
  static void waitForSitesToUpdate() {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return false;
      }
      public String description() {
        return "Waiting for all sites to get updated";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10000, 500, false);
  }
  
  public static void checkAllSiteMetaData(     
      Map<Integer, List<Integer>> dsVsPorts) {
    waitForSitesToUpdate();
    assertNotNull(system);
//    Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((DistributionConfigImpl)system
//        .getConfig()).getAllServerLocatorsInfo();
    
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((InternalLocator)locator).getlocatorMembershipListener().getAllLocatorsInfo();
    System.out.println("allSiteMetaData : " + allSiteMetaData);
    System.out.println("dsVsPorts : " + dsVsPorts);
    System.out.println("Server allSiteMetaData : " + ((InternalLocator)locator).getlocatorMembershipListener().getAllServerLocatorsInfo());
    
    //assertEquals(dsVsPorts.size(), allSiteMetaData.size());
    for (Map.Entry<Integer, List<Integer>> entry : dsVsPorts.entrySet()) {
      Set<DistributionLocatorId> locators = allSiteMetaData.get(entry.getKey());
      assertNotNull(locators);
      List<Integer> value = entry.getValue();
      //assertEquals(locators.size(), value.size());
      for (Integer port : entry.getValue()) {
        boolean portAvailable = false;
        for(DistributionLocatorId locId : locators){
          if(locId.getPort() == port){
            portAvailable = true;
          }
        }
        assertTrue(portAvailable);
      }
    }
  }
  
  public static Long checkAllSiteMetaDataFor3Sites(final Map<Integer, Set<String>> dsVsPort) {
    
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (system != null) {
          return true;
        }
        else {
          return false;
        }
      }

      public String description() {
        return "Making sure system is initialized";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 50000, 1000, true); 
    assertNotNull(system);
    
//    final Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((DistributionConfigImpl)system
//        .getConfig()).getAllServerLocatorsInfo();
    
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    LocatorMembershipListener listener = ((InternalLocator)locator).getlocatorMembershipListener();
    if(listener == null) {
      fail("No locator membership listener available. WAN is likely not enabled. Is this test in the WAN project?");
    }
    final Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = listener.getAllLocatorsInfo();
    System.out.println("allSiteMetaData : " + allSiteMetaData);
    
    wc = new WaitCriterion() {
      public boolean done() {
        if (dsVsPort.size() == allSiteMetaData.size()) {
          for (Map.Entry<Integer, Set<String>> entry : dsVsPort.entrySet()) {
            Set<DistributionLocatorId> locators = allSiteMetaData.get(entry
                .getKey());
            for (String locator : entry.getValue()) {
              DistributionLocatorId locatorId = new DistributionLocatorId(
                  locator);
              if (!locators.contains(locatorId)) {
                return false;
              }
            }
          }
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected site Metadata: " + dsVsPort
            + " but actual meta data: " + allSiteMetaData;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 300000, 500, true); 
    return System.currentTimeMillis();
  }
  
  public static void checkLocatorsinSender(String senderId, InetSocketAddress locatorToWaitFor)
      throws InterruptedException {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    
    MyLocatorCallback callback = (MyLocatorCallback)((AbstractGatewaySender)sender)
        .getLocatorDiscoveryCallback();
     
    boolean discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    Assert.assertTrue(
        "Waited " + MAX_WAIT + " for " + locatorToWaitFor
            + " to be discovered on client. List is now: "
            + callback.getDiscovered(), discovered);
  }
  
  public static void validateQueueContents(final String senderId,
      final int regionSize) {
    ExpectedException exp1 = addExpectedException(InterruptedException.class
        .getName());
    ExpectedException exp2 = addExpectedException(GatewaySenderException.class
        .getName());
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
        final Set<RegionQueue> queues = ((AbstractGatewaySender) sender)
            .getQueues();
        WaitCriterion wc = new WaitCriterion() {
          int size = 0;

          public boolean done() {
            size = 0;
            for (RegionQueue q : queues) {
              size += q.size();
            }
            if (size == regionSize) {
              return true;
            }
            return false;
          }

          public String description() {
            return "Expected queue entries: " + regionSize
                + " but actual entries: " + size;
          }
        };
        DistributedTestCase.waitForCriterion(wc, 120000, 500, true);

      } else if (sender.isParallel()) {
        final RegionQueue regionQueue;
        regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(
            new RegionQueue[1])[0];
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            if (regionQueue.size() == regionSize) {
              return true;
            }
            return false;
          }

          public String description() {
            return "Expected queue entries: " + regionSize
                + " but actual entries: " + regionQueue.size();
          }
        };
        DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
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
      regionQueue = ((AbstractGatewaySender)sender)
          .getQueuesForConcurrentSerialGatewaySender();
    } else {
      regionQueue = null;
    }
    assertEquals(null, regionQueue);
  }
  
  public static void validateQueueContentsForConcurrentSerialGatewaySender(
      final String senderId, final int regionSize) {
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
      regionQueue = ((AbstractGatewaySender)sender)
          .getQueuesForConcurrentSerialGatewaySender();
    } else {
      regionQueue = null;
    }
    WaitCriterion wc = new WaitCriterion() {
      int size = 0;

      public boolean done() {
        size = 0;
        for (RegionQueue q : regionQueue) {
          size += q.size();
        }
        if (size == regionSize) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected queue entries: " + regionSize
            + " but actual entries: " + size;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
  }
  
  public static Integer getQueueContentSize(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (!sender.isParallel()) {
      final Set<RegionQueue> queues = ((AbstractGatewaySender)sender)
          .getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      return size;
    } else if (sender.isParallel()) {
      RegionQueue regionQueue = null;
      regionQueue = ((AbstractGatewaySender)sender).getQueues().toArray(
          new RegionQueue[1])[0];
      return regionQueue.getRegion().size();
    }
    return 0;
  }
  
  public static void validateParallelSenderQueueBucketSize(final String senderId, final int bucketSize) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    RegionQueue regionQueue = ((AbstractGatewaySender)sender).getQueues().toArray(new RegionQueue[1])[0];
    Set<BucketRegion> buckets = ((PartitionedRegion)regionQueue.getRegion()).getDataStore().getAllLocalPrimaryBucketRegions();
    for (BucketRegion bucket : buckets) {
      assertEquals("Expected bucket entries for bucket " + bucket.getId() + " is different than actual.", bucketSize, bucket.keySet().size());
    }
  }

  public static void validateParallelSenderQueueAllBucketsDrained(
      final String senderId) {
    ExpectedException exp = addExpectedException(RegionDestroyedException.class
        .getName());
    ExpectedException exp1 = addExpectedException(ForceReattemptException.class
        .getName());
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      RegionQueue regionQueue = ((AbstractGatewaySender) sender)
          .getQueues().toArray(new RegionQueue[1])[0];
      Set<BucketRegion> buckets = ((PartitionedRegion) regionQueue.getRegion())
          .getDataStore().getAllLocalPrimaryBucketRegions();
      for (final BucketRegion bucket : buckets) {
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            if (bucket.keySet().size() == 0) {
              getLogWriter().info("Bucket " + bucket.getId() + " is empty");
              return true;
            }
            return false;
          }

          public String description() {
            return "Expected bucket entries for bucket: " + bucket.getId()
                + " is: 0 but actual entries: " + bucket.keySet().size()
                + " This bucket isPrimary: "
                + bucket.getBucketAdvisor().isPrimary() + " KEYSET: "
                + bucket.keySet();
          }
        };
        DistributedTestCase.waitForCriterion(wc, 180000, 50, true);
      }// for loop ends
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

    final MyGatewayEventFilter_AfterAck filter = (MyGatewayEventFilter_AfterAck)sender
        .getGatewayEventFilters().get(0);
    return filter.getAckList().size();
  }
  
  public static int verifyAndGetEventsDispatchedByConcurrentDispatchers(
      final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    ConcurrentParallelGatewaySenderEventProcessor cProc = (ConcurrentParallelGatewaySenderEventProcessor)((AbstractGatewaySender)sender)
        .getEventProcessor();
    if (cProc == null) return 0;

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
      regionQueue = ((AbstractGatewaySender)sender).getQueues().toArray(
          new RegionQueue[1])[0];
      numEntries = ((ConcurrentParallelGatewaySenderQueue)regionQueue)
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
      regionQueue = ((AbstractGatewaySender)sender).getQueues().toArray(
          new RegionQueue[1])[0];
      numEntries = ((ConcurrentParallelGatewaySenderQueue)regionQueue)
          .getNumEntriesInVMTestOnly();
    }
    return numEntries;
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
      final Set<RegionQueue> queues = ((AbstractGatewaySender)sender)
          .getQueues();
      int queueSize = 0;
      for (RegionQueue q : queues) {
        queueSize += q.size();
      }

      assertEquals("verifyQueueSize failed for sender " + senderId, size,
          queueSize);
    } else if (sender.isParallel()) {
      RegionQueue regionQueue = ((AbstractGatewaySender)sender).getQueues()
          .toArray(new RegionQueue[1])[0];
      assertEquals("verifyQueueSize failed for sender " + senderId, size,
          regionQueue.size());
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
      final Set<RegionQueue> queues = ((AbstractGatewaySender)sender)
          .getQueues();
      int queueSize = 0;
      for (RegionQueue q : queues) {
        queueSize += q.size();
      }

      assertTrue(queueSize > 0);
    } else if (sender.isParallel()) {
      RegionQueue regionQueue = ((AbstractGatewaySender)sender).getQueues()
          .toArray(new RegionQueue[1])[0];
      assertTrue(regionQueue.size() > 0);
    }
  }

  public static void verifyRegionQueueNotEmptyForConcurrentSender(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (!sender.isParallel()) {
      Set<RegionQueue> queues = ((AbstractGatewaySender)sender).getQueuesForConcurrentSerialGatewaySender();
      for(RegionQueue q:queues) {
        assertTrue(q.size() > 0);  
      }
    }
  }
  
  /**
   * Test methods for sender operations 
   * @param senderId
   */
  public static void verifySenderPausedState(String senderId) {    
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    //exln.remove();
    assertTrue(sender.isPaused());
  }

  public static void verifySenderResumedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertTrue(sender.isRunning());
  }

  public static void verifySenderStoppedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertFalse(sender.isRunning());
    //assertFalse(sender.isPaused());
  }
  
  public static void verifySenderRunningState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertTrue(sender.isRunning());
  }
  
  public static void removeSenderFromTheRegion(String senderId,
      String regionName) {
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
    ((AbstractGatewaySender) sender).destroy();
  }
  
  public static void verifySenderDestroyed(String senderId, boolean isParallel) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
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
    
    Set<LocalRegion> allRegions = ((GemFireCacheImpl) cache).getAllRegions();
    for (LocalRegion region : allRegions) {
      if (region.getName().indexOf(senderId + queueRegionNameSuffix) != -1) {
        fail("Region underlying the sender is not destroyed.");
      }
    }
  }

  protected Integer[] createLNAndNYLocators() {
    Integer lnPort = (Integer) vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));
    return new Integer[] { lnPort, nyPort };
  }

  public static class MyLocatorCallback extends
      LocatorDiscoveryCallbackAdapter {

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

    public boolean waitForRemove(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(removedLocators, locator, time);
    }

    private synchronized boolean waitFor(Set set, InetSocketAddress locator,
        long time) throws InterruptedException {
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
  
  protected static class PutTask implements Callable {
    private final Region region;

    private final AtomicInteger key_value;

    private final int numPuts;
    
    public PutTask(Region region, AtomicInteger key_value, int numPuts) {
      this.region = region;
      this.key_value = key_value;
      this.numPuts = numPuts;
    }

    public Object call() throws Exception {
      while (true) {
        int key = key_value.incrementAndGet();
        if(key < numPuts) {
          region.put(key, key);
        }
        else {
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
    
    public MyGatewayEventFilter() {
    }
    
    public MyGatewayEventFilter(String id) {
      Id = id;
    }
    
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      this.beforeEnqueueInvoked = true;
      if ((Long)event.getKey() >= 500 && (Long)event.getKey() < 600) {
        return false;
      }
      return true;
    }

    public boolean beforeTransmit(GatewayQueueEvent event) {
      this.beforeTransmitInvoked = true;
      if ((Long)event.getKey() >= 600 && (Long)event.getKey() < 700) {
        return false;
      }
      return true;
    }

    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    public void afterAcknowledgement(GatewayQueueEvent event) {
      this.afterAckInvoked = true;
      // TODO Auto-generated method stub
    }
    
    public boolean equals(Object obj){
      if(this == obj){
        return true;
      }
      if ( !(obj instanceof MyGatewayEventFilter) ) return false;
      MyGatewayEventFilter filter = (MyGatewayEventFilter)obj;
      return this.Id.equals(filter.Id);
    }
  }
  
  public static class MyGatewayEventFilter_AfterAck implements
      GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter_AfterAck";

    ConcurrentSkipListSet<Long> ackList = new ConcurrentSkipListSet<Long>();

    public MyGatewayEventFilter_AfterAck() {
    }

    public MyGatewayEventFilter_AfterAck(String id) {
      Id = id;
    }

    public boolean beforeEnqueue(GatewayQueueEvent event) {
      return true;
    }

    public boolean beforeTransmit(GatewayQueueEvent event) {
      return true;
    }

    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    public void afterAcknowledgement(GatewayQueueEvent event) {
      ackList.add((Long)event.getKey());
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
      MyGatewayEventFilter filter = (MyGatewayEventFilter)obj;
      return this.Id.equals(filter.Id);
    }
  }
  
 public static class PDXGatewayEventFilter implements GatewayEventFilter, Serializable {
    
    String Id = "PDXGatewayEventFilter";
    
    public int beforeEnqueueInvoked;
    public int beforeTransmitInvoked;
    public int afterAckInvoked;
    
    public PDXGatewayEventFilter() {
    }
    
    public PDXGatewayEventFilter(String id) {
      Id = id;
    }
    
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      System.out.println("Invoked enqueue for " + event);
      this.beforeEnqueueInvoked++;
      return true;
    }

    public boolean beforeTransmit(GatewayQueueEvent event) {
      System.out.println("Invoked transmit for " + event);
      this.beforeTransmitInvoked++;
      return true;
    }

    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    public void afterAcknowledgement(GatewayQueueEvent event) {
      System.out.println("Invoked afterAck for " + event);
      this.afterAckInvoked++;
      // TODO Auto-generated method stub
    }
    
    public boolean equals(Object obj){
      if(this == obj){
        return true;
      }
      if ( !(obj instanceof MyGatewayEventFilter) ) return false;
      MyGatewayEventFilter filter = (MyGatewayEventFilter)obj;
      return this.Id.equals(filter.Id);
    }
  }
  
/*  static class MyAsyncEventFilter implements AsyncEventFilter, Serializable{
    
    String Id = "MyAsyncEventFilter";
    
    public MyAsyncEventFilter() {
    }
    
    public MyAsyncEventFilter(String id) {
      Id = id;
    }
    
    public boolean beforeEnqueue(AsyncEvent event) {
      return true;
    }

    public boolean beforeTransmit(AsyncEvent event) {
      return true;
    }

    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    public void afterAcknowledgement(AsyncEvent event) {
      // TODO Auto-generated method stub
    }
    
    public boolean equals(Object obj){
      if(this == obj){
        return true;
      }
      if ( !(obj instanceof MyAsyncEventFilter) ) return false;
      MyAsyncEventFilter filter = (MyAsyncEventFilter)obj;
      return this.Id.equals(filter.Id);
    }
  }*/
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    cleanupVM();
    vm0.invoke(WANTestBase.class, "cleanupVM");
    vm1.invoke(WANTestBase.class, "cleanupVM");
    vm2.invoke(WANTestBase.class, "cleanupVM");
    vm3.invoke(WANTestBase.class, "cleanupVM");
    vm4.invoke(WANTestBase.class, "cleanupVM");
    vm5.invoke(WANTestBase.class, "cleanupVM");
    vm6.invoke(WANTestBase.class, "cleanupVM");
    vm7.invoke(WANTestBase.class, "cleanupVM");
  }
  
  public static void cleanupVM() throws IOException {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    closeCache();
    deletePDXDir();
  }

  public static void closeCache() throws IOException {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      cache = null;
    } else {
      WANTestBase test = new WANTestBase(testName);
      if (test.isConnectedToDS()) {
        test.getSystem().disconnect();
      }
    }
  }

  public static void deletePDXDir() throws IOException {
    File pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    FileUtil.delete(pdxDir);
  }
  
  public static void shutdownLocator() {
    WANTestBase test = new WANTestBase(testName);
    test.getSystem().disconnect();
  }  
  
  public static void printEventListenerMap() {
    ((MyGatewaySenderEventListener)eventListener1).printMap();
  }
  
  @Override
  public InternalDistributedSystem getSystem(Properties props) {
    // For now all WANTestBase tests allocate off-heap memory even though
    // many of them never use it.
    // The problem is that WANTestBase has static methods that create instances
    // of WANTestBase (instead of instances of the subclass). So we can't override
    // this method so that only the off-heap subclasses allocate off heap memory.
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "300m");
    return super.getSystem(props);
  }
  
  /**
   * Returns true if the test should create off-heap regions.
   * OffHeap tests should over-ride this method and return false.
   */
  public boolean isOffHeap() {
    return false;
  }
}
