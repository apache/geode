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
package org.apache.geode.internal.cache.partitioned.fixed;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class FixedPartitioningWithTransactionDistributedTest implements
    Serializable {

  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
  private VM server2;
  private VM accessor;

  private static final String FIXED_PARTITION_NAME = "singleBucket";


  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedDiskDirRule distributedDiskDir = new DistributedDiskDirRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setup() throws Exception {
    server1 = getVM(0);
    server2 = getVM(1);
    accessor = getVM(2);
    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    Invoke.invokeInEveryVM(() -> {
      TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = true;
    });
  }

  @After
  public void tearDown() {
    Invoke.invokeInEveryVM(() -> {
      TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS = false;
      cacheRule.closeAndNullCache();
    });
  }

  @Test
  public void executeFunctionOnMovedPrimaryBucketFailWithTransactionDataRebalancedException() {
    createData();

    server1.invoke(() -> {
      cacheRule.closeAndNullCache();
    });

    TransactionId transactionId = server2.invoke(() -> doFunctionTransactionAndSuspend());

    server1.invoke(() -> restartPrimary());

    server2.invoke(() -> {
      assertThatThrownBy(() -> resumeFunctionTransaction(transactionId))
          .isInstanceOf(TransactionDataRebalancedException.class);
    });
  }

  @Test
  public void accessorExecuteFunctionOnMovedPrimaryBucketFailWithTransactionDataRebalancedException() {
    createData();
    server2.invoke(() -> registerFunctions());

    accessor.invoke(() -> createServerRegion(false, 1, 1, true));
    accessor.invoke(() -> registerFunctions());
    server1.invoke(() -> {
      cacheRule.closeAndNullCache();
    });

    TransactionId transactionId = accessor.invoke(() -> doFunctionTransactionAndSuspend());

    server1.invoke(() -> restartPrimary());

    accessor.invoke(() -> {
      assertThatThrownBy(() -> resumeFunctionTransaction(transactionId))
          .isInstanceOf(TransactionDataRebalancedException.class);
    });
  }

  @Test
  public void clientExecuteFunctionOnMovedPrimaryBucketFailWithTransactionDataRebalancedException() {
    server1.invoke(() -> createServerRegion(true, 1, 1));
    int port2 = server2.invoke(() -> createServerRegion(false, 1, 1));
    int port1 = accessor.invoke(() -> createServerRegion(false, 1, 1, true));
    server1.invoke(() -> doPuts());
    server1.invoke(() -> {
      cacheRule.closeAndNullCache();
    });
    server2.invoke(() -> registerFunctions());
    accessor.invoke(() -> registerFunctions());


    createClientRegion(true, port1, port2);
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    TransactionId transactionId = doFunctionTransactionAndSuspend(region, txManager);

    accessor.invoke(() -> {
      cacheRule.closeAndNullCache();
    });
    server1.invoke(() -> restartPrimary());

    Throwable caughtException =
        catchThrowable(() -> resumeFunctionTransaction(transactionId, region, txManager));
    Assertions.assertThat(caughtException).isInstanceOf(FunctionException.class);
    Assertions.assertThat(caughtException.getCause())
        .isInstanceOf(TransactionDataRebalancedException.class);
  }

  private void restartPrimary() throws Exception {
    createServerRegion(true, 1, 1);
    PartitionedRegion partitionedRegion =
        (PartitionedRegion) cacheRule.getCache().getRegion(regionName);
    assertThat(partitionedRegion.get(1)).isEqualTo(1);
    await().until(() -> partitionedRegion.getBucketPrimary(0)
        .equals(cacheRule.getCache().getInternalDistributedSystem().getDistributedMember()));
  }

  private void createData() {
    server1.invoke(() -> createServerRegion(true, 1, 1));
    server2.invoke(() -> createServerRegion(false, 1, 1));
    server1.invoke(() -> doPuts());
  }

  private void doPuts() {
    Region region = cacheRule.getCache().getRegion(regionName);
    region.put(1, 1);
    region.put(2, 2);
    region.put(3, 3);
  }

  private int createServerRegion(boolean isPrimary, int redundantCopies, int totalNumOfBuckets)
      throws Exception {
    return createServerRegion(isPrimary, redundantCopies, totalNumOfBuckets, false);
  }

  private int createServerRegion(boolean isPrimary, int redundantCopies, int totalNumOfBuckets,
      boolean isAccessor) throws Exception {
    FixedPartitionAttributes fixedPartition =
        FixedPartitionAttributes.createFixedPartition(FIXED_PARTITION_NAME, isPrimary,
            totalNumOfBuckets);
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumOfBuckets)
        .setPartitionResolver(new MyFixedPartitionResolver());
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    } else {
      factory.addFixedPartitionAttributes(fixedPartition);
    }

    cacheRule.getOrCreateCache().createRegionFactory(
        isAccessor ? RegionShortcut.PARTITION : RegionShortcut.PARTITION_PERSISTENT)
        .setPartitionAttributes(factory.create()).create(regionName);
    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(boolean connectToFirstPort, int... ports) {
    clientCacheRule.createClientCache();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl pool;
    try {
      pool = getPool(ports);
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);

    if (ports.length > 1 && connectToFirstPort) {
      // first connection to the first port in the list
      pool.acquireConnection(new ServerLocation(hostName, ports[0]));
    }
  }

  private PoolImpl getPool(int... ports) {
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    factory.setPRSingleHopEnabled(false);
    return (PoolImpl) factory.create(uniqueName);
  }

  private void registerFunctions() {
    FunctionService.registerFunction(new MySuspendTransactionFunction());
    FunctionService.registerFunction(new MyResumeTransactionFunction());
  }

  private TransactionId doFunctionTransactionAndSuspend() {
    Region region = cacheRule.getCache().getRegion(regionName);
    TXManagerImpl manager = cacheRule.getCache().getTxManager();
    return doFunctionTransactionAndSuspend(region, manager);
  }

  private TransactionId doFunctionTransactionAndSuspend(Region region,
      CacheTransactionManager manager) {
    Execution execution = FunctionService.onRegion(region);
    manager.begin();
    Set keySet = new HashSet();
    keySet.add(2);
    ResultCollector resultCollector = execution.execute(new MySuspendTransactionFunction());
    resultCollector.getResult();
    return manager.suspend();
  }

  private void resumeFunctionTransaction(TransactionId transactionId) {
    Region region = cacheRule.getCache().getRegion(regionName);
    TXManagerImpl manager = cacheRule.getCache().getTxManager();
    resumeFunctionTransaction(transactionId, region, manager);
  }

  private void resumeFunctionTransaction(TransactionId transactionId, Region region,
      CacheTransactionManager manager) {
    Execution execution = FunctionService.onRegion(region);
    manager.resume(transactionId);
    try {
      Set keySet = new HashSet();
      keySet.add(3);
      ResultCollector resultCollector =
          execution.withFilter(keySet).execute(new MyResumeTransactionFunction());
      resultCollector.getResult();
    } finally {
      manager.rollback();
    }
  }

  private static class MyFixedPartitionResolver implements FixedPartitionResolver {

    public MyFixedPartitionResolver() {}

    @Override
    public String getPartitionName(final EntryOperation opDetails,
        @Deprecated final Set targetPartitions) {
      return FIXED_PARTITION_NAME;
    }

    @Override
    public Object getRoutingObject(final EntryOperation opDetails) {
      return opDetails.getKey();
    }

    @Override
    public String getName() {
      return getClass().getName();
    }
  }

  public static class MySuspendTransactionFunction implements Function, DataSerializable {
    @Override
    public void execute(FunctionContext context) {
      assertThat(context instanceof RegionFunctionContext);
      PartitionedRegion region = (PartitionedRegion) ((RegionFunctionContext) context).getDataSet();
      region.containsValueForKey(2);
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }

  public static class MyResumeTransactionFunction implements Function, DataSerializable {
    @Override
    public void execute(FunctionContext context) {
      assertThat(context instanceof RegionFunctionContext);
      PartitionedRegion region = (PartitionedRegion) ((RegionFunctionContext) context).getDataSet();
      region.containsValueForKey(3);
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }
}
