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

import static java.util.concurrent.TimeUnit.MINUTES;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

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
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ClientMetadataService;
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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@RunWith(Parameterized.class)
public class FixedPartitioningWithTransactionDistributedTest implements
    Serializable {

  private String hostName;
  private String uniqueName;
  private String regionName;
  private VM server1;
  private VM server2;
  private VM accessor;
  private int port1;
  private int port2;
  private transient PoolImpl pool;

  private enum Type {
    ON_REGION, ON_SERVER, ON_MEMBER
  }

  private static final String FIXED_PARTITION_NAME = "singleBucket";

  private enum ExecuteFunctionMethod {
    ExecuteFunctionByObject, ExecuteFunctionById
  }

  @Parameters(name = "{0}")
  public static ExecuteFunctionMethod[] data() {
    return ExecuteFunctionMethod.values();
  }

  @Parameter
  public ExecuteFunctionMethod functionExecutionType;

  private boolean executeFunctionByIdOnClient() {
    return ExecuteFunctionMethod.ExecuteFunctionById == functionExecutionType;
  }

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
  public void setup() {
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

    server1.invoke(() -> cacheRule.closeAndNullCache());

    TransactionId transactionId = server2.invoke(
        (SerializableCallableIF<TransactionId>) this::doFunctionTransactionAndSuspend);

    server1.invoke(this::restartPrimary);
    server2.invoke(() -> {
      assertThatThrownBy(() -> resumeFunctionTransaction(transactionId))
          .isInstanceOf(TransactionDataRebalancedException.class);
    });
  }

  @Test
  public void accessorExecuteFunctionOnMovedPrimaryBucketFailWithTransactionDataRebalancedException() {
    createData();
    server2.invoke(this::registerFunctions);

    accessor.invoke(() -> createServerRegion(false, 1, 1, true));
    accessor.invoke(this::registerFunctions);
    server1.invoke(() -> cacheRule.closeAndNullCache());

    TransactionId transactionId = accessor.invoke(
        (SerializableCallableIF<TransactionId>) this::doFunctionTransactionAndSuspend);

    server1.invoke(this::restartPrimary);

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
    server1.invoke((SerializableRunnableIF) this::doPuts);
    server1.invoke(() -> cacheRule.closeAndNullCache());
    server2.invoke(this::registerFunctions);
    accessor.invoke(this::registerFunctions);


    createClientRegion(true, port1, port2);
    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    TransactionId transactionId = doFunctionTransactionAndSuspend(region, txManager);

    accessor.invoke(() -> cacheRule.closeAndNullCache());
    server1.invoke(this::restartPrimary);

    Throwable caughtException =
        catchThrowable(() -> resumeFunctionTransaction(transactionId, region, txManager));
    Assertions.assertThat(caughtException).isInstanceOf(FunctionException.class);
    Assertions.assertThat(caughtException.getCause())
        .isInstanceOf(TransactionDataRebalancedException.class);
  }

  @Test
  public void clientCanRollbackFunctionOnRegionWithFilterAndWithSingleHopEnabled() {
    setupServers();
    setupClient();

    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();

    TransactionId transactionId =
        doFunctionTransactionAndSuspend(region, txManager, new MyTransactionFunction());
    txManager.resume(transactionId);
    txManager.rollback();

    server1.invoke(() -> {
      assertThat(cacheRule.getCache().getRegion(regionName).get(2)).isEqualTo(2);
    });
  }

  private void forceClientMetadataUpdate(Region region) {
    ClientMetadataService clientMetadataService =
        ((InternalCache) clientCacheRule.getClientCache()).getClientMetadataService();
    clientMetadataService.scheduleGetPRMetaData((InternalRegion) region, true);
    await().atMost(5, MINUTES).until(clientMetadataService::isMetadataStable);
  }

  @Test
  public void clientCanRollbackFunctionOnRegionWithoutFilterAndWithSingleHopEnabled() {
    setupServers();
    setupClient();

    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();

    try {
      TransactionId transactionId = doFunctionTransactionAndSuspend(region, txManager,
          new MyTransactionFunction(), Type.ON_REGION, false);
      txManager.resume(transactionId);
      txManager.rollback();
    } catch (FunctionException functionException) {
      // without filter function can target to any server and may not go to primary.
      assertThat(functionException.getCause()).isInstanceOf(ServerOperationException.class);
      assertThat(functionException.getCause().getCause()).isInstanceOf(FunctionException.class);
      assertThat(functionException.getCause().getCause().getCause())
          .isInstanceOf(TransactionDataRebalancedException.class);
      txManager.rollback();
    }

    server1.invoke(() -> {
      assertThat(cacheRule.getCache().getRegion(regionName).get(2)).isEqualTo(2);
    });
  }

  @Test
  public void clientTransactionFailsIfExecuteFunctionOnMember() {
    setupServers();
    setupClient();

    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();

    Throwable caughtException = catchThrowable(() -> doFunctionTransactionAndSuspend(region,
        txManager, new MyTransactionFunction(), Type.ON_MEMBER));

    assertThat(caughtException).isInstanceOf(UnsupportedOperationException.class);
    txManager.rollback();
  }

  @Test
  public void clientTransactionFailsIfExecuteFunctionOnServer() {
    setupServers();
    setupClient();

    Region region = clientCacheRule.getClientCache().getRegion(regionName);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();

    Throwable caughtException = catchThrowable(() -> doFunctionTransactionAndSuspend(region,
        txManager, new MyTransactionFunction(), Type.ON_SERVER));

    assertThat(caughtException).isInstanceOf(FunctionException.class);
    assertThat(caughtException.getCause()).isInstanceOf(UnsupportedOperationException.class);
    txManager.rollback();
  }

  private void setupServers() {
    port1 = server1.invoke(() -> createServerRegion(true, 1, 1));
    port2 = server2.invoke(() -> createServerRegion(false, 1, 1));

    server1.invoke(this::registerFunctions);
    server2.invoke(this::registerFunctions);
  }

  private void setupClient() {
    createClientRegion(true, true, port1, port2);
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    doPuts(region);
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
    server1.invoke((SerializableRunnableIF) this::doPuts);
  }

  private void doPuts() {
    doPuts(cacheRule.getCache().getRegion(regionName));
  }

  private void doPuts(Region<Integer, Integer> region) {
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
    PartitionAttributesFactory<Integer, Integer> factory = new PartitionAttributesFactory<>();
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
    createClientRegion(connectToFirstPort, false, ports);
  }

  private void createClientRegion(boolean connectToFirstPort, boolean singleHopEnabled,
      int... ports) {
    clientCacheRule.createClientCache();

    CacheServerTestUtil.disableShufflingOfEndpoints();
    try {
      pool = getPool(singleHopEnabled, ports);
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

  private PoolImpl getPool(boolean singleHopEnabled, int... ports) {
    PoolFactory factory = PoolManager.createFactory();
    for (int port : ports) {
      factory.addServer(hostName, port);
    }
    factory.setPRSingleHopEnabled(singleHopEnabled);
    return (PoolImpl) factory.create(uniqueName);
  }

  private void registerFunctions() {
    FunctionService.registerFunction(new MySuspendTransactionFunction());
    FunctionService.registerFunction(new MyResumeTransactionFunction());
    FunctionService.registerFunction(new MyTransactionFunction());
  }

  private TransactionId doFunctionTransactionAndSuspend() {
    Region region = cacheRule.getCache().getRegion(regionName);
    TXManagerImpl manager = cacheRule.getCache().getTxManager();
    return doFunctionTransactionAndSuspend(region, manager);
  }

  private TransactionId doFunctionTransactionAndSuspend(Region region,
      CacheTransactionManager manager) {
    return doFunctionTransactionAndSuspend(region, manager, new MySuspendTransactionFunction());
  }

  private TransactionId doFunctionTransactionAndSuspend(Region region,
      CacheTransactionManager manager, Function function) {
    return doFunctionTransactionAndSuspend(region, manager, function, Type.ON_REGION);
  }

  private TransactionId doFunctionTransactionAndSuspend(Region region,
      CacheTransactionManager manager, Function function, Type type) {
    return doFunctionTransactionAndSuspend(region, manager, function, type, true);
  }

  private TransactionId doFunctionTransactionAndSuspend(Region region,
      CacheTransactionManager manager, Function function, Type type, boolean withFilter) {
    Execution execution;
    Set<Integer> keySet = new HashSet<>();
    keySet.add(2);
    switch (type) {
      case ON_MEMBER:
        execution = FunctionService.onMembers();
        break;
      case ON_REGION:
        execution = FunctionService.onRegion(region);
        if (withFilter) {
          execution = execution.withFilter(keySet);
        }
        break;
      case ON_SERVER:
        execution = FunctionService.onServers(pool);
        break;
      default:
        throw new RuntimeException("unexpected type");
    }

    boolean executeFunctionByIdOnClient = false;
    if (clientCacheRule.getClientCache() != null) {
      forceClientMetadataUpdate(region);
      executeFunctionByIdOnClient = executeFunctionByIdOnClient() && type != Type.ON_MEMBER;
    }

    manager.begin();
    final ResultCollector resultCollector;
    if (executeFunctionByIdOnClient) {
      resultCollector = execution.execute(function.getId());
    } else {
      resultCollector = execution.execute(function);
    }
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
      Set<Integer> keySet = new HashSet<>();
      keySet.add(3);
      ResultCollector resultCollector =
          execution.withFilter(keySet).execute(new MyResumeTransactionFunction());
      resultCollector.getResult();
    } finally {
      manager.rollback();
    }
  }

  public static class MyFixedPartitionResolver implements FixedPartitionResolver<Integer, Integer> {

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
      assertThat(context).isInstanceOf(RegionFunctionContext.class);
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
      assertThat(context).isInstanceOf(RegionFunctionContext.class);
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

  public static class MyTransactionFunction implements Function, DataSerializable {
    @Override
    public void execute(FunctionContext context) {
      if (context instanceof RegionFunctionContext) {
        PartitionedRegion region =
            (PartitionedRegion) ((RegionFunctionContext) context).getDataSet();
        region.destroy(2);
        context.getResultSender().lastResult(Boolean.TRUE);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }
  }
}
