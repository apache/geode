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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Java6Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class FunctionExecutionWithTransactionDistributedTest implements
    Serializable {

  private String hostName;
  private String uniqueName;
  private String partitionedRegionName;
  private String replicateRegionName;
  private VM server1;
  private VM server2;
  private VM server3;
  private VM accessor;
  private int port1;
  private int port2;
  private int port3;
  private DistributedMember distributedMember;
  private DistributedMember distributedMember2;
  private transient PoolImpl pool;

  private static final int KEY1 = 1;
  private static final int KEY2 = 2;
  private static final int KEY3 = 3;
  private static final int KEY4 = 4;
  private final String value = "value";

  private enum Type {
    ON_REGION, ON_MEMBER
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setup() throws Exception {
    accessor = getVM(0);
    server1 = getVM(1);
    server2 = getVM(2);
    server3 = getVM(3);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    partitionedRegionName = uniqueName + "_partitionedRegion";
    replicateRegionName = uniqueName + "_replicateRegion";
  }

  @Test
  public void clientCanRollbackTransactionWithMultipleFunctions() {
    port1 = server1.invoke(() -> createServerRegions(1, 3, false));
    port2 = server2.invoke(() -> createServerRegions(1, 3, false));
    port3 = server3.invoke(() -> createServerRegions(1, 3, false));
    createClientRegions(true, port1, port2, port3);

    doMultipleFunctionsThenRollbackOnClient();

    verifyDataNotChangedOnServersAfterRollback();
  }

  private void doMultipleFunctionsThenRollbackOnClient() {
    Region region = clientCacheRule.getClientCache().getRegion(partitionedRegionName);
    Region replicateRegion = clientCacheRule.getClientCache().getRegion(replicateRegionName);
    doPuts(region);
    doPuts(replicateRegion);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true);
    doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
    txManager.rollback();
  }

  @Test
  public void clientCanRollbackTransactionWithOnReplicateRegionFunction() {
    port1 = server1.invoke(() -> createServerRegions(1, 3, false));
    port2 = server2.invoke(() -> createServerRegions(1, 3, false));
    port3 = server3.invoke(() -> createServerRegions(1, 3, false));
    createClientRegions(true, port1, port2, port3);

    Region replicateRegion = clientCacheRule.getClientCache().getRegion(replicateRegionName);
    doPuts(replicateRegion);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
    txManager.rollback();

    verifyReplicateRegionDataNotChangedOnServersAfterRollback();
  }

  private void verifyReplicateRegionDataNotChangedOnServersAfterRollback() {
    server1.invoke(() -> verifyData(cacheRule.getCache().getRegion(replicateRegionName)));
    server2.invoke(() -> verifyData(cacheRule.getCache().getRegion(replicateRegionName)));
    server3.invoke(() -> verifyData(cacheRule.getCache().getRegion(replicateRegionName)));
  }

  @Test
  public void clientConnectToAccessorCanRollbackTransactionWithMultipleFunctions() {
    port1 = accessor.invoke(() -> createServerRegions(1, 3, true));
    setupServerRegions();
    createClientRegions(true, port1);

    doMultipleFunctionsThenRollbackOnClient();

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void clientFunctionOnPartitionedRegionWithTransactionFailsIfWithoutFilter() {
    accessor.invoke(() -> createServerRegions(1, 3, true));
    port1 = server1.invoke(() -> createServerRegions(1, 3, false));
    server2.invoke(() -> createServerRegions(1, 3, false));
    server3.invoke(() -> createServerRegions(1, 3, false));
    createClientRegions(true, port1);

    Region region = clientCacheRule.getClientCache().getRegion(partitionedRegionName);
    doPuts(region);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    Throwable caughtException = catchThrowable(() -> doFunction(region,
        new MyPartitionRegionFunction(), Type.ON_REGION, false));
    assertThat(caughtException).isInstanceOf(FunctionException.class);
    assertThat(caughtException.getCause()).isInstanceOf(TransactionException.class)
        .hasMessage("Function inside a transaction cannot execute on more than one node");
    txManager.rollback();

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  private void verifyPartitionedRegionNotChangedOnServersAfterRollback() {
    server1.invoke(() -> verifyData(cacheRule.getCache().getRegion(partitionedRegionName)));
    server2.invoke(() -> verifyData(cacheRule.getCache().getRegion(partitionedRegionName)));
    server3.invoke(() -> verifyData(cacheRule.getCache().getRegion(partitionedRegionName)));
  }

  @Test
  public void clientCanRollbackTransactionsWithMultipleColocatedKeysFilter() {
    port1 = server1.invoke(() -> createServerRegions(1, 3, false));
    port2 = server2.invoke(() -> createServerRegions(1, 3, false));
    port3 = server3.invoke(() -> createServerRegions(1, 3, false));
    createClientRegions(true, port1, port2, port3);

    Region region = clientCacheRule.getClientCache().getRegion(partitionedRegionName);
    Region replicateRegion = clientCacheRule.getClientCache().getRegion(replicateRegionName);
    doPuts(region);
    doPuts(replicateRegion);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true, true, true);
    doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
    txManager.rollback();

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void clientFunctionWithTransactionsFailsIfWithMultipleNonColocatedKeysFilter() {
    port1 = accessor.invoke(() -> createServerRegions(1, 3, true));
    setupServerRegions();
    createClientRegions(true, port1);

    Region region = clientCacheRule.getClientCache().getRegion(partitionedRegionName);
    doPuts(region);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    Throwable caughtException = catchThrowable(() -> doFunction(region,
        new MyPartitionRegionFunction(), Type.ON_REGION, true, true, false));
    assertThat(caughtException).isInstanceOf(FunctionException.class);
    assertThat(caughtException.getCause()).isInstanceOf(TransactionException.class)
        .hasMessage("Function inside a transaction cannot execute on more than one node");

    txManager.rollback();

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  @Test
  public void clientCanRollbackTransactionWithNonColocatedFunctions() {
    port1 = server1.invoke(() -> createServerRegions(1, 3, false));
    server2.invoke(() -> createServerRegions(1, 3, false));
    server3.invoke(() -> createServerRegions(1, 3, false));
    createClientRegions(true, port1);

    Region region = clientCacheRule.getClientCache().getRegion(partitionedRegionName);
    Region replicateRegion = clientCacheRule.getClientCache().getRegion(replicateRegionName);
    doPuts(region);
    doPuts(replicateRegion);
    CacheTransactionManager txManager =
        clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true);
    doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);

    Throwable caughtException = catchThrowable(() -> doFunction2(region,
        new MyPartitionRegionFunction(), Type.ON_REGION));
    assertThat(caughtException).isInstanceOf(FunctionException.class);
    assertThat(caughtException.getCause()).isInstanceOf(TransactionDataRebalancedException.class)
        .hasMessageContaining("Function execution is not colocated with transaction.");

    txManager.rollback();

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void memberCanRollbackTransactionWithMultipleOnRegionFunctions() {
    setupServerRegions();
    server2.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      Region replicateRegion = cacheRule.getCache().getRegion(replicateRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true);
      doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
      txManager.rollback();
    });

    verifyDataNotChangedOnServersAfterRollback();
  }

  private void verifyDataNotChangedOnServersAfterRollback() {
    server1.invoke(() -> verifyData());
    server2.invoke(() -> verifyData());
    server3.invoke(() -> verifyData());
  }

  private void setupServerRegions() {
    setupServerRegions(false);
  }

  private void setupServerRegions(boolean doPut) {
    int redundantCopies = 1;
    server1.invoke(() -> {
      createServerRegions(redundantCopies, 3, false);
      if (doPut) {
        cacheRule.getCache().getRegion(partitionedRegionName).put(KEY1, value + KEY1);
      }
    });
    server2.invoke(() -> {
      createServerRegions(redundantCopies, 3, false);
      if (doPut) {
        cacheRule.getCache().getRegion(partitionedRegionName).put(KEY2, value + KEY2);
      }
    });
    server3.invoke(() -> {
      createServerRegions(redundantCopies, 3, false);
      if (doPut) {
        cacheRule.getCache().getRegion(partitionedRegionName).put(KEY3, value + KEY3);
      }
    });
  }

  @Test
  public void memberCanRollbackTransactionWithOnReplicateRegionFunction() {
    setupServerRegions();
    server2.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region replicateRegion = cacheRule.getCache().getRegion(replicateRegionName);
      doPuts(replicateRegion);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
      txManager.rollback();
    });

    verifyReplicateRegionDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void accessorCanRollbackTransactionWithMultipleFunctions() {
    accessor.invoke(() -> createServerRegions(1, 3, true));
    setupServerRegions();
    accessor.invoke(() -> doPuts());

    accessor.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      Region replicateRegion = cacheRule.getCache().getRegion(replicateRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true);
      doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
      txManager.rollback();
    });

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void memberFunctionOnPartitionedRegionWithTransactionFailsIfWithoutFilter() {
    setupServerRegions();
    server3.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      doPuts(region);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      Throwable caughtException = catchThrowable(() -> doFunction(region,
          new MyPartitionRegionFunction(), Type.ON_REGION, false));
      assertThat(caughtException).isInstanceOf(TransactionException.class)
          .hasMessage("Function inside a transaction cannot execute on more than one node");
      txManager.rollback();
    });

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  @Test
  public void memberCanRollbackTransactionsWithMultipleColocatedKeysFilter() {
    setupServerRegions();
    server2.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      Region replicateRegion = cacheRule.getCache().getRegion(replicateRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true, true, true);
      doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
      txManager.rollback();
    });

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void memberFunctionWithTransactionsFailsIfWithMultipleNonColocatedKeysFilter() {
    setupServerRegions();
    server1.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      Throwable caughtException = catchThrowable(() -> doFunction(region,
          new MyPartitionRegionFunction(), Type.ON_REGION, true, true, false));
      assertThat(caughtException).isInstanceOf(TransactionException.class)
          .hasMessage("Function inside a transaction cannot execute on more than one node");

      txManager.rollback();
    });

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  @Test
  public void memberTransactionFailsWithNonColocatedFunctions() {
    setupServerRegions();
    server1.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      Region replicateRegion = cacheRule.getCache().getRegion(replicateRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true);
      doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);

      Throwable caughtException = catchThrowable(() -> doFunction2(region,
          new MyPartitionRegionFunction(), Type.ON_REGION));
      assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class)
          .hasMessageContaining("Function execution is not colocated with transaction.");

      txManager.rollback();
    });

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void transactionFailsIfIsOnMembersFunction() {
    setupServerRegions();
    server1.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      Throwable caughtException = catchThrowable(() -> doFunction(region,
          new MyPartitionRegionFunction(), Type.ON_MEMBER, false));
      assertThat(caughtException).isInstanceOf(TransactionException.class)
          .hasMessage("Function inside a transaction cannot execute on more than one node");

      txManager.rollback();
    });

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  @Test
  public void transactionFailsIfIsOnMultipleMembersFunction() {
    setupServerRegions();
    distributedMember = server1.invoke(() -> {
      return cacheRule.getCache().getDistributedSystem().getDistributedMember();
    });
    distributedMember2 = server2.invoke(() -> {
      return cacheRule.getCache().getDistributedSystem().getDistributedMember();
    });
    server1.invoke(() -> doPuts());

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      Throwable caughtException = catchThrowable(() -> doFunction(region,
          new MyPartitionRegionFunction(), Type.ON_MEMBER, false));
      assertThat(caughtException).isInstanceOf(TransactionException.class)
          .hasMessage("Function inside a transaction cannot execute on more than one node");

      txManager.rollback();
    });

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  @Test
  public void accessorCanRollbackTransactionWithOnMemberFunctions() {
    accessor.invoke(() -> createServerRegions(1, 3, true));
    setupServerRegions(true);
    distributedMember = server1.invoke(() -> {
      return cacheRule.getCache().getDistributedSystem().getDistributedMember();
    });
    server2.invoke(() -> doPuts());

    accessor.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(partitionedRegionName);
      CacheTransactionManager txManager =
          cacheRule.getCache().getCacheTransactionManager();
      txManager.begin();
      doFunction(region, new MyPartitionRegionFunction(), Type.ON_MEMBER, false);
      txManager.rollback();
    });

    verifyPartitionedRegionNotChangedOnServersAfterRollback();
  }

  @Test
  public void memberCanRollbackTransactionWithOnRegionAndOnMemberFunctions() {
    setupServerRegions(true);
    distributedMember = server1.invoke(() -> {
      return cacheRule.getCache().getDistributedSystem().getDistributedMember();
    });
    server2.invoke(() -> doPuts());

    server1.invoke(this::doOnRegionAndOnMemberFunctionThenRollback);

    verifyDataNotChangedOnServersAfterRollback();
  }

  @Test
  public void accessorCanRollbackTransactionWithOnRegionAndOnMemberFunctions() {
    accessor.invoke(() -> createServerRegions(1, 3, true));
    setupServerRegions(true);
    distributedMember = server1.invoke(() -> {
      return cacheRule.getCache().getDistributedSystem().getDistributedMember();
    });
    server2.invoke(() -> doPuts());

    accessor.invoke(this::doOnRegionAndOnMemberFunctionThenRollback);

    verifyDataNotChangedOnServersAfterRollback();
  }

  private void doOnRegionAndOnMemberFunctionThenRollback() {
    Region region = cacheRule.getCache().getRegion(partitionedRegionName);
    Region replicateRegion = null;
    replicateRegion = cacheRule.getCache().getRegion(replicateRegionName);
    CacheTransactionManager txManager =
        cacheRule.getCache().getCacheTransactionManager();
    txManager.begin();
    doFunction(region, new MyPartitionRegionFunction(), Type.ON_REGION, true);
    doFunction(replicateRegion, new MyReplicateRegionFunction(), Type.ON_REGION, false);
    doFunction(region, new MyPartitionRegionFunction(), Type.ON_MEMBER, false);
    txManager.rollback();
  }

  private int createServerRegions(int redundantCopies, int totalNumOfBuckets,
      boolean isAccessor) throws Exception {
    PartitionAttributesFactory factory = new PartitionAttributesFactory();
    factory.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumOfBuckets);
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    }

    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(factory.create()).create(partitionedRegionName);
    cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE)
        .create(replicateRegionName);
    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegions(boolean singleHopEnabled, int... ports) {
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
    crf.create(partitionedRegionName);
    crf.create(replicateRegionName);

    if (ports.length > 1) {
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

  private void doPuts() {
    doPuts(cacheRule.getCache().getRegion(partitionedRegionName));
    doPuts(cacheRule.getCache().getRegion(replicateRegionName));
  }

  private void doPuts(Region region) {
    region.put(KEY1, value + KEY1);
    region.put(KEY2, value + KEY2);
    region.put(KEY3, value + KEY3);
    region.put(KEY4, value + KEY4);
  }

  private void verifyData() {
    verifyData(cacheRule.getCache().getRegion(partitionedRegionName));
    verifyData(cacheRule.getCache().getRegion(replicateRegionName));
  }

  private void verifyData(Region region) {
    assertThat(region.get(KEY1)).isEqualTo(value + KEY1);
    assertThat(region.get(KEY2)).isEqualTo(value + KEY2);
    assertThat(region.get(KEY3)).isEqualTo(value + KEY3);
    assertThat(region.get(KEY4)).isEqualTo(value + KEY4);
  }

  private void doFunction(Region region, Function function, Type type, boolean withFilter) {
    doFunction(region, function, type, withFilter, false);
  }

  private void doFunction(Region region, Function function, Type type, boolean withFilter,
      boolean withMultipleKeys) {
    doFunction(region, function, type, withFilter, withMultipleKeys, false);
  }

  private void doFunction(Region region, Function function, Type type, boolean withFilter,
      boolean withMultipleKeys, boolean areColocatedKeys) {
    Execution execution;
    Set keySet = new HashSet();
    keySet.add(KEY1);
    if (withMultipleKeys) {
      if (areColocatedKeys) {
        keySet.add(KEY4);
      } else {
        keySet.add(KEY2);
      }
    }
    switch (type) {
      case ON_MEMBER:
        if (distributedMember != null) {
          if (distributedMember2 != null) {
            Set membersSet = new HashSet();
            membersSet.add(distributedMember);
            membersSet.add(distributedMember2);
            execution = FunctionService.onMembers(membersSet);
          } else {
            execution = FunctionService.onMember(distributedMember).setArguments(
                partitionedRegionName);
          }
        } else {
          execution = FunctionService.onMembers();
        }
        break;
      case ON_REGION:
        execution = FunctionService.onRegion(region);
        if (withFilter) {
          execution = execution.withFilter(keySet);
        }
        break;
      default:
        throw new RuntimeException("unexpected type");
    }

    ResultCollector resultCollector = execution.execute(function);
    resultCollector.getResult();
  }

  private void doFunction2(Region region, Function function, Type type) {
    Execution execution;
    Set keySet = new HashSet();
    keySet.add(KEY2);
    switch (type) {
      case ON_MEMBER:
        execution = FunctionService.onMembers();
        break;
      case ON_REGION:
        execution = FunctionService.onRegion(region);
        execution = execution.withFilter(keySet);
        break;
      default:
        throw new RuntimeException("unexpected type");
    }

    ResultCollector resultCollector = execution.execute(function);
    resultCollector.getResult();
  }

  public static class MyPartitionRegionFunction implements Function, DataSerializable {
    @Override
    public void execute(FunctionContext context) {
      if (context instanceof RegionFunctionContext) {
        PartitionedRegion region =
            (PartitionedRegion) ((RegionFunctionContext) context).getDataSet();
        Set keySet = ((RegionFunctionContext) context).getFilter();
        for (final Object key : keySet) {
          region.destroy(key);
        }
      } else if (context instanceof FunctionContextImpl) {
        String regionName = context.getArguments().toString();
        Region region = context.getCache().getRegion(regionName);
        region.destroy(KEY4);
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }
  }

  public static class MyReplicateRegionFunction implements Function, DataSerializable {
    @Override
    public void execute(FunctionContext context) {
      if (context instanceof RegionFunctionContext) {
        Region region = ((RegionFunctionContext) context).getDataSet();
        region.destroy(KEY2);
        region.destroy(KEY3);
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
