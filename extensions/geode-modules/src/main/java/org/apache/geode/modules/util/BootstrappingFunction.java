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
package org.apache.geode.modules.util;

import static java.util.Collections.singletonList;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

public class BootstrappingFunction implements Function, MembershipListener, DataSerializable {
  private static final Logger logger = LogService.getLogger();

  private static final String ID = "bootstrapping-function";
  private static final int TIME_TO_WAIT_FOR_CACHE =
      Integer.getInteger("gemfiremodules.timeToWaitForCache", 30000);

  private static final Lock registerFunctionLock = new ReentrantLock();

  private final AtomicReference<java.util.function.Function<String, Boolean>> isFunctionRegistered =
      new AtomicReference<>(functionId -> FunctionService.isRegistered(functionId));
  private final AtomicReference<Consumer<Function<?>>> registerFunction =
      new AtomicReference<>(function -> FunctionService.registerFunction(function));
  private final AtomicReference<java.util.function.Function<DistributedMember, Execution<?, ?, ?>>> onMember =
      new AtomicReference<>(member -> FunctionService.onMember(member));

  private final AtomicReference<Supplier<Cache>> cacheSingleton =
      new AtomicReference<>(() -> CacheFactory.getAnyInstance());
  private final AtomicReference<Supplier<Cache>> cacheFactory =
      new AtomicReference<>(() -> new CacheFactory().create());

  private final AtomicReference<Supplier<CreateRegionFunction>> createRegionFunctionFactory =
      new AtomicReference<>(() -> new CreateRegionFunction());
  private final AtomicReference<Supplier<TouchPartitionedRegionEntriesFunction>> touchPartitionedRegionEntriesFunctionFactory =
      new AtomicReference<>(() -> new TouchPartitionedRegionEntriesFunction());
  private final AtomicReference<Supplier<TouchReplicatedRegionEntriesFunction>> touchReplicatedRegionEntriesFunctionFactory =
      new AtomicReference<>(() -> new TouchReplicatedRegionEntriesFunction());
  private final AtomicReference<Supplier<RegionSizeFunction>> regionSizeFunctionFactory =
      new AtomicReference<>(() -> new RegionSizeFunction());

  public BootstrappingFunction() {
    this(// FunctionService functions
        functionId -> FunctionService.isRegistered(functionId),
        function -> FunctionService.registerFunction(function),
        member -> FunctionService.onMember(member),
        // Cache singleton and factory
        () -> CacheFactory.getAnyInstance(),
        () -> new CacheFactory().create(),
        // Function factories
        () -> new CreateRegionFunction(),
        () -> new TouchPartitionedRegionEntriesFunction(),
        () -> new TouchReplicatedRegionEntriesFunction(),
        () -> new RegionSizeFunction());
  }

  @VisibleForTesting
  BootstrappingFunction(java.util.function.Function<String, Boolean> isFunctionRegistered,
      Consumer<Function<?>> registerFunction,
      java.util.function.Function<DistributedMember, Execution<?, ?, ?>> onMember,
      Supplier<Cache> cacheSingleton,
      Supplier<Cache> cacheFactory,
      Supplier<CreateRegionFunction> createRegionFunctionFactory,
      Supplier<TouchPartitionedRegionEntriesFunction> touchPartitionedRegionEntriesFunctionFactory,
      Supplier<TouchReplicatedRegionEntriesFunction> touchReplicatedRegionEntriesFunctionFactory,
      Supplier<RegionSizeFunction> regionSizeFunctionFactory) {
    this.isFunctionRegistered.set(isFunctionRegistered);
    this.registerFunction.set(registerFunction);
    this.onMember.set(onMember);
    this.cacheSingleton.set(cacheSingleton);
    this.cacheFactory.set(cacheFactory);
    this.createRegionFunctionFactory.set(createRegionFunctionFactory);
    this.touchPartitionedRegionEntriesFunctionFactory
        .set(touchPartitionedRegionEntriesFunctionFactory);
    this.touchReplicatedRegionEntriesFunctionFactory
        .set(touchReplicatedRegionEntriesFunctionFactory);
    this.regionSizeFunctionFactory.set(regionSizeFunctionFactory);
  }

  @Override
  public void execute(FunctionContext context) {
    // Verify that the cache exists before continuing.
    // When this function is executed by a remote membership listener, it is
    // being invoked before the cache is started.
    Cache cache = verifyCacheExists();

    // Register as membership listener
    registerAsMembershipListener(cache);

    if (!isLocator(cache)) {
      // Register functions
      registerFunctions();
    }

    // Return status
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  @VisibleForTesting
  boolean isLocator(Cache cache) {
    DistributedSystem system = cache.getDistributedSystem();
    InternalDistributedMember member = (InternalDistributedMember) system.getDistributedMember();
    return member.getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE;
  }

  @VisibleForTesting
  Cache verifyCacheExists() {
    int timeToWait = 0;
    Cache cache = null;
    while (timeToWait < TIME_TO_WAIT_FOR_CACHE) {
      try {
        cache = cacheSingleton.get().get();
        break;
      } catch (Exception ignore) {
        // keep trying and hope for the best
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
      timeToWait += 250;
    }

    if (cache == null) {
      cache = cacheFactory.get().get();
    }

    return cache;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return singletonList(ResourcePermissions.CLUSTER_MANAGE);
  }

  private void registerAsMembershipListener(Cache cache) {
    DistributionManager distributionManager =
        ((InternalDistributedSystem) cache.getDistributedSystem()).getDistributionManager();
    distributionManager.addMembershipListener(this);
  }

  @VisibleForTesting
  void registerFunctions() {
    // Synchronize so that these functions aren't registered twice. The
    // constructor for the CreateRegionFunction creates a meta region.
    registerFunctionLock.lock();
    try {
      // Register the create region function if it is not already registered
      if (!isFunctionRegistered.get().apply(CreateRegionFunction.ID)) {
        registerFunction.get().accept(createRegionFunctionFactory.get().get());
      }

      // Register the touch partitioned region entries function if it is not already registered
      if (!isFunctionRegistered.get().apply(TouchPartitionedRegionEntriesFunction.ID)) {
        registerFunction.get().accept(touchPartitionedRegionEntriesFunctionFactory.get().get());
      }

      // Register the touch replicated region entries function if it is not already registered
      if (!isFunctionRegistered.get().apply(TouchReplicatedRegionEntriesFunction.ID)) {
        registerFunction.get().accept(touchReplicatedRegionEntriesFunctionFactory.get().get());
      }

      // Register the region size function if it is not already registered
      if (!isFunctionRegistered.get().apply(RegionSizeFunction.ID)) {
        registerFunction.get().accept(regionSizeFunctionFactory.get().get());
      }
    } finally {
      registerFunctionLock.unlock();
    }
  }

  private void bootstrapMember(DistributedMember member) {
    // Create and execute the function
    Execution execution = onMember.get().apply(member);
    ResultCollector collector = execution.execute(this);

    // Get the result. Nothing is being done with it.
    try {
      collector.getResult();
    } catch (Exception e) {
      // If an exception occurs in the function, log it.
      logger.warn("Caught unexpected exception:", e);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public int hashCode() {
    // This method is only implemented so that multiple instances of this class
    // don't get added as membership listeners.
    return ID.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // This method is only implemented so that multiple instances of this class
    // don't get added as membership listeners.
    if (this == obj) {
      return true;
    }
    return obj instanceof BootstrappingFunction;
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    bootstrapMember(id);
  }

  @Override
  public void toData(DataOutput out) {
    // nothing
  }

  @Override
  public void fromData(DataInput in) {
    // nothing
  }

  private static final long serialVersionUID = 1856043174458190605L;
}
