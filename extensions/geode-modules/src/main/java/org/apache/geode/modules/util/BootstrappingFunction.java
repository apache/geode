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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

public class BootstrappingFunction implements Function, MembershipListener, DataSerializable {

  private static final long serialVersionUID = 1856043174458190605L;

  public static final String ID = "bootstrapping-function";
  private static final ReentrantLock registerFunctionLock = new ReentrantLock();

  private static final int TIME_TO_WAIT_FOR_CACHE =
      Integer.getInteger("gemfiremodules.timeToWaitForCache", 300000);

  public BootstrappingFunction() {}

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

  protected boolean isLocator(Cache cache) {
    DistributedSystem system = cache.getDistributedSystem();
    InternalDistributedMember member = (InternalDistributedMember) system.getDistributedMember();
    return member.getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE;
  }

  protected Cache verifyCacheExists() {
    int timeToWait = 0;
    Cache cache = null;
    while (timeToWait < TIME_TO_WAIT_FOR_CACHE) {
      try {
        cache = CacheFactory.getAnyInstance();
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
      cache = new CacheFactory().create();
    }

    return cache;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(ResourcePermissions.CLUSTER_MANAGE);
  }

  private void registerAsMembershipListener(Cache cache) {
    DistributionManager dm =
        ((InternalDistributedSystem) cache.getDistributedSystem()).getDistributionManager();
    dm.addMembershipListener(this);
  }

  protected void registerFunctions() {
    // Synchronize so that these functions aren't registered twice. The
    // constructor for the CreateRegionFunction creates a meta region.
    registerFunctionLock.lock();
    try {
      // Register the create region function if it is not already registered
      if (!FunctionService.isRegistered(CreateRegionFunction.ID)) {
        FunctionService.registerFunction(new CreateRegionFunction());
      }

      // Register the touch partitioned region entries function if it is not already registered
      if (!FunctionService.isRegistered(TouchPartitionedRegionEntriesFunction.ID)) {
        FunctionService.registerFunction(new TouchPartitionedRegionEntriesFunction());
      }

      // Register the touch replicated region entries function if it is not already registered
      if (!FunctionService.isRegistered(TouchReplicatedRegionEntriesFunction.ID)) {
        FunctionService.registerFunction(new TouchReplicatedRegionEntriesFunction());
      }

      // Register the region size function if it is not already registered
      if (!FunctionService.isRegistered(RegionSizeFunction.ID)) {
        FunctionService.registerFunction(new RegionSizeFunction());
      }
    } finally {
      registerFunctionLock.unlock();
    }
  }

  private void bootstrapMember(InternalDistributedMember member) {
    // Create and execute the function
    Cache cache = CacheFactory.getAnyInstance();
    Execution execution = FunctionService.onMember(member);
    ResultCollector collector = execution.execute(this);

    // Get the result. Nothing is being done with it.
    try {
      collector.getResult();
    } catch (Exception e) {
      // If an exception occurs in the function, log it.
      cache.getLogger().warning("Caught unexpected exception:", e);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  public int hashCode() {
    // This method is only implemented so that multiple instances of this class
    // don't get added as membership listeners.
    return ID.hashCode();
  }

  public boolean equals(Object obj) {
    // This method is only implemented so that multiple instances of this class
    // don't get added as membership listeners.
    if (this == obj) {
      return true;
    }
    return obj instanceof BootstrappingFunction;
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {}

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    bootstrapMember(id);
  }

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> internalDistributedMembers,
      List<InternalDistributedMember> internalDistributedMembers2) {}

  @Override
  public void toData(DataOutput out) throws IOException {}

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {}
}
