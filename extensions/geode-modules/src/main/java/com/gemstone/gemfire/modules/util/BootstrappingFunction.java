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
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

import java.util.List;
import java.util.Set;

public class BootstrappingFunction implements Function, MembershipListener {

  private static final long serialVersionUID = 1856043174458190605L;

  public static final String ID = "bootstrapping-function";

  private static final int TIME_TO_WAIT_FOR_CACHE = Integer.getInteger("gemfiremodules.timeToWaitForCache", 30000);

  @Override
  public void execute(FunctionContext context) {
    // Verify that the cache exists before continuing.
    // When this function is executed by a remote membership listener, it is
    // being invoked before the cache is started.
    Cache cache = verifyCacheExists();

    // Register as membership listener
    registerAsMembershipListener(cache);

    // Register functions
    registerFunctions();

    // Return status
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  private Cache verifyCacheExists() {
    int timeToWait = 0;
    Cache cache = null;
    while (timeToWait < TIME_TO_WAIT_FOR_CACHE) {
      try {
        cache = CacheFactory.getAnyInstance();
        break;
      } catch (Exception ignore) {
        //keep trying and hope for the best
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

  private void registerAsMembershipListener(Cache cache) {
    DM dm = ((InternalDistributedSystem) cache.getDistributedSystem()).getDistributionManager();
    dm.addMembershipListener(this);
  }

  private void registerFunctions() {
    // Synchronize so that these functions aren't registered twice. The
    // constructor for the CreateRegionFunction creates a meta region.
    synchronized (ID) {
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
    }
  }

  private void bootstrapMember(InternalDistributedMember member) {
    // Create and execute the function
    Cache cache = CacheFactory.getAnyInstance();
    Execution execution = FunctionService.onMember(cache.getDistributedSystem(), member);
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

    if (obj == null || !(obj instanceof BootstrappingFunction)) {
      return false;
    }

    return true;
  }

  @Override
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
  }

  @Override
  public void memberJoined(InternalDistributedMember id) {
    bootstrapMember(id);
  }

  @Override
  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected, String reason) {
  }

  @Override
  public void quorumLost(Set<InternalDistributedMember> internalDistributedMembers,
      List<InternalDistributedMember> internalDistributedMembers2) {
  }
}
