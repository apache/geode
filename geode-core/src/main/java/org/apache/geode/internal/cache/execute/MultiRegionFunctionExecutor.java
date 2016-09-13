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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionDataNotColocatedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * 
 * 
 */
public class MultiRegionFunctionExecutor extends AbstractExecution {

  private final Set<Region> regions;

  private ServerToClientFunctionResultSender sender;

  public MultiRegionFunctionExecutor(Set<Region> regions) {
    this.regions = regions;
  }

  private MultiRegionFunctionExecutor(MultiRegionFunctionExecutor drfe) {
    super(drfe);
    this.regions = drfe.regions;
    if (drfe.filter != null) {
      this.filter.clear();
      this.filter.addAll(drfe.filter);
    }
    this.sender = drfe.sender;
  }

  private MultiRegionFunctionExecutor(Set<Region> regions, Set filter2,
      Object args, MemberMappedArgument memberMappedArg,
      ServerToClientFunctionResultSender resultSender) {
    if (args != null) {
      this.args = args;
    }
    else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      this.isMemberMappedArgument = true;
    }
    this.sender = resultSender;
    if (filter2 != null) {
      this.filter.clear();
      this.filter.addAll(filter2);
    }
    this.regions = regions;
    this.isClientServerMode = true;
  }

  private MultiRegionFunctionExecutor(MultiRegionFunctionExecutor executor,
      MemberMappedArgument argument) {
    super(executor);
    this.regions = executor.getRegions();
    this.filter.clear();
    this.filter.addAll(executor.filter);
    this.sender = executor.getServerResultSender();
    this.memberMappedArg = argument;
    this.isMemberMappedArgument = true;

  }

  private MultiRegionFunctionExecutor(MultiRegionFunctionExecutor executor,
      ResultCollector rs) {
    super(executor);
    this.regions = executor.getRegions();
    this.filter.clear();
    this.filter.addAll(executor.filter);
    this.sender = executor.getServerResultSender();
    this.rc = rs;
  }

  public MultiRegionFunctionExecutor(MultiRegionFunctionExecutor executor,
      Object args) {
    super(executor);
    this.regions = executor.getRegions();
    this.filter.clear();
    this.filter.addAll(executor.filter);
    this.sender = executor.getServerResultSender();

    this.args = args;
  }
  
  public MultiRegionFunctionExecutor(MultiRegionFunctionExecutor executor,
      boolean isReExecute) {
    super(executor);
    this.regions = executor.getRegions();
    this.filter.clear();
    this.filter.addAll(executor.filter);
    this.sender = executor.getServerResultSender();

    this.isReExecute = isReExecute;
  }
  
  public InternalExecution withMemberMappedArgument(
      MemberMappedArgument argument) {
    if (argument == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("MemberMapped Arg"));
    }
    return new MultiRegionFunctionExecutor(this, argument);
  }

  public Set<Region> getRegions() {
    return this.regions;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return this.sender;
  }

  public Execution withArgs(Object args) {
    if (args == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("args"));
    }
    return new MultiRegionFunctionExecutor(this, args);
  }

  public Execution withCollector(ResultCollector<?, ?> rc) {
    if (rc == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Result Collector"));
    }
    return new MultiRegionFunctionExecutor(this, rc);
  }

  public Execution withFilter(Set<?> filter) {
    throw new FunctionException(
        LocalizedStrings.ExecuteFunction_CANNOT_SPECIFY_0_FOR_ONREGIONS_FUNCTION
            .toLocalizedString("filter"));
  }
  
  @Override
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    throw new FunctionException(
        LocalizedStrings.ExecuteFunction_CANNOT_SPECIFY_0_FOR_ONREGIONS_FUNCTION
            .toLocalizedString("bucket as filter"));
  }

  @Override
  protected ResultCollector executeFunction(Function function) {
    if (function.hasResult()) {
      ResultCollector rc = this.rc;
      if (rc == null) {
        rc = new DefaultResultCollector();
      }
      return executeFunction(function, rc);
    }
    else {
      executeFunction(function, null);
      return new NoResult();
    }
  }

  private ResultCollector executeFunction(final Function function,
      ResultCollector resultCollector) {
    InternalDistributedSystem ds = InternalDistributedSystem
        .getConnectedInstance();
    if (ds == null) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_DS_NOT_CREATED_OR_NOT_READY
              .toLocalizedString());
    }
    final DM dm = ds.getDistributionManager();
    final Map<InternalDistributedMember, Set<String>> memberToRegionMap = calculateMemberToRegionMap();
    final Set<InternalDistributedMember> dest = new HashSet<InternalDistributedMember>(
        memberToRegionMap.keySet());

    if (dest.isEmpty()) {
      throw new FunctionException(
          LocalizedStrings.MemberFunctionExecutor_NO_MEMBER_FOUND_FOR_EXECUTING_FUNCTION_0
              .toLocalizedString(function.getId()));
    }
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (function.optimizeForWrite() && cache != null
        && cache.getResourceManager().getHeapMonitor().containsHeapCriticalMembers(dest)
        && !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      Set<InternalDistributedMember> hcm = cache.getResourceAdvisor()
          .adviseCritialMembers();
      Set<DistributedMember> sm = SetUtils.intersection(hcm, dest);
      throw new LowMemoryException(
          LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1
              .toLocalizedString(new Object[] { function.getId(), sm }), sm);
    }
    setExecutionNodes(dest);

    final InternalDistributedMember localVM = cache.getMyId();
    final LocalResultCollector<?, ?> localResultCollector = getLocalResultCollector(
        function, resultCollector);
    boolean remoteOnly = false;
    boolean localOnly = false;
    if (!dest.contains(localVM)) {
      remoteOnly = true;
    }
    if (dest.size() == 1 && dest.contains(localVM)) {
      localOnly = true;
    }
    validateExecution(function, dest);
    final MemberFunctionResultSender resultSender = new MemberFunctionResultSender(
        dm, localResultCollector, function, localOnly, remoteOnly, null);
    if (dest.contains(localVM)) {
      // if member is local VM
      dest.remove(localVM);
      Set<String> regionPathSet = memberToRegionMap.get(localVM);
      Set<Region> regions = new HashSet<Region>();
      if (regionPathSet != null) {
        Cache cache1 = GemFireCacheImpl.getInstance();
        for (String regionPath : regionPathSet) {
          regions.add(cache1.getRegion(regionPath));
        }
      }
      final FunctionContextImpl context = new MultiRegionFunctionContextImpl(
          function.getId(), getArgumentsForMember(localVM.getId()),
          resultSender, regions, this.isReExecute);
      boolean isTx = cache.getTxManager().getTXState() == null ? false : true;
      executeFunctionOnLocalNode(function, context, resultSender, dm, isTx);
    }
    if (!dest.isEmpty()) {
      HashMap<InternalDistributedMember, Object> memberArgs = new HashMap<InternalDistributedMember, Object>();
      for (InternalDistributedMember recip : dest) {
        memberArgs.put(recip, getArgumentsForMember(recip.getId()));
      }
      Assert.assertTrue(memberArgs.size() == dest.size());
      MultiRegionFunctionResultWaiter waiter = new MultiRegionFunctionResultWaiter(
          ds, localResultCollector, function, dest, memberArgs, resultSender,
          memberToRegionMap);

      ResultCollector reply = waiter
          .getFunctionResultFrom(dest, function, this);
      return reply;
    }
    return localResultCollector;
  }

  private Map<InternalDistributedMember, Set<String>> calculateMemberToRegionMap() {
    Map<InternalDistributedMember, Set<String>> memberToRegions = new HashMap<InternalDistributedMember, Set<String>>();
    // nodes is maintained for node pruning logic
    Set<InternalDistributedMember> nodes = new HashSet<InternalDistributedMember>();
    for (Region region : regions) {
      DataPolicy dp = region.getAttributes().getDataPolicy();
      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)region;
        Set<InternalDistributedMember> prMembers = pr.getRegionAdvisor()
            .advisePrimaryOwners();
        if (pr.isDataStore()) {
          GemFireCacheImpl cache = (GemFireCacheImpl)region.getCache();
          // Add local node
          InternalDistributedMember localVm = cache.getMyId();
          Set<String> regions = memberToRegions.get(localVm);
          if (regions == null) {
            regions = new HashSet<String>();
          }
          regions.add(pr.getFullPath());
          memberToRegions.put(localVm, regions);
        }
        if (prMembers != null) {
          for (InternalDistributedMember member : prMembers) {
            Set<String> regions = memberToRegions.get(member);
            if (regions == null) {
              regions = new HashSet<String>();
            }
            regions.add(pr.getFullPath());
            memberToRegions.put(member, regions);
          }
          nodes.addAll(prMembers);
        }
      }
      else if (region instanceof DistributedRegion) {
        if (dp.isEmpty() || dp.isNormal()) {
          // Add local members
          DistributedRegion dr = (DistributedRegion)region;
          Set<InternalDistributedMember> replicates = dr
              .getCacheDistributionAdvisor().adviseInitializedReplicates();
          // if existing nodes contain one of the nodes from replicates
          boolean added = false;
          for (InternalDistributedMember member : replicates) {
            if (nodes.contains(member)) {
              added = true;
              Set<String> regions = memberToRegions.get(member);
              if (regions == null) {
                regions = new HashSet<String>();
              }
              regions.add(dr.getFullPath());
              memberToRegions.put(member, regions);
              break;
            }
          }
          // if existing nodes set is mutually exclusive to replicates
          if (replicates.size() != 0 && !added) {
            // select a random replicate
            InternalDistributedMember member = (InternalDistributedMember)(replicates
                .toArray()[new Random().nextInt(replicates.size())]);
            Set<String> regions = memberToRegions.get(member);
            if (regions == null) {
              regions = new HashSet<String>();
            }
            regions.add(dr.getFullPath());
            memberToRegions.put(member, regions);
          }
        }
        else if (dp.withReplication()) {
          GemFireCacheImpl cache = (GemFireCacheImpl)region.getCache();
          // Add local node
          InternalDistributedMember local = cache.getMyId();
          Set<String> regions = memberToRegions.get(local);
          if (regions == null) {
            regions = new HashSet<String>();
          }
          regions.add(region.getFullPath());
          memberToRegions.put(local, regions);
        }
      }
      else if (region instanceof LocalRegion) {
        GemFireCacheImpl cache = (GemFireCacheImpl)region.getCache();
        // Add local node
        InternalDistributedMember local = cache.getMyId();
        Set<String> regions = memberToRegions.get(local);
        if (regions == null) {
          regions = new HashSet<String>();
        }
        regions.add(region.getFullPath());
        memberToRegions.put(local, regions);
      }
    }
    return memberToRegions;
  }
  
  @Override
  public AbstractExecution setIsReExecute() {
    return new MultiRegionFunctionExecutor(this, true);
  }
  @Override
  public void validateExecution(Function function, Set targetMembers) {
    GemFireCacheImpl cache = null;
    for (Region r : regions) {
      cache = (GemFireCacheImpl)r.getCache();
      break;
    }
    if (cache != null && cache.getTxManager().getTXState() != null) {
      if (targetMembers.size() > 1) {
        throw new TransactionException(LocalizedStrings.PartitionedRegion_TX_FUNCTION_ON_MORE_THAN_ONE_NODE
            .toLocalizedString());
      } else {
        assert targetMembers.size() == 1;
        DistributedMember funcTarget = (DistributedMember)targetMembers.iterator().next();
        DistributedMember target = cache.getTxManager().getTXState().getTarget();
        if (target == null) {
          cache.getTxManager().getTXState().setTarget(funcTarget);
        } else if (!target.equals(funcTarget)) {
          throw new TransactionDataNotColocatedException(LocalizedStrings.PartitionedRegion_TX_FUNCTION_EXECUTION_NOT_COLOCATED_0_1
              .toLocalizedString(new Object[] {target,funcTarget}));
        }
      }
    }
    if (function.optimizeForWrite() && cache.getResourceManager().getHeapMonitor().
        containsHeapCriticalMembers(targetMembers) &&
        !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      Set<InternalDistributedMember> hcm  = cache.getResourceAdvisor().adviseCritialMembers();
      Set<DistributedMember> sm = SetUtils.intersection(hcm, targetMembers);
      throw new LowMemoryException(LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1.toLocalizedString(
          new Object[] {function.getId(), sm}), sm);
    }
  }
}
