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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;

public class MultiRegionFunctionExecutor<IN, OUT, AGG> extends AbstractExecution<IN, OUT, AGG> {

  private final Set<Region<?, ?>> regions;

  private ServerToClientFunctionResultSender sender;

  public MultiRegionFunctionExecutor(Set<Region<?, ?>> regions) {
    this.regions = regions;
  }

  private MultiRegionFunctionExecutor(MultiRegionFunctionExecutor<IN, OUT, AGG> executor,
      MemberMappedArgument argument) {
    super(executor);
    regions = executor.getRegions();
    filter.clear();
    filter.addAll(executor.filter);
    sender = executor.getServerResultSender();
    memberMappedArg = argument;
    isMemberMappedArgument = true;

  }

  private MultiRegionFunctionExecutor(MultiRegionFunctionExecutor<IN, OUT, AGG> executor,
      ResultCollector<OUT, AGG> rs) {
    super(executor);
    regions = executor.getRegions();
    filter.clear();
    filter.addAll(executor.filter);
    sender = executor.getServerResultSender();
    rc = rs;
  }

  public MultiRegionFunctionExecutor(MultiRegionFunctionExecutor<IN, OUT, AGG> executor, IN args) {
    super(executor);
    regions = executor.getRegions();
    filter.clear();
    filter.addAll(executor.filter);
    sender = executor.getServerResultSender();

    this.args = args;
  }

  public MultiRegionFunctionExecutor(MultiRegionFunctionExecutor<IN, OUT, AGG> executor,
      boolean isReExecute) {
    super(executor);
    regions = executor.getRegions();
    filter.clear();
    filter.addAll(executor.filter);
    sender = executor.getServerResultSender();

    this.isReExecute = isReExecute;
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "MemberMapped Arg"));
    }
    return new MultiRegionFunctionExecutor<>(this, argument);
  }

  public Set<Region<?, ?>> getRegions() {
    return regions;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return sender;
  }

  @Override
  public Execution<IN, OUT, AGG> setArguments(IN args) {
    if (args == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "args"));
    }
    return new MultiRegionFunctionExecutor<>(this, args);
  }

  @Override
  public Execution<IN, OUT, AGG> withArgs(IN args) {
    return setArguments(args);
  }

  @Override
  public Execution<IN, OUT, AGG> withCollector(ResultCollector<OUT, AGG> rc) {
    if (rc == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Result Collector"));
    }
    return new MultiRegionFunctionExecutor<>(this, rc);
  }

  @Override
  public Execution<IN, OUT, AGG> withFilter(Set<?> filter) {
    throw new FunctionException(
        String.format("Cannot specify %s for multi region function",
            "filter"));
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withBucketFilter(Set<Integer> bucketIDs) {
    throw new FunctionException(
        String.format("Cannot specify %s for multi region function",
            "bucket as filter"));
  }

  @Override
  protected ResultCollector<OUT, AGG> executeFunction(Function<IN> function, long timeout,
      TimeUnit unit) {
    if (!function.hasResult()) {
      executeFunction(function, null);
      return new NoResult<>();
    }
    ResultCollector<OUT, AGG> inRc =
        (rc == null) ? uncheckedCast(new DefaultResultCollector<>()) : rc;
    ResultCollector<OUT, AGG> rcToReturn = executeFunction(function, inRc);
    if (timeout > 0) {
      try {
        rcToReturn.getResult(timeout, unit);
      } catch (Exception e) {
        throw new FunctionException(e);
      }
    }
    return rcToReturn;
  }

  private ResultCollector<OUT, AGG> executeFunction(final Function<IN> function,
      ResultCollector<OUT, AGG> resultCollector) {
    InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds == null) {
      throw new IllegalStateException(
          "DistributedSystem is either not created or not ready");
    }
    final DistributionManager dm = ds.getDistributionManager();
    final Map<InternalDistributedMember, Set<String>> memberToRegionMap =
        calculateMemberToRegionMap();
    final Set<InternalDistributedMember> dest =
        new HashSet<>(memberToRegionMap.keySet());

    if (dest.isEmpty()) {
      throw new FunctionException(
          String.format("No member found for executing function : %s.",
              function.getId()));
    }
    final InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(function,
          Collections.unmodifiableSet(dest));
    }
    setExecutionNodes(dest);

    final InternalDistributedMember localVM = cache.getMyId();
    final LocalResultCollector<OUT, AGG> localResultCollector =
        getLocalResultCollector(function, resultCollector);
    boolean remoteOnly = false;
    boolean localOnly = false;
    if (!dest.contains(localVM)) {
      remoteOnly = true;
    }
    if (dest.size() == 1 && dest.contains(localVM)) {
      localOnly = true;
    }
    validateExecution(function, dest);
    final MemberFunctionResultSender resultSender = new MemberFunctionResultSender(dm,
        localResultCollector, function, localOnly, remoteOnly, null);
    if (dest.contains(localVM)) {
      // if member is local VM
      dest.remove(localVM);
      Set<String> regionPathSet = memberToRegionMap.get(localVM);
      Set<Region<?, ?>> regions = new HashSet<>();
      if (regionPathSet != null) {
        InternalCache cache1 = GemFireCacheImpl.getInstance();
        for (String regionPath : regionPathSet) {
          regions.add(cache1.getRegion(regionPath));
        }
      }
      final FunctionContextImpl<IN> context =
          new MultiRegionFunctionContextImpl<>(cache, function.getId(),
              getArgumentsForMember(localVM.getId()), resultSender, regions, isReExecute);
      boolean isTx = cache.getTxManager().getTXState() != null;
      executeFunctionOnLocalNode(function, context, resultSender, dm, isTx);
    }
    if (!dest.isEmpty()) {
      HashMap<InternalDistributedMember, Object> memberArgs =
          new HashMap<>();
      for (InternalDistributedMember recip : dest) {
        memberArgs.put(recip, getArgumentsForMember(recip.getId()));
      }
      Assert.assertTrue(memberArgs.size() == dest.size());
      MultiRegionFunctionResultWaiter waiter = new MultiRegionFunctionResultWaiter(ds,
          localResultCollector, function, dest, memberArgs, resultSender, memberToRegionMap);

      return waiter.getFunctionResultFrom(dest, function, this);
    }
    return localResultCollector;
  }

  private Map<InternalDistributedMember, Set<String>> calculateMemberToRegionMap() {
    Map<InternalDistributedMember, Set<String>> memberToRegions = new HashMap<>();
    // nodes is maintained for node pruning logic
    Set<InternalDistributedMember> nodes = new HashSet<>();
    for (Region<?, ?> region : regions) {
      DataPolicy dp = region.getAttributes().getDataPolicy();
      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) region;
        Set<InternalDistributedMember> prMembers = pr.getRegionAdvisor().advisePrimaryOwners();
        if (pr.isDataStore()) {
          InternalCache cache = (InternalCache) region.getCache();
          // Add local node
          InternalDistributedMember localVm = cache.getMyId();
          Set<String> regions = memberToRegions.get(localVm);
          if (regions == null) {
            regions = new HashSet<>();
          }
          regions.add(pr.getFullPath());
          memberToRegions.put(localVm, regions);
        }
        if (prMembers != null) {
          for (InternalDistributedMember member : prMembers) {
            Set<String> regions = memberToRegions.get(member);
            if (regions == null) {
              regions = new HashSet<>();
            }
            regions.add(pr.getFullPath());
            memberToRegions.put(member, regions);
          }
          nodes.addAll(prMembers);
        }
      } else if (region instanceof DistributedRegion) {
        if (dp.isEmpty() || dp.isNormal()) {
          // Add local members
          DistributedRegion dr = (DistributedRegion) region;
          Set<InternalDistributedMember> replicates =
              dr.getCacheDistributionAdvisor().adviseInitializedReplicates();
          // if existing nodes contain one of the nodes from replicates
          boolean added = false;
          for (InternalDistributedMember member : replicates) {
            if (nodes.contains(member)) {
              added = true;
              Set<String> regions = memberToRegions.get(member);
              if (regions == null) {
                regions = new HashSet<>();
              }
              regions.add(dr.getFullPath());
              memberToRegions.put(member, regions);
              break;
            }
          }
          // if existing nodes set is mutually exclusive to replicates
          if (replicates.size() != 0 && !added) {
            // select a random replicate
            InternalDistributedMember member = (InternalDistributedMember) (replicates
                .toArray()[new Random().nextInt(replicates.size())]);
            Set<String> regions = memberToRegions.get(member);
            if (regions == null) {
              regions = new HashSet<>();
            }
            regions.add(dr.getFullPath());
            memberToRegions.put(member, regions);
          }
        } else if (dp.withReplication()) {
          InternalCache cache = (InternalCache) region.getCache();
          // Add local node
          InternalDistributedMember local = cache.getMyId();
          Set<String> regions = memberToRegions.get(local);
          if (regions == null) {
            regions = new HashSet<>();
          }
          regions.add(region.getFullPath());
          memberToRegions.put(local, regions);
        }
      } else if (region instanceof LocalRegion) {
        InternalCache cache = (InternalCache) region.getCache();
        // Add local node
        InternalDistributedMember local = cache.getMyId();
        Set<String> regions = memberToRegions.get(local);
        if (regions == null) {
          regions = new HashSet<>();
        }
        regions.add(region.getFullPath());
        memberToRegions.put(local, regions);
      }
    }
    return memberToRegions;
  }

  @Override
  public AbstractExecution<IN, OUT, AGG> setIsReExecute() {
    return new MultiRegionFunctionExecutor<>(this, true);
  }

  @Override
  public void validateExecution(final Function<IN> function,
      final Set<? extends DistributedMember> targetMembers) {
    InternalCache cache = null;
    for (Region<?, ?> r : regions) {
      cache = (InternalCache) r.getCache();
      break;
    }
    if (cache == null) {
      return;
    }
    if (cache.getTxManager().getTXState() != null) {
      if (targetMembers.size() > 1) {
        throw new TransactionException(
            "Function inside a transaction cannot execute on more than one node");
      } else {
        assert targetMembers.size() == 1;
        DistributedMember funcTarget = targetMembers.iterator().next();
        DistributedMember target = cache.getTxManager().getTXState().getTarget();
        if (target == null) {
          cache.getTxManager().getTXState().setTarget(funcTarget);
        } else if (!target.equals(funcTarget)) {
          throw new TransactionDataNotColocatedException(
              String.format(
                  "Function execution is not colocated with transaction. The transactional data is hosted on node %s, but you are trying to target node %s",
                  target, funcTarget));
        }
      }
    }
    cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(function, targetMembers);
  }
}
