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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.MemoryThresholdInfo;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.MemoryThresholds;

/**
 * Executes Function on Distributed Regions.
 *
 * For DistributedRegions with DataPolicy.NORMAL, it throws UnsupportedOperationException. <br>
 * For DistributedRegions with DataPolicy.EMPTY, execute the function on any random member which has
 * DataPolicy.REPLICATE <br>
 * For DistributedRegions with DataPolicy.REPLICATE, execute the function locally.
 *
 *
 * @since GemFire 5.8 LA
 *
 */
public class DistributedRegionFunctionExecutor<IN, OUT, AGG>
    extends AbstractExecution<IN, OUT, AGG> {

  private final LocalRegion region;

  private ServerToClientFunctionResultSender sender;

  public DistributedRegionFunctionExecutor(Region<?, ?> r) {
    if (r == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "region"));
    }
    region = (LocalRegion) r;
  }

  public DistributedRegionFunctionExecutor(DistributedRegion region, Set<Object> filter,
      Object args,
      MemberMappedArgument memberMappedArg, ServerToClientFunctionResultSender resultSender) {
    if (args != null) {
      this.args = args;
    } else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      isMemberMappedArgument = true;
    }
    sender = resultSender;
    if (filter != null) {
      this.filter.clear();
      this.filter.addAll(filter);
    }
    this.region = region;
    isClientServerMode = true;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor<IN, OUT, AGG> distributedRegionFunctionExecutor,
      MemberMappedArgument argument) {
    super(distributedRegionFunctionExecutor);

    region = distributedRegionFunctionExecutor.getRegion();
    filter.clear();
    filter.addAll(distributedRegionFunctionExecutor.filter);
    sender = distributedRegionFunctionExecutor.getServerResultSender();

    memberMappedArg = argument;
    isMemberMappedArgument = true;

  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor<IN, OUT, AGG> distributedRegionFunctionExecutor,
      ResultCollector<OUT, AGG> rs) {
    super(distributedRegionFunctionExecutor);

    region = distributedRegionFunctionExecutor.getRegion();
    filter.clear();
    filter.addAll(distributedRegionFunctionExecutor.filter);
    sender = distributedRegionFunctionExecutor.getServerResultSender();

    rc = rs;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor<IN, OUT, AGG> distributedRegionFunctionExecutor,
      Object args) {
    super(distributedRegionFunctionExecutor);

    region = distributedRegionFunctionExecutor.getRegion();
    filter.clear();
    filter.addAll(distributedRegionFunctionExecutor.filter);
    sender = distributedRegionFunctionExecutor.getServerResultSender();

    this.args = args;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor<IN, OUT, AGG> distributedRegionFunctionExecutor,
      Set<Object> filter2) {
    super(distributedRegionFunctionExecutor);

    region = distributedRegionFunctionExecutor.getRegion();
    sender = distributedRegionFunctionExecutor.getServerResultSender();

    filter.clear();
    filter.addAll(filter2);
  }

  private DistributedRegionFunctionExecutor(DistributedRegionFunctionExecutor<IN, OUT, AGG> other,
      boolean isReExecute) {
    super(other);
    region = other.region;
    if (other.filter != null) {
      filter.clear();
      filter.addAll(other.filter);
    }
    sender = other.sender;
    this.isReExecute = isReExecute;
  }

  @Override
  public ResultCollector<OUT, AGG> execute(final String functionName, long timeout, TimeUnit unit) {
    if (functionName == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    isFunctionSerializationRequired = false;
    Function<IN> functionObject = uncheckedCast(FunctionService.getFunction(functionName));
    if (functionObject == null) {
      throw new FunctionException(
          String.format("Function named %s is not registered to FunctionService",
              functionName));
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          "Function execution on region with DataPolicy.NORMAL is not supported");
    }
    return executeFunction(functionObject, timeout, unit);
  }

  @Override
  public ResultCollector<OUT, AGG> execute(final String functionName) {
    return execute(functionName, getTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  @Override
  public ResultCollector<OUT, AGG> execute(@SuppressWarnings("rawtypes") final Function function,
      long timeout, TimeUnit unit) {
    if (function == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "function instance"));
    }
    if (function.isHA() && !function.hasResult()) {
      throw new FunctionException(
          "For Functions with isHA true, hasResult must also be true.");
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          "Function execution on region with DataPolicy.NORMAL is not supported");
    }
    String id = function.getId();
    if (id == null) {
      throw new FunctionException(
          "The Function#getID() returned null");
    }
    isFunctionSerializationRequired = true;
    return executeFunction(uncheckedCast(function), timeout, unit);
  }

  @Override
  public ResultCollector<OUT, AGG> execute(@SuppressWarnings("rawtypes") final Function function) {
    return execute(function, getTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected ResultCollector<OUT, AGG> executeFunction(Function<IN> function, long timeout,
      TimeUnit unit) {
    if (!function.hasResult()) {
      region.executeFunction(this, function, args, null, filter, sender);
      return new NoResult<>();
    }
    ResultCollector<OUT, AGG> inRc =
        (rc == null) ? uncheckedCast(new DefaultResultCollector<>()) : rc;
    ResultCollector<OUT, AGG> rcToReturn =
        uncheckedCast(region.executeFunction(this, function, args, inRc, filter, sender));
    if (timeout > 0) {
      try {
        rcToReturn.getResult(timeout, unit);
      } catch (Exception exception) {
        throw new FunctionException(exception);
      }
    }
    return rcToReturn;
  }

  @Override
  public Execution<IN, OUT, AGG> withFilter(@SuppressWarnings("rawtypes") Set filter) {
    if (filter == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "filter"));
    }

    // noinspection unchecked
    return new DistributedRegionFunctionExecutor<IN, OUT, AGG>(this, filter);
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withBucketFilter(Set<Integer> bucketIDs) {
    if (bucketIDs != null && !bucketIDs.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Buckets as filter cannot be applied to a non partitioned region: %s",
              region.getName()));
    }
    return this;
  }

  public LocalRegion getRegion() {
    return region;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return sender;
  }



  @Override
  public Execution<IN, OUT, AGG> setArguments(Object args) {
    if (args == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Args"));
    }
    return new DistributedRegionFunctionExecutor<>(this, args);
  }

  @Override
  public Execution<IN, OUT, AGG> withArgs(Object args) {
    return setArguments(args);
  }

  @Override
  public Execution<IN, OUT, AGG> withCollector(ResultCollector<OUT, AGG> rs) {
    if (rs == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Result Collector"));
    }
    return new DistributedRegionFunctionExecutor<>(this, rs);
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "MemberMappedArgument"));
    }
    return new DistributedRegionFunctionExecutor<>(this, argument);
  }

  @Override
  public AbstractExecution<IN, OUT, AGG> setIsReExecute() {
    return new DistributedRegionFunctionExecutor<>(this, true);
  }

  @Override
  public String toString() {
    return "[DistributedRegionFunctionExecutor:"
        + "args="
        + args
        + ";region="
        + region.getName()
        + "]";
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.execute.AbstractExecution#validateExecution(org.apache.geode.
   * cache.execute.Function, java.util.Set)
   */
  @Override
  public void validateExecution(final Function<?> function,
      final Set<? extends DistributedMember> targetMembers) {
    InternalCache cache = region.getGemFireCache();
    if (cache != null && cache.getTxManager().getTXState() != null) {
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
    if (!MemoryThresholds.isLowMemoryExceptionDisabled() && function.optimizeForWrite()) {
      MemoryThresholdInfo info = region.getAtomicThresholdInfo();
      if (info.isMemoryThresholdReached()) {
        InternalResourceManager.getInternalResourceManager(region.getCache()).getHeapMonitor()
            .updateStateAndSendEvent();
        Set<DistributedMember> criticalMembers = info.getMembersThatReachedThreshold();
        throw new LowMemoryException(
            String.format(
                "Function: %s cannot be executed because the members %s are running low on memory",
                function.getId(), criticalMembers),
            criticalMembers);
      }
    }
  }

}
