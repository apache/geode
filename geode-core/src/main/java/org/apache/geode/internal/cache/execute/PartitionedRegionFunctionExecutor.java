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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;

public class PartitionedRegionFunctionExecutor<IN, OUT, AGG>
    extends AbstractExecution<IN, OUT, AGG> {

  private final PartitionedRegion pr;

  private ServerToClientFunctionResultSender sender;

  private boolean executeOnBucketSet = false;

  private boolean isPRSingleHop = false;

  public PartitionedRegionFunctionExecutor(Region<?, ?> r) {
    if (r == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "region"));
    }
    pr = (PartitionedRegion) r;
  }

  private PartitionedRegionFunctionExecutor(PartitionedRegionFunctionExecutor<IN, OUT, AGG> other,
      MemberMappedArgument argument) {
    // super copies args, rc and memberMappedArgument
    super(other);

    pr = other.pr;
    executeOnBucketSet = other.executeOnBucketSet;
    isPRSingleHop = other.isPRSingleHop;
    filter.addAll(other.filter);
    sender = other.sender;
    // override member mapped arguments
    memberMappedArg = argument;
    isMemberMappedArgument = true;
  }

  private PartitionedRegionFunctionExecutor(PartitionedRegionFunctionExecutor<IN, OUT, AGG> other,
      ResultCollector<OUT, AGG> rs) {
    // super copies args, rc and memberMappedArgument
    super(other);

    pr = other.pr;
    executeOnBucketSet = other.executeOnBucketSet;
    isPRSingleHop = other.isPRSingleHop;
    filter.addAll(other.filter);
    sender = other.sender;
    // override ResultCollector
    rc = rs;
  }

  private PartitionedRegionFunctionExecutor(PartitionedRegionFunctionExecutor<IN, OUT, AGG> other,
      IN arguments) {

    // super copies args, rc and memberMappedArgument
    super(other);
    pr = other.pr;
    executeOnBucketSet = other.executeOnBucketSet;
    isPRSingleHop = other.isPRSingleHop;
    filter.addAll(other.filter);
    sender = other.sender;
    // override arguments
    args = arguments;
  }

  private PartitionedRegionFunctionExecutor(PartitionedRegionFunctionExecutor<IN, OUT, AGG> other,
      Set<?> filter) {
    // super copies args, rc and memberMappedArgument
    super(other);
    pr = other.pr;
    executeOnBucketSet = other.executeOnBucketSet;
    isPRSingleHop = other.isPRSingleHop;
    sender = other.sender;
    this.filter.addAll(filter);
    isReExecute = other.isReExecute;
  }

  private PartitionedRegionFunctionExecutor(PartitionedRegionFunctionExecutor<IN, OUT, AGG> other,
      Set<Integer> bucketsAsFilter, boolean executeOnBucketSet) {
    // super copies args, rc and memberMappedArgument
    super(other);
    pr = other.pr;
    this.executeOnBucketSet = executeOnBucketSet;
    isPRSingleHop = other.isPRSingleHop;
    sender = other.sender;
    filter.addAll(bucketsAsFilter);
    isReExecute = other.isReExecute;
  }

  private PartitionedRegionFunctionExecutor(PartitionedRegionFunctionExecutor<IN, OUT, AGG> other,
      boolean isReExecute) {
    super(other);
    pr = other.pr;
    executeOnBucketSet = other.executeOnBucketSet;
    isPRSingleHop = other.isPRSingleHop;
    if (other.filter != null) {
      filter.addAll(other.filter);
    }
    if (other.sender != null) {
      sender = other.sender;
    }
    this.isReExecute = isReExecute;
    isClientServerMode = other.isClientServerMode;
    if (other.failedNodes != null) {
      failedNodes.clear();
      failedNodes.addAll(other.failedNodes);
    }
  }

  public PartitionedRegionFunctionExecutor(PartitionedRegion region, Set<Object> filter, IN args,
      MemberMappedArgument memberMappedArg, ServerToClientFunctionResultSender resultSender,
      Set<String> failedNodes, boolean executeOnBucketSet) {
    pr = region;
    sender = resultSender;
    isClientServerMode = true;
    this.executeOnBucketSet = executeOnBucketSet;
    if (filter != null) {
      this.filter.addAll(filter);
    }

    if (args != null) {
      this.args = args;
    } else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      isMemberMappedArgument = true;
    }

    if (failedNodes != null) {
      this.failedNodes.clear();
      this.failedNodes.addAll(failedNodes);
    }

  }


  public PartitionedRegionFunctionExecutor(PartitionedRegion region, Set<?> filter, IN args,
      MemberMappedArgument memberMappedArg, ServerToClientFunctionResultSender resultSender,
      Set<String> failedNodes, boolean executeOnBucketSet, boolean isPRSingleHop) {
    pr = region;
    sender = resultSender;
    isClientServerMode = true;
    this.executeOnBucketSet = executeOnBucketSet;
    this.isPRSingleHop = isPRSingleHop;
    if (filter != null) {
      this.filter.addAll(filter);
    }

    if (args != null) {
      this.args = args;
    } else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      isMemberMappedArgument = true;
    }

    if (failedNodes != null) {
      this.failedNodes.clear();
      this.failedNodes.addAll(failedNodes);
    }
  }

  @Immutable
  private static final ResultCollector<?, ?> NO_RESULT = new NoResult<>();

  @SuppressWarnings("unchecked")
  private static <T, S> ResultCollector<T, S> noResult() {
    return (ResultCollector<T, S>) NO_RESULT;
  }

  @Override
  public ResultCollector<OUT, AGG> executeFunction(final Function<IN> function, long timeout,
      TimeUnit unit) {
    try {
      if (!function.hasResult()) /* NO RESULT:fire-n-forget */ {
        pr.executeFunction(function, this, null, executeOnBucketSet);
        return noResult();
      }
      ResultCollector<OUT, AGG> inRc =
          (rc == null) ? uncheckedCast(new DefaultResultCollector<>()) : rc;
      ResultCollector<OUT, AGG> rcToReturn =
          pr.executeFunction(function, this, inRc, executeOnBucketSet);
      if (timeout > 0) {
        try {
          rcToReturn.getResult(timeout, unit);
        } catch (Exception exception) {
          throw new FunctionException(exception);
        }
      }
      return rcToReturn;
    } finally {
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        pr.clearNetworkHopData();
      }
    }
  }

  @Override
  public Execution<IN, OUT, AGG> withFilter(Set<?> filter) {
    if (filter == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "filter"));
    }
    executeOnBucketSet = false;
    return new PartitionedRegionFunctionExecutor<>(this, filter);
  }


  @Override
  public InternalExecution<IN, OUT, AGG> withBucketFilter(Set<Integer> bucketIDs) {
    if (bucketIDs == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "buckets as filter"));
    } else if (bucketIDs.isEmpty()) {
      throw new FunctionException("Bucket IDs list is empty");
    }

    Set<Integer> actualBucketSet = pr.getRegionAdvisor().getBucketSet();

    bucketIDs.retainAll(actualBucketSet);

    for (final int bid : bucketIDs) {
      if (!actualBucketSet.contains(bid)) {
        throw new FunctionException("Bucket " + bid + " does not exist.");
      }
    }
    if (bucketIDs.isEmpty()) {
      throw new FunctionException("No valid buckets to execute on");
    }
    return new PartitionedRegionFunctionExecutor<>(this, bucketIDs, true);
  }

  public LocalRegion getRegion() {
    return pr;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return sender;
  }

  @Override
  public Execution<IN, OUT, AGG> setArguments(IN args) {
    if (args == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "args"));
    }
    return new PartitionedRegionFunctionExecutor<>(this, args);
  }

  @Override
  public Execution<IN, OUT, AGG> withArgs(IN args) {
    return setArguments(args);
  }

  @Override
  public Execution<IN, OUT, AGG> withCollector(ResultCollector<OUT, AGG> rs) {
    if (rs == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "Result Collector"));
    }
    return new PartitionedRegionFunctionExecutor<>(this, rs);
  }

  @Override
  public AbstractExecution<IN, OUT, AGG> setIsReExecute() {
    return new PartitionedRegionFunctionExecutor<>(this, true);
  }

  public boolean isPrSingleHop() {
    return isPRSingleHop;
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "MemberMapped arg"));
    }
    return new PartitionedRegionFunctionExecutor<>(this, argument);
  }

  @Override
  public String toString() {
    return "[PartitionedRegionFunctionExecutor:"
        + "args="
        + args
        + ";filter="
        + filter
        + ";region="
        + pr.getName()
        + "]";
  }

  @Override
  public void validateExecution(final Function<IN> function,
      final Set<? extends DistributedMember> targetMembers) {
    InternalCache cache = pr.getGemFireCache();
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
          throw new TransactionDataRebalancedException(
              String.format(
                  "Function execution is not colocated with transaction. The transactional data is hosted on node %s, but you are trying to target node %s",
                  target, funcTarget));
        }
      }
    }
    cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(function, targetMembers);
  }
}
