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

import java.util.Set;

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
public class DistributedRegionFunctionExecutor extends AbstractExecution {

  private final LocalRegion region;

  private ServerToClientFunctionResultSender sender;

  public DistributedRegionFunctionExecutor(Region r) {
    if (r == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "region"));
    }
    this.region = (LocalRegion) r;
  }

  private DistributedRegionFunctionExecutor(DistributedRegionFunctionExecutor drfe) {
    super(drfe);
    this.region = drfe.region;
    if (drfe.filter != null) {
      this.filter.clear();
      this.filter.addAll(drfe.filter);
    }
    this.sender = drfe.sender;
  }

  public DistributedRegionFunctionExecutor(DistributedRegion region, Set filter2, Object args,
      MemberMappedArgument memberMappedArg, ServerToClientFunctionResultSender resultSender) {
    if (args != null) {
      this.args = args;
    } else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      this.isMemberMappedArgument = true;
    }
    this.sender = resultSender;
    if (filter2 != null) {
      this.filter.clear();
      this.filter.addAll(filter2);
    }
    this.region = region;
    this.isClientServerMode = true;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor,
      MemberMappedArgument argument) {
    super(distributedRegionFunctionExecutor);

    this.region = distributedRegionFunctionExecutor.getRegion();
    this.filter.clear();
    this.filter.addAll(distributedRegionFunctionExecutor.filter);
    this.sender = distributedRegionFunctionExecutor.getServerResultSender();

    this.memberMappedArg = argument;
    this.isMemberMappedArgument = true;

  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor, ResultCollector rs) {
    super(distributedRegionFunctionExecutor);

    this.region = distributedRegionFunctionExecutor.getRegion();
    this.filter.clear();
    this.filter.addAll(distributedRegionFunctionExecutor.filter);
    this.sender = distributedRegionFunctionExecutor.getServerResultSender();

    this.rc = rs;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor, Object args) {
    super(distributedRegionFunctionExecutor);

    this.region = distributedRegionFunctionExecutor.getRegion();
    this.filter.clear();
    this.filter.addAll(distributedRegionFunctionExecutor.filter);
    this.sender = distributedRegionFunctionExecutor.getServerResultSender();

    this.args = args;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor, Set filter2) {
    super(distributedRegionFunctionExecutor);

    this.region = distributedRegionFunctionExecutor.getRegion();
    this.sender = distributedRegionFunctionExecutor.getServerResultSender();

    this.filter.clear();
    this.filter.addAll(filter2);
  }

  private DistributedRegionFunctionExecutor(DistributedRegionFunctionExecutor drfe,
      boolean isReExecute) {
    super(drfe);
    this.region = drfe.region;
    if (drfe.filter != null) {
      this.filter.clear();
      this.filter.addAll(drfe.filter);
    }
    this.sender = drfe.sender;
    this.isReExecute = isReExecute;
  }

  public ResultCollector execute(final String functionName) {
    if (functionName == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    this.isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          String.format("Function named %s is not registered to FunctionService",
              functionObject));
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          "Function execution on region with DataPolicy.NORMAL is not supported");
    }
    return executeFunction(functionObject);
  }

  @Override
  public ResultCollector execute(final Function function) {
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
    this.isFnSerializationReqd = true;
    return executeFunction(function);
  }

  @Override
  protected ResultCollector executeFunction(Function function) {
    if (function.hasResult()) { // have Results
      if (this.rc == null) { // Default Result Collector
        ResultCollector defaultCollector = new DefaultResultCollector();
        return this.region.executeFunction(this, function, args, defaultCollector, this.filter,
            this.sender);
      } else { // Custome Result COllector
        return this.region.executeFunction(this, function, args, rc, this.filter, this.sender);
      }
    } else { // No results
      this.region.executeFunction(this, function, args, null, this.filter, this.sender);
      return new NoResult();
    }
  }

  public Execution withFilter(Set filter) {
    if (filter == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "filter"));
    }

    return new DistributedRegionFunctionExecutor(this, filter);
  }

  @Override
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    if (bucketIDs != null && !bucketIDs.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Buckets as filter cannot be applied to a non partitioned region: %s",
              region.getName()));
    }
    return this;
  }

  public LocalRegion getRegion() {
    return this.region;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return this.sender;
  }



  @Override
  public Execution setArguments(Object args) {
    if (args == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Args"));
    }
    return new DistributedRegionFunctionExecutor(this, args);
  }

  public Execution withArgs(Object args) {
    return setArguments(args);
  }

  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Result Collector"));
    }
    return new DistributedRegionFunctionExecutor(this, rs);
  }

  public InternalExecution withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "MemberMappedArgument"));
    }
    return new DistributedRegionFunctionExecutor(this, argument);
  }

  @Override
  public AbstractExecution setIsReExecute() {
    return new DistributedRegionFunctionExecutor(this, true);
  }

  @Override
  public String toString() {
    final StringBuffer buf = new StringBuffer();
    buf.append("[ DistributedRegionFunctionExecutor:");
    buf.append("args=");
    buf.append(this.args);
    buf.append(";region=");
    buf.append(this.region.getName());
    buf.append("]");
    return buf.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.execute.AbstractExecution#validateExecution(org.apache.geode.
   * cache.execute.Function, java.util.Set)
   */
  @Override
  public void validateExecution(Function function, Set targetMembers) {
    InternalCache cache = region.getGemFireCache();
    if (cache != null && cache.getTxManager().getTXState() != null) {
      if (targetMembers.size() > 1) {
        throw new TransactionException(
            "Function inside a transaction cannot execute on more than one node");
      } else {
        assert targetMembers.size() == 1;
        DistributedMember funcTarget = (DistributedMember) targetMembers.iterator().next();
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
