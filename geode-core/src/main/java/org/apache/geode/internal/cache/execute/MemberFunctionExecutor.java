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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;

public class MemberFunctionExecutor extends AbstractExecution {

  protected InternalDistributedSystem distributedSystem;

  protected Set<InternalDistributedMember> members;

  private ServerToClientFunctionResultSender sender;

  MemberFunctionExecutor(DistributedSystem distributedSystem) {
    this(distributedSystem,
        new HashSet(((InternalDistributedSystem) distributedSystem).getDistributionManager()
            .getNormalDistributionManagerIds()));
  }

  MemberFunctionExecutor(DistributedSystem distributedSystem, DistributedMember distributedMember) {
    this(distributedSystem, Collections.singleton((InternalDistributedMember) distributedMember));
  }

  MemberFunctionExecutor(DistributedSystem distributedSystem,
      Set<? extends DistributedMember> members) {
    this.distributedSystem = (InternalDistributedSystem) distributedSystem;
    this.members = (Set<InternalDistributedMember>) members;
  }

  public MemberFunctionExecutor(DistributedSystem distributedSystem,
      Set<? extends DistributedMember> members,
      ServerToClientFunctionResultSender sender) {
    this(distributedSystem, members);
    this.sender = sender;
  }

  private MemberFunctionExecutor(MemberFunctionExecutor memFunctionExecutor) {
    super(memFunctionExecutor);
    distributedSystem = memFunctionExecutor.distributedSystem;
    members = new HashSet<>(memFunctionExecutor.members);
    sender = memFunctionExecutor.sender;
  }

  private MemberFunctionExecutor(MemberFunctionExecutor memberFunctionExecutor,
      MemberMappedArgument argument) {
    this(memberFunctionExecutor);

    memberMappedArg = argument;
    isMemberMappedArgument = true;
  }

  private MemberFunctionExecutor(MemberFunctionExecutor memberFunctionExecutor,
      ResultCollector rs) {
    this(memberFunctionExecutor);

    rc = rs;
  }

  private MemberFunctionExecutor(MemberFunctionExecutor memberFunctionExecutor, Object arguments) {
    this(memberFunctionExecutor);

    args = arguments;
  }

  private ResultCollector executeFunction(final Function function,
      ResultCollector resultCollector) {
    final DistributionManager dm = distributedSystem.getDistributionManager();
    final Set<InternalDistributedMember> dest = new HashSet<>(members);
    if (dest.isEmpty()) {
      throw new FunctionException(
          String.format("No member found for executing function : %s.",
              function.getId()));
    }
    validateExecution(function, dest);
    setExecutionNodes(dest);

    final InternalDistributedMember localVM =
        distributedSystem.getDistributionManager().getDistributionManagerId();
    final LocalResultCollector<?, ?> localRC = getLocalResultCollector(function, resultCollector);
    boolean remoteOnly = false;
    boolean localOnly = false;
    if (!dest.contains(localVM)) {
      remoteOnly = true;
    }
    if (dest.size() == 1 && dest.contains(localVM)) {
      localOnly = true;
    }


    final MemberFunctionResultSender resultSender =
        new MemberFunctionResultSender(dm, localRC, function, localOnly, remoteOnly, sender);
    if (dest.contains(localVM)) {
      // if member is local VM
      dest.remove(localVM);

      boolean isTx = false;
      InternalCache cache = GemFireCacheImpl.getInstance();
      if (cache != null) {
        isTx = cache.getTxManager().getTXState() != null;
      }
      final FunctionContext context = new FunctionContextImpl(cache, function.getId(),
          getArgumentsForMember(localVM.getId()), resultSender);
      executeFunctionOnLocalNode(function, context, resultSender, dm, isTx);
    }

    if (!dest.isEmpty()) {
      HashMap<InternalDistributedMember, Object> memberArgs = new HashMap<>();
      for (InternalDistributedMember distributedMember : dest) {
        memberArgs.put(distributedMember, getArgumentsForMember(distributedMember.getId()));
      }
      Assert.assertTrue(memberArgs.size() == dest.size());
      MemberFunctionResultWaiter resultReceiver =
          new MemberFunctionResultWaiter(distributedSystem, localRC,
              function, memberArgs, dest, resultSender);

      return resultReceiver.getFunctionResultFrom(dest, function, this);
    }
    return localRC;
  }

  @Override
  public void validateExecution(final Function function,
      final Set<? extends DistributedMember> dest) {
    final InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      return;
    }
    if (cache.getTxManager().getTXState() != null) {
      if (dest.size() > 1) {
        throw new TransactionException(
            "Function inside a transaction cannot execute on more than one node");
      } else {
        assert dest.size() == 1;
        if (cache.isClient()) {
          throw new UnsupportedOperationException(
              "Client function execution on members is not supported with transaction");
        }
        DistributedMember funcTarget = dest.iterator().next();
        DistributedMember target = cache.getTxManager().getTXState().getTarget();
        if (target == null) {
          cache.getTxManager().getTXState().setTarget(funcTarget);
        } else if (!target.equals(funcTarget)) {
          throw new TransactionDataNotColocatedException(
              "Function execution is not colocated with transaction");
        }
      }
    }
    cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(function, dest);
  }

  @Override
  protected ResultCollector executeFunction(Function function) {
    if (function.hasResult()) {
      ResultCollector rc = this.rc;
      if (rc == null) {
        rc = new DefaultResultCollector();
      }
      return executeFunction(function, rc);
    } else {
      executeFunction(function, null);
      return new NoResult();
    }
  }

  @Override
  public Execution setArguments(Object args) {
    if (args == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "args"));
    }
    return new MemberFunctionExecutor(this, args);
  }

  // Changing the object!!
  @Override
  public Execution withArgs(Object args) {
    return setArguments(args);
  }

  // Changing the object!!
  @Override
  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Result Collector"));
    }
    return new MemberFunctionExecutor(this, rs);
  }

  @Override
  public Execution withFilter(Set filter) {
    throw new FunctionException(
        String.format("Cannot specify %s for data independent functions",
            "filter"));
  }

  @Override
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    throw new FunctionException(
        String.format("Cannot specify %s for data independent functions",
            "bucket as filter"));
  }

  @Override
  public InternalExecution withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "MemberMappedArgs"));
    }
    return new MemberFunctionExecutor(this, argument);
  }

  @Override
  public Object getArgumentsForMember(String memberId) {
    if (!isMemberMappedArgument) {
      return args;
    } else {
      return memberMappedArg.getArgumentsForMember(memberId);
    }
  }

  @Override
  public MemberMappedArgument getMemberMappedArgument() {
    return memberMappedArg;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return sender;
  }
}
