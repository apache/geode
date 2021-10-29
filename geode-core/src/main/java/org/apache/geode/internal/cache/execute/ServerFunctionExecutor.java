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
import java.util.function.Supplier;

import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.ExecuteFunctionNoAckOp;
import org.apache.geode.cache.client.internal.ExecuteFunctionOp;
import org.apache.geode.cache.client.internal.ExecuteFunctionOp.ExecuteFunctionOpImpl;
import org.apache.geode.cache.client.internal.GetFunctionAttributeOp;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.execute.util.SynchronizedResultCollector;
import org.apache.geode.internal.cache.partitioned.BucketId;

public class ServerFunctionExecutor<IN, OUT, AGG> extends AbstractExecution<IN, OUT, AGG> {

  private PoolImpl pool;

  private final boolean allServers;

  private final String[] groups;


  ServerFunctionExecutor(Pool pool, boolean allServers, String... groups) {
    this.pool = (PoolImpl) pool;
    this.allServers = allServers;
    this.groups = groups;
  }

  ServerFunctionExecutor(Pool pool, boolean allServers, ProxyCache proxyCache, String... groups) {
    this.pool = (PoolImpl) pool;
    this.allServers = allServers;
    this.proxyCache = proxyCache;
    this.groups = groups;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor<IN, OUT, AGG> sfe) {
    super(sfe);
    if (sfe.pool != null) {
      pool = sfe.pool;
    }
    allServers = sfe.allServers;
    groups = sfe.groups;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor<IN, OUT, AGG> sfe, IN args) {
    this(sfe);
    this.args = args;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor<IN, OUT, AGG> sfe,
      ResultCollector<OUT, AGG> collector) {
    this(sfe);
    rc = collector != null ? new SynchronizedResultCollector<>(collector) : null;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor<IN, OUT, AGG> sfe,
      MemberMappedArgument argument) {
    this(sfe);
    memberMappedArg = argument;
    isMemberMappedArgument = true;
  }

  protected ResultCollector<OUT, AGG> executeFunction(final String functionId, boolean result,
      boolean isHA,
      boolean optimizeForWrite, long timeout, TimeUnit unit) {
    try {
      if (proxyCache != null) {
        if (proxyCache.isClosed()) {
          throw proxyCache.getCacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
      }

      final int timeoutMs = TimeoutHelper.toMillis(timeout, unit);
      byte hasResult = 0;
      if (result) {
        hasResult = 1;
        if (rc == null) {
          ResultCollector<OUT, AGG> defaultCollector =
              uncheckedCast(new DefaultResultCollector<>());
          return executeOnServer(functionId, defaultCollector, hasResult, isHA, optimizeForWrite,
              timeoutMs);
        } else {
          return executeOnServer(functionId, rc, hasResult, isHA, optimizeForWrite, timeoutMs);
        }
      } else {
        executeOnServerNoAck(functionId, hasResult, isHA, optimizeForWrite);
        return new NoResult<>();
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }
  }

  @Override
  protected ResultCollector<OUT, AGG> executeFunction(final Function<IN> function, long timeout,
      TimeUnit unit) {
    byte hasResult = 0;
    try {
      if (proxyCache != null) {
        if (proxyCache.isClosed()) {
          throw proxyCache.getCacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
      }

      if (function.hasResult()) {
        final int timeoutMs = TimeoutHelper.toMillis(timeout, unit);

        if (rc == null) {
          ResultCollector<OUT, AGG> defaultCollector =
              uncheckedCast(new DefaultResultCollector<>());
          return executeOnServer(function, defaultCollector, timeoutMs);
        } else {
          return executeOnServer(function, rc, timeoutMs);
        }
      } else {
        executeOnServerNoAck(function, hasResult);
        return new NoResult<>();
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }

  }

  private ResultCollector<OUT, AGG> executeOnServer(Function<IN> function,
      ResultCollector<OUT, AGG> rc,
      int timeoutMs) {
    FunctionStats stats = FunctionStatsManager.getFunctionStats(function.getId());
    long start = stats.startFunctionExecution(true);
    try {
      validateExecution(function, null);

      final ExecuteFunctionOpImpl executeFunctionOp =
          new ExecuteFunctionOpImpl(function, args, memberMappedArg,
              rc, isFunctionSerializationRequired, (byte) 0, groups, allServers,
              isIgnoreDepartedMembers(),
              timeoutMs);

      final Supplier<ExecuteFunctionOpImpl> executeFunctionOpSupplier =
          () -> new ExecuteFunctionOpImpl(function, args, memberMappedArg,
              rc, isFunctionSerializationRequired, (byte) 0,
              null/* onGroups does not use single-hop for now */,
              false, false, timeoutMs);

      final Supplier<ExecuteFunctionOpImpl> reExecuteFunctionOpSupplier =
          () -> new ExecuteFunctionOpImpl(function, getArguments(), getMemberMappedArgument(), rc,
              isFunctionSerializationRequired, (byte) 1, groups, allServers,
              isIgnoreDepartedMembers(), timeoutMs);

      ExecuteFunctionOp.execute(pool, allServers,
          rc, function.isHA(), UserAttributes.userAttributes.get(), groups,
          executeFunctionOp,
          executeFunctionOpSupplier,
          reExecuteFunctionOpSupplier);

      stats.endFunctionExecution(start, true);
      rc.endResults();
      return rc;
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, true);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, true);
      throw new FunctionException(exception);
    }
  }

  private ResultCollector<OUT, AGG> executeOnServer(String functionId, ResultCollector<OUT, AGG> rc,
      byte hasResult,
      boolean isHA, boolean optimizeForWrite, int timeoutMs) {
    FunctionStats stats = FunctionStatsManager.getFunctionStats(functionId);
    long start = stats.startFunctionExecution(true);
    try {
      validateExecution(null, null);

      final ExecuteFunctionOpImpl executeFunctionOp =
          new ExecuteFunctionOpImpl(functionId, args, memberMappedArg, hasResult,
              rc, isFunctionSerializationRequired, isHA, optimizeForWrite, (byte) 0, groups,
              allServers,
              isIgnoreDepartedMembers(), timeoutMs);

      final Supplier<ExecuteFunctionOpImpl> executeFunctionOpSupplier =
          () -> new ExecuteFunctionOpImpl(functionId, args, memberMappedArg,
              hasResult,
              rc, isFunctionSerializationRequired, isHA, optimizeForWrite, (byte) 0,
              null/* onGroups does not use single-hop for now */, false, false, timeoutMs);

      final Supplier<ExecuteFunctionOpImpl> reExecuteFunctionOpSupplier =
          () -> new ExecuteFunctionOpImpl(functionId, args,
              getMemberMappedArgument(),
              hasResult, rc, isFunctionSerializationRequired, isHA, optimizeForWrite, (byte) 1,
              groups, allServers, isIgnoreDepartedMembers(), timeoutMs);

      ExecuteFunctionOp.execute(pool, allServers,
          rc, isHA,
          UserAttributes.userAttributes.get(), groups,
          executeFunctionOp,
          executeFunctionOpSupplier,
          reExecuteFunctionOpSupplier);

      stats.endFunctionExecution(start, true);
      rc.endResults();
      return rc;
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, true);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, true);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(Function<IN> function, byte hasResult) {
    FunctionStats stats = FunctionStatsManager.getFunctionStats(function.getId());
    long start = stats.startFunctionExecution(false);
    try {
      validateExecution(function, null);
      ExecuteFunctionNoAckOp.execute(pool, function, args, memberMappedArg, allServers,
          hasResult, isFunctionSerializationRequired, groups);
      stats.endFunctionExecution(start, false);
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, false);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, false);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(String functionId, byte hasResult, boolean isHA,
      boolean optimizeForWrite) {
    FunctionStats stats = FunctionStatsManager.getFunctionStats(functionId);
    long start = stats.startFunctionExecution(false);
    try {
      validateExecution(null, null);
      ExecuteFunctionNoAckOp.execute(pool, functionId, args, memberMappedArg, allServers,
          hasResult, isFunctionSerializationRequired, isHA, optimizeForWrite, groups);
      stats.endFunctionExecution(start, false);
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, false);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, false);
      throw new FunctionException(exception);
    }
  }

  public Pool getPool() {
    return pool;
  }

  @Override
  public Execution<IN, OUT, AGG> withFilter(Set<?> filter) {
    throw new FunctionException(
        String.format("Cannot specify %s for data independent functions",
            "filter"));
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withBucketFilter(Set<BucketId> bucketIDs) {
    throw new FunctionException(
        String.format("Cannot specify %s for data independent functions",
            "buckets as filter"));
  }

  @Override
  public Execution<IN, OUT, AGG> setArguments(IN args) {
    if (args == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "args"));
    }
    return new ServerFunctionExecutor<>(this, args);
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
    return new ServerFunctionExecutor<>(this, rs);
  }

  @Override
  public InternalExecution<IN, OUT, AGG> withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "MemberMapped Args"));
    }
    return new ServerFunctionExecutor<>(this, argument);
  }

  @Override
  public void validateExecution(final Function<IN> function,
      final Set<? extends DistributedMember> targetMembers) {
    if (TXManagerImpl.getCurrentTXUniqueId() != TXManagerImpl.NOTX) {
      throw new UnsupportedOperationException();
    }
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
      byte[] functionAttributes = getFunctionAttributes(functionName);
      if (functionAttributes == null) {
        // Set authentication properties before executing the internal function.
        try {
          if (proxyCache != null) {
            if (proxyCache.isClosed()) {
              throw proxyCache.getCacheClosedException("Cache is closed for this user.");
            }
            UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
          }

          Object obj = GetFunctionAttributeOp.execute(pool, functionName);
          functionAttributes = (byte[]) obj;
          addFunctionAttributes(functionName, functionAttributes);
        } finally {
          UserAttributes.userAttributes.set(null);
        }
      }

      boolean isHA = functionAttributes[1] == 1;
      boolean hasResult = functionAttributes[0] == 1;
      boolean optimizeForWrite = functionAttributes[2] == 1;
      return executeFunction(functionName, hasResult, isHA, optimizeForWrite, timeout, unit);
    } else {
      return executeFunction(functionObject, timeout, unit);
    }

  }

  @Override
  public ResultCollector<OUT, AGG> execute(final String functionName) {
    return execute(functionName, getTimeoutMs(), TimeUnit.MILLISECONDS);
  }
}
