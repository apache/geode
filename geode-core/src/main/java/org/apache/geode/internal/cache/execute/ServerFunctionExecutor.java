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

import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.ExecuteFunctionNoAckOp;
import org.apache.geode.cache.client.internal.ExecuteFunctionOp;
import org.apache.geode.cache.client.internal.GetFunctionAttributeOp;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.execute.util.SynchronizedResultCollector;

public class ServerFunctionExecutor extends AbstractExecution {

  private PoolImpl pool;

  private final boolean allServers;

  private String[] groups;

  public ServerFunctionExecutor(Pool p, boolean allServers, String... groups) {
    this.pool = (PoolImpl) p;
    this.allServers = allServers;
    this.groups = groups;
  }

  public ServerFunctionExecutor(Pool p, boolean allServers, ProxyCache proxyCache,
      String... groups) {
    this.pool = (PoolImpl) p;
    this.allServers = allServers;
    this.proxyCache = proxyCache;
    this.groups = groups;
  }

  public ServerFunctionExecutor(ServerFunctionExecutor sfe) {
    super(sfe);
    if (sfe.pool != null) {
      this.pool = sfe.pool;
    }
    this.allServers = sfe.allServers;
    this.groups = sfe.groups;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor sfe, Object args) {
    this(sfe);
    this.args = args;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor sfe, ResultCollector collector) {
    this(sfe);
    this.rc = collector != null ? new SynchronizedResultCollector(collector) : collector;
  }

  private ServerFunctionExecutor(ServerFunctionExecutor sfe, MemberMappedArgument argument) {
    this(sfe);
    this.memberMappedArg = argument;
    this.isMemberMappedArgument = true;
  }

  protected ResultCollector executeFunction(final String functionId, boolean result, boolean isHA,
      boolean optimizeForWrite) {
    try {
      if (proxyCache != null) {
        if (this.proxyCache.isClosed()) {
          throw proxyCache.getCacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
      }

      byte hasResult = 0;
      if (result) { // have Results
        hasResult = 1;
        if (this.rc == null) { // Default Result Collector
          ResultCollector defaultCollector = new DefaultResultCollector();
          return executeOnServer(functionId, defaultCollector, hasResult, isHA, optimizeForWrite);
        } else { // Custome Result COllector
          return executeOnServer(functionId, this.rc, hasResult, isHA, optimizeForWrite);
        }
      } else { // No results
        executeOnServerNoAck(functionId, hasResult, isHA, optimizeForWrite);
        return new NoResult();
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }
  }

  @Override
  protected ResultCollector executeFunction(final Function function) {
    byte hasResult = 0;
    try {
      if (proxyCache != null) {
        if (this.proxyCache.isClosed()) {
          throw proxyCache.getCacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
      }

      if (function.hasResult()) { // have Results
        hasResult = 1;
        if (this.rc == null) { // Default Result Collector
          ResultCollector defaultCollector = new DefaultResultCollector();
          return executeOnServer(function, defaultCollector, hasResult);
        } else { // Custome Result COllector
          return executeOnServer(function, this.rc, hasResult);
        }
      } else { // No results
        executeOnServerNoAck(function, hasResult);
        return new NoResult();
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }

  }

  private ResultCollector executeOnServer(Function function, ResultCollector rc, byte hasResult) {
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId());
    try {
      validateExecution(function, null);
      long start = stats.startTime();
      stats.startFunctionExecution(true);
      ExecuteFunctionOp.execute(this.pool, function, this, args, memberMappedArg, this.allServers,
          hasResult, rc, this.isFnSerializationReqd, UserAttributes.userAttributes.get(), groups);
      stats.endFunctionExecution(start, true);
      rc.endResults();
      return rc;
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(true);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(true);
      throw new FunctionException(exception);
    }
  }

  private ResultCollector executeOnServer(String functionId, ResultCollector rc, byte hasResult,
      boolean isHA, boolean optimizeForWrite) {
    FunctionStats stats = FunctionStats.getFunctionStats(functionId);
    try {
      validateExecution(null, null);
      long start = stats.startTime();
      stats.startFunctionExecution(true);
      ExecuteFunctionOp.execute(this.pool, functionId, this, args, memberMappedArg, this.allServers,
          hasResult, rc, this.isFnSerializationReqd, isHA, optimizeForWrite,
          UserAttributes.userAttributes.get(), groups);
      stats.endFunctionExecution(start, true);
      rc.endResults();
      return rc;
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(true);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(true);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(Function function, byte hasResult) {
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId());
    try {
      validateExecution(function, null);
      long start = stats.startTime();
      stats.startFunctionExecution(false);
      ExecuteFunctionNoAckOp.execute(this.pool, function, args, memberMappedArg, this.allServers,
          hasResult, this.isFnSerializationReqd, groups);
      stats.endFunctionExecution(start, false);
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(false);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(false);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(String functionId, byte hasResult, boolean isHA,
      boolean optimizeForWrite) {
    FunctionStats stats = FunctionStats.getFunctionStats(functionId);
    try {
      validateExecution(null, null);
      long start = stats.startTime();
      stats.startFunctionExecution(false);
      ExecuteFunctionNoAckOp.execute(this.pool, functionId, args, memberMappedArg, this.allServers,
          hasResult, this.isFnSerializationReqd, isHA, optimizeForWrite, groups);
      stats.endFunctionExecution(start, false);
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(false);
      throw functionException;
    } catch (ServerConnectivityException exception) {
      throw exception;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(false);
      throw new FunctionException(exception);
    }
  }

  public Pool getPool() {
    return this.pool;
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
            "buckets as filter"));
  }

  @Override
  public Execution setArguments(Object args) {
    if (args == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "args"));
    }
    return new ServerFunctionExecutor(this, args);
  }

  @Override
  public Execution withArgs(Object args) {
    return setArguments(args);
  }

  @Override
  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "Result Collector"));
    }
    return new ServerFunctionExecutor(this, rs);
  }

  @Override
  public InternalExecution withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "MemberMapped Args"));
    }
    return new ServerFunctionExecutor(this, argument);
  }

  @Override
  public void validateExecution(Function function, Set targetMembers) {
    if (TXManagerImpl.getCurrentTXUniqueId() != TXManagerImpl.NOTX) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public ResultCollector execute(final String functionName) {
    if (functionName == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    this.isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      byte[] functionAttributes = getFunctionAttributes(functionName);

      if (functionAttributes == null) {
        // GEODE-5618: Set authentication properties before executing the internal function.
        try {
          if (proxyCache != null) {
            if (this.proxyCache.isClosed()) {
              throw proxyCache.getCacheClosedException("Cache is closed for this user.");
            }
            UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
          }

          Object obj = GetFunctionAttributeOp.execute(this.pool, functionName);
          functionAttributes = (byte[]) obj;
          addFunctionAttributes(functionName, functionAttributes);
        } finally {
          UserAttributes.userAttributes.set(null);
        }
      }

      boolean isHA = functionAttributes[1] == 1;
      boolean hasResult = functionAttributes[0] == 1;
      boolean optimizeForWrite = functionAttributes[2] == 1;
      return executeFunction(functionName, hasResult, isHA, optimizeForWrite);
    } else {
      return executeFunction(functionObject);
    }
  }
}
