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
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.execute.util.SynchronizedResultCollector;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Executes Function with FunctionService#onRegion(Region region) in client server mode.
 *
 * @see FunctionService#onRegion(Region) *
 * @since GemFire 5.8 LA
 */
public class ServerRegionFunctionExecutor extends AbstractExecution {
  private static final Logger logger = LogService.getLogger();

  private final LocalRegion region;
  private boolean executeOnBucketSet = false;

  ServerRegionFunctionExecutor(Region r, ProxyCache proxyCache) {
    if (r == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "Region"));
    }
    region = (LocalRegion) r;
    this.proxyCache = proxyCache;
  }

  private ServerRegionFunctionExecutor(ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      Object args) {
    super(serverRegionFunctionExecutor);

    region = serverRegionFunctionExecutor.region;
    filter.clear();
    filter.addAll(serverRegionFunctionExecutor.filter);
    this.args = args;
    executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }

  private ServerRegionFunctionExecutor(ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      MemberMappedArgument memberMapargs) {
    super(serverRegionFunctionExecutor);

    region = serverRegionFunctionExecutor.region;
    filter.clear();
    filter.addAll(serverRegionFunctionExecutor.filter);
    memberMappedArg = memberMapargs;
    executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }


  private ServerRegionFunctionExecutor(ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      ResultCollector rc) {
    super(serverRegionFunctionExecutor);

    region = serverRegionFunctionExecutor.region;
    filter.clear();
    filter.addAll(serverRegionFunctionExecutor.filter);
    this.rc = rc != null ? new SynchronizedResultCollector(rc) : null;
    executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }

  private ServerRegionFunctionExecutor(ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      Set filter2) {

    super(serverRegionFunctionExecutor);

    region = serverRegionFunctionExecutor.region;
    filter.clear();
    filter.addAll(filter2);
    executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }

  private ServerRegionFunctionExecutor(ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      Set<Integer> bucketsAsFilter, boolean executeOnBucketSet) {

    super(serverRegionFunctionExecutor);

    region = serverRegionFunctionExecutor.region;
    filter.clear();
    filter.addAll(bucketsAsFilter);
    this.executeOnBucketSet = executeOnBucketSet;
  }

  @Override
  public Execution withFilter(Set fltr) {
    if (fltr == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "filter"));
    }
    executeOnBucketSet = false;
    return new ServerRegionFunctionExecutor(this, fltr);
  }

  @Override
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    if (bucketIDs == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "buckets as filter"));
    }
    return new ServerRegionFunctionExecutor(this, bucketIDs, true /* execute on bucketset */);
  }

  @Override
  protected ResultCollector executeFunction(final Function function, long timeout, TimeUnit unit) {
    byte hasResult = 0;
    try {
      if (proxyCache != null) {
        if (proxyCache.isClosed()) {
          throw proxyCache.getCacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
      }

      if (function.hasResult()) { // have Results
        final int timeoutMs = TimeoutHelper.toMillis(timeout, unit);
        hasResult = 1;
        if (rc == null) { // Default Result Collector
          ResultCollector defaultCollector = new DefaultResultCollector();
          return executeOnServer(function, defaultCollector, hasResult, timeoutMs);
        } else { // Custome Result COllector
          return executeOnServer(function, rc, hasResult, timeoutMs);
        }
      } else { // No results
        executeOnServerNoAck(function, hasResult);
        return new NoResult();
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }
  }

  protected ResultCollector executeFunction(final String functionId, boolean resultReq,
      boolean isHA, boolean optimizeForWrite, long timeout, TimeUnit unit) {
    try {
      if (proxyCache != null) {
        if (proxyCache.isClosed()) {
          throw proxyCache.getCacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
      }
      byte hasResult = 0;
      if (resultReq) { // have Results
        hasResult = 1;
        final int timeoutMs = TimeoutHelper.toMillis(timeout, unit);
        if (rc == null) { // Default Result Collector
          ResultCollector defaultCollector = new DefaultResultCollector();
          return executeOnServer(functionId, defaultCollector, hasResult, isHA, optimizeForWrite,
              timeoutMs);
        } else { // Custome Result COllector
          return executeOnServer(functionId, rc, hasResult, isHA, optimizeForWrite, timeoutMs);
        }
      } else { // No results
        executeOnServerNoAck(functionId, hasResult, isHA, optimizeForWrite);
        return new NoResult();
      }
    } finally {
      UserAttributes.userAttributes.set(null);
    }
  }

  private ResultCollector executeOnServer(Function function, ResultCollector collector,
      byte hasResult, int timeoutMs) throws FunctionException {
    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats =
        FunctionStatsManager.getFunctionStats(function.getId(), region.getSystem());
    long start = stats.startFunctionExecution(true);
    try {
      validateExecution(function, null);
      srp.executeFunction(function, this, collector, hasResult,
          timeoutMs);
      stats.endFunctionExecution(start, true);
      return collector;
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, true);
      throw functionException;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, true);
      throw new FunctionException(exception);
    }
  }

  private ResultCollector executeOnServer(String functionId, ResultCollector collector,
      byte hasResult, boolean isHA, boolean optimizeForWrite, int timeoutMs)
      throws FunctionException {

    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats = FunctionStatsManager.getFunctionStats(functionId, region.getSystem());
    long start = stats.startFunctionExecution(true);
    try {
      validateExecution(null, null);
      srp.executeFunction(functionId, this, collector, hasResult, isHA,
          optimizeForWrite, timeoutMs);
      stats.endFunctionExecution(start, true);
      return collector;
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, true);
      throw functionException;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, true);
      throw new FunctionException(exception);
    }
  }


  private void executeOnServerNoAck(Function function, byte hasResult) throws FunctionException {
    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats =
        FunctionStatsManager.getFunctionStats(function.getId(), region.getSystem());
    long start = stats.startFunctionExecution(false);
    try {
      validateExecution(function, null);
      srp.executeFunctionNoAck(region.getFullPath(), function, this, hasResult, false);
      stats.endFunctionExecution(start, false);
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, false);
      throw functionException;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, false);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(String functionId, byte hasResult, boolean isHA,
      boolean optimizeForWrite) throws FunctionException {
    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats = FunctionStatsManager.getFunctionStats(functionId, region.getSystem());
    long start = stats.startFunctionExecution(false);
    try {
      validateExecution(null, null);
      srp.executeFunctionNoAck(region.getFullPath(), functionId, this, hasResult, isHA,
          optimizeForWrite, false);
      stats.endFunctionExecution(start, false);
    } catch (FunctionException functionException) {
      stats.endFunctionExecutionWithException(start, false);
      throw functionException;
    } catch (Exception exception) {
      stats.endFunctionExecutionWithException(start, false);
      throw new FunctionException(exception);
    }
  }

  private ServerRegionProxy getServerRegionProxy() throws FunctionException {
    ServerRegionProxy srp = region.getServerProxy();
    if (srp != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Found server region proxy on region. RegionName: {}", region.getName());
      }
      return srp;
    } else {
      String message = srp + ": "
          + "No available connection was found. Server Region Proxy is not available for this region "
          + region.getName();
      throw new FunctionException(message);
    }
  }

  public LocalRegion getRegion() {
    return region;
  }

  @Override
  public String toString() {
    return "[ ServerRegionExecutor: args=" + args + " ;filter=" + filter + " ;region="
        + region.getName() + "]";
  }

  @Override
  public Execution setArguments(Object args) {
    if (args == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "args"));
    }
    return new ServerRegionFunctionExecutor(this, args);
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
    return new ServerRegionFunctionExecutor(this, rs);
  }

  @Override
  public InternalExecution withMemberMappedArgument(MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          String.format("The input %s for the execute function request is null",
              "MemberMappedArgument"));
    }
    return new ServerRegionFunctionExecutor(this, argument);
  }

  @Override
  public void validateExecution(Function function, Set targetMembers) {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null && cache.getTxManager().getTXState() != null) {
      TXStateProxyImpl tx = (TXStateProxyImpl) cache.getTxManager().getTXState();
      tx.getRealDeal(null, region);
      tx.incOperationCount();
    }
  }

  @Override
  public ResultCollector execute(final String functionName) {
    return execute(functionName, getTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  @Override
  public ResultCollector execute(final String functionName, long timeout, TimeUnit unit) {
    if (functionName == null) {
      throw new FunctionException(
          "The input function for the execute function request is null");
    }
    int timeoutInMs = (int) TimeUnit.MILLISECONDS.convert(timeout, unit);
    isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      byte[] functionAttributes = getFunctionAttributes(functionName);

      if (functionAttributes == null) {
        // GEODE-5618: Set authentication properties before executing the internal function.
        try {
          if (proxyCache != null) {
            if (proxyCache.isClosed()) {
              throw proxyCache.getCacheClosedException("Cache is closed for this user.");
            }
            UserAttributes.userAttributes.set(proxyCache.getUserAttributes());
          }

          ServerRegionProxy srp = getServerRegionProxy();
          Object obj = srp.getFunctionAttributes(functionName);
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

  public boolean getExecuteOnBucketSetFlag() {
    return executeOnBucketSet;
  }
}
