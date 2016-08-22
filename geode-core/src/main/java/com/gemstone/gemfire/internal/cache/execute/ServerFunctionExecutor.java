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

import java.util.Set;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.internal.ExecuteFunctionNoAckOp;
import com.gemstone.gemfire.cache.client.internal.ExecuteFunctionOp;
import com.gemstone.gemfire.cache.client.internal.GetFunctionAttributeOp;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ProxyCache;
import com.gemstone.gemfire.cache.client.internal.UserAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
/**
 * 
 *
 */
public class ServerFunctionExecutor extends AbstractExecution {

  private PoolImpl pool;

  private final boolean allServers;
  
  private String[] groups;

  public ServerFunctionExecutor(Pool p, boolean allServers, String... groups) {
    this.pool = (PoolImpl)p;
    this.allServers = allServers;
    this.groups = groups;
  }

  public ServerFunctionExecutor(Pool p, boolean allServers, ProxyCache proxyCache, String... groups) {
    this.pool = (PoolImpl)p;
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
  
  private ServerFunctionExecutor(ServerFunctionExecutor sfe,Object args) {
    this(sfe);
    this.args = args;
  }
  
  private ServerFunctionExecutor(ServerFunctionExecutor sfe, ResultCollector rs) {
    this(sfe);
    this.rc = rs;
  }
  
  private ServerFunctionExecutor(ServerFunctionExecutor sfe, MemberMappedArgument argument) {
    this(sfe);
    this.memberMappedArg = argument;
    this.isMemberMappedArgument = true;
  }
  
  protected ResultCollector executeFunction(final String functionId, boolean result, boolean isHA, boolean optimizeForWrite) {
    try {
      if (proxyCache != null) {
        if (this.proxyCache.isClosed()) {
          throw new CacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
      }

    byte hasResult = 0;
    if (result) { // have Results
      hasResult = 1;
      if (this.rc == null) { // Default Result Collector
        ResultCollector defaultCollector = new DefaultResultCollector();
        return executeOnServer(functionId, defaultCollector, hasResult, isHA, optimizeForWrite);
      }
      else { // Custome Result COllector
        return executeOnServer(functionId, this.rc, hasResult, isHA, optimizeForWrite);
      }
    }
    else { // No results
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
          throw new CacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
      }

      if (function.hasResult()) { // have Results
        hasResult = 1;
        if (this.rc == null) { // Default Result Collector
          ResultCollector defaultCollector = new DefaultResultCollector();
          return executeOnServer(function, defaultCollector, hasResult);
        }
        else { // Custome Result COllector
          return executeOnServer(function, this.rc, hasResult);
        }
      }
      else { // No results
        executeOnServerNoAck(function, hasResult);
        return new NoResult();
      }
    }
    finally {
      UserAttributes.userAttributes.set(null);
    }

  }

  private ResultCollector executeOnServer(Function function,
      ResultCollector rc, byte hasResult) {
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId(), null);
    try {
      validateExecution(function, null);
      long start = stats.startTime();
      stats.startFunctionExecution(true);
      ExecuteFunctionOp.execute(this.pool, function, this, args, memberMappedArg,
          this.allServers, hasResult, rc, this.isFnSerializationReqd,
          UserAttributes.userAttributes.get(), groups);
      stats.endFunctionExecution(start, true);
      rc.endResults();
      return rc;
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(true);
      throw functionException;
    }
    catch(ServerConnectivityException exception){
      throw exception;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(true);
      throw new FunctionException(exception);
    }
  }
  
  private ResultCollector executeOnServer(String functionId,
      ResultCollector rc, byte hasResult, boolean isHA, boolean optimizeForWrite) {
    FunctionStats stats = FunctionStats.getFunctionStats(functionId, null);
    try {
      validateExecution(null, null);
      long start = stats.startTime();
      stats.startFunctionExecution(true);
      ExecuteFunctionOp.execute(this.pool, functionId,this, args, memberMappedArg,
          this.allServers, hasResult, rc, this.isFnSerializationReqd, isHA, optimizeForWrite, UserAttributes.userAttributes.get(), groups);
      stats.endFunctionExecution(start, true);
      rc.endResults();
      return rc;
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(true);
      throw functionException;
    }
    catch(ServerConnectivityException exception){
      throw exception;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(true);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(Function function, byte hasResult) {
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId(), null);
    try {
      validateExecution(function, null);
      long start = stats.startTime();
      stats.startFunctionExecution(false);
      ExecuteFunctionNoAckOp.execute(this.pool, function, args,
          memberMappedArg, this.allServers, hasResult, this.isFnSerializationReqd, groups);
      stats.endFunctionExecution(start, false);
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(false);
      throw functionException;
    }
    catch(ServerConnectivityException exception){
      throw exception;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(false);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(String functionId, byte hasResult,
      boolean isHA, boolean optimizeForWrite) {
    FunctionStats stats = FunctionStats.getFunctionStats(functionId, null);
    try {
      validateExecution(null, null);
      long start = stats.startTime();
      stats.startFunctionExecution(false);
      ExecuteFunctionNoAckOp.execute(this.pool, functionId, args,
          memberMappedArg, this.allServers, hasResult, this.isFnSerializationReqd, isHA, optimizeForWrite, groups);
      stats.endFunctionExecution(start, false);
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(false);
      throw functionException;
    }
    catch(ServerConnectivityException exception){
      throw exception;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(false);
      throw new FunctionException(exception);
    }
  }
  
  public Pool getPool() {
    return this.pool;
  }

  public boolean getAllServers() {
    return this.allServers;
  }

  public Execution withFilter(Set filter) {
    throw new FunctionException(
        LocalizedStrings.ExecuteFunction_CANNOT_SPECIFY_0_FOR_DATA_INDEPENDENT_FUNCTIONS
            .toLocalizedString("filter"));
  }
  
  @Override
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    throw new FunctionException(
        LocalizedStrings.ExecuteFunction_CANNOT_SPECIFY_0_FOR_DATA_INDEPENDENT_FUNCTIONS
            .toLocalizedString("buckets as filter"));
  }

  public Execution withArgs(Object args) {
    if (args == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("args"));
      }
    return new ServerFunctionExecutor(this, args);
  }

  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Result Collector"));
    }
    return new ServerFunctionExecutor(this, rs);
  }

  public InternalExecution withMemberMappedArgument(
      MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("MemberMapped Args"));
    }
    return new ServerFunctionExecutor(this, argument);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.execute.AbstractExecution#validateExecution(com.gemstone.gemfire.cache.execute.Function, java.util.Set)
   */
  @Override
  public void validateExecution(Function function, Set targetMembers) {
    if (TXManagerImpl.getCurrentTXUniqueId() != TXManagerImpl.NOTX) {
      throw new UnsupportedOperationException();
    }
  }

  public ResultCollector execute(final String functionName) {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    this.isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      byte[] functionAttributes = getFunctionAttributes(functionName);
      if (functionAttributes == null) {
        Object obj = GetFunctionAttributeOp.execute(this.pool, functionName);
        functionAttributes = (byte[])obj;
        addFunctionAttributes(functionName, functionAttributes);
      }
      boolean hasResult = ((functionAttributes[0] == 1) ? true : false);
      boolean isHA = ((functionAttributes[1] == 1) ? true : false);
      boolean optimizeForWrite = ((functionAttributes[2] == 1) ? true : false);
      return executeFunction(functionName, hasResult, isHA, optimizeForWrite);
    }
    else {
      return executeFunction(functionObject);
    }
  }

  public ResultCollector execute(String functionName, boolean hasResult)
      throws FunctionException {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    this.isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      return executeFunction(functionName,hasResult, hasResult, false);   
    }
    else {
      byte registeredFunctionState = AbstractExecution.getFunctionState(
          functionObject.isHA(), functionObject.hasResult(), functionObject
              .optimizeForWrite());

      byte functionState = AbstractExecution.getFunctionState(hasResult, hasResult, false);
      if (registeredFunctionState != functionState) {
        throw new FunctionException(
            LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
                .toLocalizedString(functionName));
      }
      return executeFunction(functionObject);
    }
  }
  
  public ResultCollector execute(String functionName, boolean hasResult, boolean isHA) throws FunctionException {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    this.isFnSerializationReqd = false;
    if (isHA && !hasResult) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH
              .toLocalizedString());
    }
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      return executeFunction(functionName,hasResult, isHA, false);
    }
    else{
      byte registeredFunctionState = AbstractExecution.getFunctionState(
          functionObject.isHA(), functionObject.hasResult(), functionObject
              .optimizeForWrite());

      byte functionState = AbstractExecution.getFunctionState(isHA, hasResult, false);
      if (registeredFunctionState != functionState) {
        throw new FunctionException(
            LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
                .toLocalizedString(functionName));
      }
      return executeFunction(functionObject);
    }
  }
  
  public ResultCollector execute(String functionName, boolean hasResult, boolean isHA, boolean isOptimizeForWrite) throws FunctionException {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    this.isFnSerializationReqd = false;
    if (isHA && !hasResult) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH
              .toLocalizedString());
    }
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      return executeFunction(functionName,hasResult, isHA, isOptimizeForWrite);
    }
    else {
      byte registeredFunctionState = AbstractExecution.getFunctionState(
          functionObject.isHA(), functionObject.hasResult(), functionObject
              .optimizeForWrite());

      byte functionState = AbstractExecution.getFunctionState(isHA, hasResult, isOptimizeForWrite);
      if (registeredFunctionState != functionState) {
        throw new FunctionException(
            LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
                .toLocalizedString(functionName));
      }
      return executeFunction(functionObject);      
    }
  }
}
