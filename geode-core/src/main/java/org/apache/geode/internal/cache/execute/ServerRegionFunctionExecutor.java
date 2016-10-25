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

package org.apache.geode.internal.cache.execute;

import org.apache.geode.cache.CacheClosedException;
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
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

import java.util.Set;

import org.apache.logging.log4j.Logger;

/**
 *
 * Executes Function with FunctionService#onRegion(Region region) in client server mode. 
 * 
 * @see FunctionService#onRegion(Region) * 
 * @since GemFire 5.8 LA
 *
 */
public class ServerRegionFunctionExecutor extends AbstractExecution {
  private static final Logger logger = LogService.getLogger();

  final private LocalRegion region;
  private boolean executeOnBucketSet = false;
  
  public ServerRegionFunctionExecutor(Region r, ProxyCache proxyCache) {
    if (r == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Region"));
    }
    this.region = (LocalRegion)r;    
    this.proxyCache = proxyCache;
  }
  
//  private ServerRegionFunctionExecutor(ServerRegionFunctionExecutor sre) {
//    super(sre);
//    this.region = sre.region;
//    if (sre.filter != null) {
//      this.filter.clear();
//      this.filter.addAll(sre.filter);
//    }
//  }

  private ServerRegionFunctionExecutor(
      ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      Object args) {
    super(serverRegionFunctionExecutor);
    
    this.region = serverRegionFunctionExecutor.region;
    this.filter.clear();
    this.filter.addAll(serverRegionFunctionExecutor.filter);
    this.args = args;
    this.executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }
  
  private ServerRegionFunctionExecutor(
      ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      MemberMappedArgument memberMapargs) {
    super(serverRegionFunctionExecutor);
    
    this.region = serverRegionFunctionExecutor.region;
    this.filter.clear();
    this.filter.addAll(serverRegionFunctionExecutor.filter);
    this.memberMappedArg = memberMapargs;
    this.executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }
  

  private ServerRegionFunctionExecutor(
      ServerRegionFunctionExecutor serverRegionFunctionExecutor,
      ResultCollector rc) {
    super(serverRegionFunctionExecutor);
    
    this.region = serverRegionFunctionExecutor.region;
    this.filter.clear();
    this.filter.addAll(serverRegionFunctionExecutor.filter);
    this.rc = rc;
    this.executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }

  private ServerRegionFunctionExecutor(
      ServerRegionFunctionExecutor serverRegionFunctionExecutor, Set filter2) {
    
    super(serverRegionFunctionExecutor);
    
    this.region = serverRegionFunctionExecutor.region;
    this.filter.clear();
    this.filter.addAll(filter2);
    this.executeOnBucketSet = serverRegionFunctionExecutor.executeOnBucketSet;
  }
  
  private ServerRegionFunctionExecutor(
      ServerRegionFunctionExecutor serverRegionFunctionExecutor, 
      Set<Integer> bucketsAsFilter, boolean executeOnBucketSet) {
    
    super(serverRegionFunctionExecutor);
    
    this.region = serverRegionFunctionExecutor.region;
    this.filter.clear();
    this.filter.addAll(bucketsAsFilter);
    this.executeOnBucketSet = executeOnBucketSet;
  }

  public Execution withFilter(Set fltr) {
    if (fltr == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("filter"));
    }
    this.executeOnBucketSet = false;
    return new ServerRegionFunctionExecutor(this, fltr);
  }
  
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    if (bucketIDs == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("buckets as filter"));
    }
    return new ServerRegionFunctionExecutor(this, bucketIDs, true /* execute on bucketset*/);
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

  protected ResultCollector executeFunction(final String functionId,
      boolean resultReq, boolean isHA, boolean optimizeForWrite) {
    try {
      if (proxyCache != null) {
        if (this.proxyCache.isClosed()) {
          throw new CacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
      }
    byte hasResult = 0;
    if (resultReq) { // have Results
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
    }
    finally {
      UserAttributes.userAttributes.set(null);
    }
  }
  
  private ResultCollector executeOnServer(Function function,
      ResultCollector collector, byte hasResult) throws FunctionException {
    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId(),
        this.region.getSystem());
    try {
      validateExecution(function, null);
      long start = stats.startTime();
      stats.startFunctionExecution(true);
      srp.executeFunction(this.region.getFullPath(), function, this, collector,
            hasResult, false);
      stats.endFunctionExecution(start, true);
      return collector;
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(true);
      throw functionException;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(true);
      throw new FunctionException(exception);
    }
  }

  private ResultCollector executeOnServer(String functionId,
      ResultCollector collector, byte hasResult, boolean isHA, boolean optimizeForWrite) throws FunctionException {

    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats = FunctionStats.getFunctionStats(functionId,
        this.region.getSystem());
    try {
      validateExecution(null, null);
      long start = stats.startTime();
      stats.startFunctionExecution(true);
      srp.executeFunction(this.region.getFullPath(), functionId, this, collector,
            hasResult, isHA, optimizeForWrite, false);
      stats.endFunctionExecution(start, true);
      return collector;
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(true);
      throw functionException;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(true);
      throw new FunctionException(exception);
    }
  }

  
  private void executeOnServerNoAck(Function function, byte hasResult)
      throws FunctionException {
    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats = FunctionStats.getFunctionStats(function.getId(),
        this.region.getSystem());
    try {
      validateExecution(function, null);
      long start = stats.startTime();
      stats.startFunctionExecution(false);
      srp.executeFunctionNoAck(this.region.getFullPath(), function, this,
            hasResult, false);
      stats.endFunctionExecution(start, false);
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(false);
      throw functionException;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(false);
      throw new FunctionException(exception);
    }
  }

  private void executeOnServerNoAck(String functionId, byte hasResult,
      boolean isHA, boolean optimizeForWrite) throws FunctionException {
    ServerRegionProxy srp = getServerRegionProxy();
    FunctionStats stats = FunctionStats.getFunctionStats(functionId,
        this.region.getSystem());
    try {
      validateExecution(null, null);
      long start = stats.startTime();
      stats.startFunctionExecution(false);
      srp.executeFunctionNoAck(this.region.getFullPath(), functionId, this,
            hasResult, isHA, optimizeForWrite, false);
      stats.endFunctionExecution(start, false);
    }
    catch(FunctionException functionException){
      stats.endFunctionExecutionWithException(false);
      throw functionException;
    }
    catch (Exception exception) {
      stats.endFunctionExecutionWithException(false);
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
    }
    else {
      StringBuffer message = new StringBuffer();
      message.append(srp).append(": ");
      message
          .append("No available connection was found. Server Region Proxy is not available for this region "
              + region.getName());
      throw new FunctionException(message.toString());
    }
  }

  public LocalRegion getRegion() {
    return this.region;
  }

  @Override
  public String toString() {
    return new StringBuffer().append("[ ServerRegionExecutor:").append("args=")
        .append(this.args).append(" ;filter=").append(this.filter).append(
            " ;region=").append(this.region.getName()).append("]").toString();
  }
  public Execution withArgs(Object params) {
    if (params == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("args"));
    }
    return new ServerRegionFunctionExecutor(this, params); 
  }

  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Result Collector"));
    }
    return new ServerRegionFunctionExecutor(this, rs);
  }

  public InternalExecution withMemberMappedArgument(
      MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("MemberMappedArgument"));
    }
    return new ServerRegionFunctionExecutor(this, argument);
  }
  /* (non-Javadoc)
   * @see org.apache.geode.internal.cache.execute.AbstractExecution#validateExecution(org.apache.geode.cache.execute.Function, java.util.Set)
   */
  @Override
  public void validateExecution(Function function, Set targetMembers) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && cache.getTxManager().getTXState() != null) {
      TXStateProxyImpl tx = (TXStateProxyImpl) cache.getTxManager().getTXState();
      tx.getRealDeal(null, region);
      tx.incOperationCount();
    }
  }
  
  
  @Override
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
        ServerRegionProxy srp = getServerRegionProxy();
        Object obj = srp.getFunctionAttributes(functionName);
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

  @Override
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
  @Override
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
  @Override
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
  
  public boolean getExecuteOnBucketSetFlag() {
    return this.executeOnBucketSet;
  }
  
 
}
