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
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.i18n.LocalizedStrings;
/**
 * Executes Function on Distributed Regions.
 * 
 * For DistributedRegions with DataPolicy.NORMAL, it throws
 * UnsupportedOperationException. <br>
 * For DistributedRegions with DataPolicy.EMPTY, execute the function on any
 * random member which has DataPolicy.REPLICATE <br>
 * For DistributedRegions with DataPolicy.REPLICATE, execute the function
 * locally.
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
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("region"));
    }
    this.region = (LocalRegion)r;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor drfe) {
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
    }
    else if (memberMappedArg != null) {
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
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor,
      ResultCollector rs) {    super(distributedRegionFunctionExecutor);
      
      this.region = distributedRegionFunctionExecutor.getRegion();
      this.filter.clear();
      this.filter.addAll(distributedRegionFunctionExecutor.filter);
      this.sender = distributedRegionFunctionExecutor.getServerResultSender();
      
      this.rc = rs;
  }

  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor,
      Object args) {
    super(distributedRegionFunctionExecutor);
    
    this.region = distributedRegionFunctionExecutor.getRegion();
    this.filter.clear();
    this.filter.addAll(distributedRegionFunctionExecutor.filter);
    this.sender = distributedRegionFunctionExecutor.getServerResultSender();
    
    this.args = args;
  }
  
  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor distributedRegionFunctionExecutor,
      Set filter2) {
    super(distributedRegionFunctionExecutor);
    
    this.region = distributedRegionFunctionExecutor.getRegion();
    this.sender = distributedRegionFunctionExecutor.getServerResultSender();
    
    this.filter.clear();
    this.filter.addAll(filter2);
  }
  
  private DistributedRegionFunctionExecutor(
      DistributedRegionFunctionExecutor drfe, boolean isReExecute) {
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
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    this.isFnSerializationReqd = false;
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
              .toLocalizedString(functionObject));
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_CAN_NOT_EXECUTE_ON_NORMAL_REGION
              .toLocalizedString());
    }
    return executeFunction(functionObject);
  }
  
  public ResultCollector execute(String functionName, boolean hasResult)
      throws FunctionException {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
              .toLocalizedString(functionObject));
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_CAN_NOT_EXECUTE_ON_NORMAL_REGION
              .toLocalizedString());
    }
    byte registeredFunctionState = AbstractExecution.getFunctionState(
        functionObject.isHA(), functionObject.hasResult(), functionObject
            .optimizeForWrite());

    byte functionState = AbstractExecution.getFunctionState(hasResult,
        hasResult, false);
    if (registeredFunctionState != functionState) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
              .toLocalizedString(functionName));
    }

    this.isFnSerializationReqd = false;
    // If hasResult is true, isHA will also be true and hasResult is false then
    // isHA will be false
    // For other combination use next API
    return executeFunction(functionObject);
  }

  public ResultCollector execute(String functionName, boolean hasResult,
      boolean isHA) throws FunctionException {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    if (isHA && !hasResult) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH
              .toLocalizedString());
    }
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
              .toLocalizedString(functionObject));
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_CAN_NOT_EXECUTE_ON_NORMAL_REGION
              .toLocalizedString());
    }
    byte registeredFunctionState = AbstractExecution.getFunctionState(
        functionObject.isHA(), functionObject.hasResult(), functionObject
            .optimizeForWrite());

    byte functionState = AbstractExecution.getFunctionState(isHA, hasResult,
        false);
    if (registeredFunctionState != functionState) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
              .toLocalizedString(functionName));
    }

    this.isFnSerializationReqd = false;
    return executeFunction(functionObject);
  }

  public ResultCollector execute(String functionName, boolean hasResult,
      boolean isHA, boolean isOptimizeForWrite) throws FunctionException {
    if (functionName == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString());
    }
    if (isHA && !hasResult) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH
              .toLocalizedString());
    }
    Function functionObject = FunctionService.getFunction(functionName);
    if (functionObject == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
              .toLocalizedString(functionObject));
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_CAN_NOT_EXECUTE_ON_NORMAL_REGION
              .toLocalizedString());
    }
    byte registeredFunctionState = AbstractExecution.getFunctionState(
        functionObject.isHA(), functionObject.hasResult(), functionObject
            .optimizeForWrite());

    byte functionState = AbstractExecution.getFunctionState(isHA, hasResult,
        isOptimizeForWrite);
    if (registeredFunctionState != functionState) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
              .toLocalizedString(functionName));
    }

    this.isFnSerializationReqd = false;
    return executeFunction(functionObject);
  }

  @Override
  public ResultCollector execute(final Function function){
    if (function == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
          .toLocalizedString("function instance"));
    }
    if (function.isHA() && !function.hasResult()) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH
              .toLocalizedString());
    }
    if (region.getAttributes().getDataPolicy().isNormal()) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_CAN_NOT_EXECUTE_ON_NORMAL_REGION
              .toLocalizedString());
    }
    String id = function.getId();
    if (id == null) {
      throw new FunctionException(LocalizedStrings.ExecuteFunction_THE_FUNCTION_GET_ID_RETURNED_NULL.toLocalizedString());
    }
    this.isFnSerializationReqd = true;
    return executeFunction(function);
  }

  @Override
  protected ResultCollector executeFunction(Function function){
    if (function.hasResult()) { // have Results
      if (this.rc == null) { // Default Result Collector
        ResultCollector defaultCollector = new DefaultResultCollector();
        return this.region.executeFunction(this,function, args, defaultCollector,
            this.filter, this.sender);
      }
      else { // Custome Result COllector
        return this.region.executeFunction(this, function, args, rc, this.filter,
            this.sender);
      }
    }
    else { // No results
      this.region.executeFunction(this,function, args, null, this.filter,
          this.sender);
      return new NoResult();
    }
  }

  public Execution withFilter(Set filter) {
    if (filter == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("filter"));
    }
    
    return new DistributedRegionFunctionExecutor(this,filter);
  }
  
  @Override
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    if(bucketIDs != null && !bucketIDs.isEmpty()) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_BUCKET_FILTER_ON_NON_PR.toLocalizedString(region.getName()));      
    }
    return this;
  }

  public LocalRegion getRegion() {
    return this.region;
  }

  public ServerToClientFunctionResultSender getServerResultSender() {
    return this.sender;
  }
  public Execution withArgs(Object args) {
    if (args == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Args"));
    }
    return new DistributedRegionFunctionExecutor(this, args); 
  }

  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Result Collector"));
    }
    return new DistributedRegionFunctionExecutor(this,rs);
  }

  public InternalExecution withMemberMappedArgument(
      MemberMappedArgument argument) {
    if (argument == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
          .toLocalizedString("MemberMappedArgument"));
    }
    return new DistributedRegionFunctionExecutor(this,argument);
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
  /* (non-Javadoc)
   * @see org.apache.geode.internal.cache.execute.AbstractExecution#validateExecution(org.apache.geode.cache.execute.Function, java.util.Set)
   */
  @Override
  public void validateExecution(Function function, Set targetMembers) {
    GemFireCacheImpl cache = region.getGemFireCache();
    if (cache != null && cache.getTxManager().getTXState() != null) {
      if (targetMembers.size() > 1) {
        throw new TransactionException(LocalizedStrings.PartitionedRegion_TX_FUNCTION_ON_MORE_THAN_ONE_NODE
            .toLocalizedString());
      } else {
        assert targetMembers.size() == 1;
        DistributedMember funcTarget = (DistributedMember)targetMembers.iterator().next();
        DistributedMember target = cache.getTxManager().getTXState().getTarget();
        if (target == null) {
          cache.getTxManager().getTXState().setTarget(funcTarget);
        } else if (!target.equals(funcTarget)) {
          throw new TransactionDataNotColocatedException(LocalizedStrings.PartitionedRegion_TX_FUNCTION_EXECUTION_NOT_COLOCATED_0_1
              .toLocalizedString(new Object[] {target,funcTarget}));
        }
      }
    }
    if (!MemoryThresholds.isLowMemoryExceptionDisabled() && function.optimizeForWrite()) {
      try {
        region.checkIfAboveThreshold(null);
      } catch (LowMemoryException e) {
        Set<DistributedMember> htrm = region.getMemoryThresholdReachedMembers();
        throw new LowMemoryException(LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1.toLocalizedString(
            new Object[] {function.getId(), htrm}), htrm);

      }
    }
  }

}
