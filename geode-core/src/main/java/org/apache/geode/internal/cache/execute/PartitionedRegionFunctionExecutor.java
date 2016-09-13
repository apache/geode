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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * 
 *
 */
public class PartitionedRegionFunctionExecutor extends AbstractExecution {

  private final PartitionedRegion pr;

  private ServerToClientFunctionResultSender sender;

  private boolean executeOnBucketSet = false;
  
  private boolean isPRSingleHop = false;
  
  public PartitionedRegionFunctionExecutor(Region r) {
    if (r == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("region"));
    }
    this.pr = (PartitionedRegion)r;
  }

  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe) {
    super(prfe);
    this.pr = prfe.pr;
    this.executeOnBucketSet = prfe.executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    this.isReExecute = prfe.isReExecute;
    if (prfe.filter != null) {
      this.filter.clear();
      this.filter.addAll(prfe.filter);
    }
    if (prfe.sender != null) {
      this.sender = prfe.sender;
    }
  }

  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe,
      MemberMappedArgument argument) {
    // super copies args, rc and memberMappedArgument
    super(prfe);
    
    this.pr = prfe.pr;
    this.executeOnBucketSet = prfe.executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    this.filter.clear();
    this.filter.addAll(prfe.filter);
    this.sender = prfe.sender;
    // override member mapped arguments
    this.memberMappedArg = argument;
    this.isMemberMappedArgument = true;
  }

  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe,
      ResultCollector rs) {
    // super copies args, rc and memberMappedArgument 
    super(prfe);
    
    this.pr = prfe.pr;
    this.executeOnBucketSet = prfe.executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    this.filter.clear();
    this.filter.addAll(prfe.filter);
    this.sender = prfe.sender;
    // override ResultCollector
    this.rc = rs;
  }

  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe,
      Object arguments) {
    
    // super copies args, rc and memberMappedArgument 
    super(prfe);
    this.pr = prfe.pr;
    this.executeOnBucketSet = prfe.executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    this.filter.clear();
    this.filter.addAll(prfe.filter);
    this.sender = prfe.sender;
    // override arguments
    this.args = arguments;
  }
  
  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe,
      Set filter2) {
    // super copies args, rc and memberMappedArgument 
    super(prfe);
    this.pr = prfe.pr;
    this.executeOnBucketSet = prfe.executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    this.sender = prfe.sender;
    this.filter.clear();
    this.filter.addAll(filter2);
    this.isReExecute = prfe.isReExecute;
  }
  
  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe,
      Set<Integer> bucketsAsFilter, boolean executeOnBucketSet) {
    // super copies args, rc and memberMappedArgument 
    super(prfe);
    this.pr = prfe.pr;
    this.executeOnBucketSet = executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    this.sender = prfe.sender;
    this.filter.clear();
    this.filter.addAll(bucketsAsFilter);
    this.isReExecute = prfe.isReExecute;
  }
  
  private PartitionedRegionFunctionExecutor(
      PartitionedRegionFunctionExecutor prfe, boolean isReExecute) {
    super(prfe);
    this.pr = prfe.pr;
    this.executeOnBucketSet = prfe.executeOnBucketSet;
    this.isPRSingleHop = prfe.isPRSingleHop;
    if (prfe.filter != null) {
      this.filter.clear();
      this.filter.addAll(prfe.filter);
    }
    if (prfe.sender != null) {
      this.sender = prfe.sender;
    }
    this.isReExecute = isReExecute;
    this.isClientServerMode = prfe.isClientServerMode;
    if (prfe.failedNodes != null) {
      this.failedNodes.clear();
      this.failedNodes.addAll(prfe.failedNodes);
    }
  }

  public PartitionedRegionFunctionExecutor(PartitionedRegion region, Set filter2, Object args,
      MemberMappedArgument memberMappedArg, 
      ServerToClientFunctionResultSender resultSender, Set failedNodes,
      boolean executeOnBucketSet) {
    this.pr = region;
    this.sender = resultSender;
    this.isClientServerMode = true ;
    this.executeOnBucketSet = executeOnBucketSet;
    if (filter2 != null) {
      this.filter.clear();
      this.filter.addAll(filter2);
    }
    
    if (args != null) {
      this.args = args;
    }
    else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      this.isMemberMappedArgument = true;
    }
    
    if (failedNodes != null) {
      this.failedNodes.clear();
      this.failedNodes.addAll(failedNodes);
    }  
    
  }
  
  
  public PartitionedRegionFunctionExecutor(PartitionedRegion region,
      Set filter2, Object args, MemberMappedArgument memberMappedArg,
      ServerToClientFunctionResultSender resultSender, Set failedNodes,
      boolean executeOnBucketSet, boolean isPRSingleHop) {
    this.pr = region;
    this.sender = resultSender;
    this.isClientServerMode = true ;
    this.executeOnBucketSet = executeOnBucketSet;
    this.isPRSingleHop = isPRSingleHop;
    if (filter2 != null) {
      this.filter.clear();
      this.filter.addAll(filter2);
    }
    
    if (args != null) {
      this.args = args;
    }
    else if (memberMappedArg != null) {
      this.memberMappedArg = memberMappedArg;
      this.isMemberMappedArgument = true;
    }
    
    if (failedNodes != null) {
      this.failedNodes.clear();
      this.failedNodes.addAll(failedNodes);
    }
  }

  public ResultCollector executeFunction(final Function function) {
    if (function.hasResult()) {
      if (this.rc == null) {
        return this.pr.executeFunction(function, this,
            new DefaultResultCollector(), this.executeOnBucketSet);
      }
      else {
        return this.pr.executeFunction(function, this, rc,
            this.executeOnBucketSet);
      }
    }
    else { /* NO RESULT:fire-n-forget */
      this.pr.executeFunction(function, this, null, this.executeOnBucketSet);
      return new NoResult();
    }
  }

  public Execution withFilter(Set filter) {
    if (filter == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("filter"));
    }
    this.executeOnBucketSet = false;
    return new PartitionedRegionFunctionExecutor(this, filter);
  }
  
  
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs) {
    if (bucketIDs == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("buckets as filter"));
    }
    else if (bucketIDs.isEmpty()) {
    	throw new FunctionException("Bucket IDs list is empty");
    }
    
    Set<Integer> actualBucketSet = pr.getRegionAdvisor().getBucketSet();
    
    bucketIDs.retainAll(actualBucketSet);
   
    Iterator<Integer> it = bucketIDs.iterator();
    while (it.hasNext()) {
      int bid = it.next();
      if (!actualBucketSet.contains(bid)) {
        throw new FunctionException("Bucket " + bid + " does not exist.");
      }
    }
    if (bucketIDs.isEmpty()) {
      throw new FunctionException("No valid buckets to execute on");
    }
    return new PartitionedRegionFunctionExecutor(this, bucketIDs, true);
  }

  public LocalRegion getRegion() {
    return this.pr;
  }
  
  public ServerToClientFunctionResultSender getServerResultSender() {
    return this.sender;
  }
  public Execution withArgs(Object args) {
    if (args == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("args"));
    }
    return new PartitionedRegionFunctionExecutor(this, args); 
  }

  public Execution withCollector(ResultCollector rs) {
    if (rs == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("Result Collector"));
    }
    return new PartitionedRegionFunctionExecutor(this, rs);
  }
  
  public AbstractExecution setIsReExecute() {
    return new PartitionedRegionFunctionExecutor(this, true);
  }
  
  public boolean isPrSingleHop(){
    return this.isPRSingleHop;
  }
  
  public InternalExecution withMemberMappedArgument(
      MemberMappedArgument argument) {
    if (argument == null) {
      throw new FunctionException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("MemberMapped arg"));
    }
    return new PartitionedRegionFunctionExecutor(this,argument);
  }

  @Override
  public String toString() {
    final StringBuffer buf = new StringBuffer();
    buf.append("[ PartitionedRegionFunctionExecutor:");
    buf.append("args=");
    buf.append(this.args);
    buf.append(";filter=");
    buf.append(this.filter);
    buf.append(";region=");
    buf.append(this.pr.getName());
    buf.append("]");
    return buf.toString();
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.execute.AbstractExecution#validateExecution(com.gemstone.gemfire.cache.execute.Function, java.util.Set)
   */
  @Override
  public void validateExecution(Function function, Set targetMembers) {
    GemFireCacheImpl cache = pr.getGemFireCache();
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
          throw new TransactionDataRebalancedException(LocalizedStrings.PartitionedRegion_TX_FUNCTION_EXECUTION_NOT_COLOCATED_0_1
              .toLocalizedString(new Object[] {target,funcTarget}));
        }
      }
    }
    if (function.optimizeForWrite() && cache.getResourceManager().getHeapMonitor().
        containsHeapCriticalMembers(targetMembers) &&
        !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      Set<InternalDistributedMember> hcm  = cache.getResourceAdvisor().adviseCritialMembers();
      Set<DistributedMember> sm = SetUtils.intersection(hcm, targetMembers);
      throw new LowMemoryException(LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1.toLocalizedString(
          new Object[] {function.getId(), sm}), sm);
    }
  }
}
