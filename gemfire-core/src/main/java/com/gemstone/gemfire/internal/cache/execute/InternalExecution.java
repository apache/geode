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

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;

/**
 * Internal interface for SQLFabric. It has internal methods specific for SQLFabric
 * 
 * @author Yogesh Mahajan
 * @since 5.8LA
 * 
 */
public interface InternalExecution extends Execution {

  public InternalExecution withMemberMappedArgument(
      MemberMappedArgument argument); 

  /**
   * Specifies a data filter of routing objects for selecting the GemFire
   * members to execute the function that are not GemFire keys rather routing
   * objects as determined by resolver. Currently used by SQL fabric for passing
   * routing objects obtained from the custom resolvers.
   * <p>
   * If the set is empty the function is executed on all members that have the
   * {@linkplain FunctionService#onRegion(com.gemstone.gemfire.cache.Region)
   * region defined}.
   * </p>
   * 
   * @param routingObjects
   *          Set defining the routing objects to be used for executing the
   *          function.
   * 
   * @return an Execution with the routing objects
   * 
   * @throws IllegalArgumentException
   *           if the set of routing objects passed is null.
   * @throws UnsupportedOperationException
   *           if not called after
   *           {@link FunctionService#onRegion(com.gemstone.gemfire.cache.Region)}
   */
  public InternalExecution withRoutingObjects(Set<Object> routingObjects);
  
  /**
   * Specifies a  filter of bucketIDs for selecting the GemFire
   * members to execute the function on.
   * <p>
   * Applicable only for regions with {@link DataPolicy#PARTITION} DataPolicy.
   * 
   * 
   * @param bucketIDs
   *          Set of bucketIDs defining the buckets to be used for executing the function
   * @return an Execution with the filter
   * @throws UnsupportedOperationException
   *           if not called after
   *           {@link FunctionService#onRegion(com.gemstone.gemfire.cache.Region)}
   * @since 9.0
   */
  public InternalExecution withBucketFilter(Set<Integer> bucketIDs);
  
  /**
   * If true, function execution waits for all exceptions from target nodes <br>
   * If false, function execution returns when first exception is occurred.
   * 
   * @param setWaitOnException
   */
  public void setWaitOnExceptionFlag(boolean setWaitOnException);
  
  /**
   * Sets the exception delivery flag.  If set, all exceptions will be forwarded
   * directly to the {@link ResultCollector}.  The user will not need to invoke
   * {@link ResultCollector#getResult()} to receive errors.  Setting this flag
   * may interface will proper handling of HA-enabled functions.
   * 
   * @param forward true if all exceptions should be forwarded to the
   *        <code>ResultCollector</code>
   */
  public void setForwardExceptions(boolean forward);

  /**
   * If true, allows results of function execution on groups to be collected in
   * presence of member failures. For the failed member, resultCollector will
   * have an Exception in place of a result, so that information about the
   * failed member can be obtained.
   * 
   * see bug #45765
   * 
   * @param ignore
   *          true to collect results
   */
  public void setIgnoreDepartedMembers(boolean ignore);
}
