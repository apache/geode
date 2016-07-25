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
package com.gemstone.gemfire.cache.execute;

import java.util.Set;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

/**
 * Defines the execution context of a data dependent {@link Function}.
 * 
 * Applications can use the methods provided to retrieve the Region 
 * and context specific routing objects.  When the function is executed 
 * using {@link FunctionService#onRegion(Region)}, the {@link Function#execute(FunctionContext) FunctionContext}
 * can be typecast to this type.
 * </p>
 * <p>
 * If the region is a partitioned region,
 * {@link PartitionRegionHelper} can be used to get local and 
 * {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated} data references.
 * </p>
 * 
 * 
 * @since GemFire 6.0
 * 
 * @see FunctionContext
 * @see PartitionRegionHelper
 */
public interface RegionFunctionContext extends FunctionContext {

  /**
   * Returns subset of keys (filter) provided by the invoking thread (aka routing
   * objects). The set of filter keys are locally present in the datastore 
   * on the executing cluster member.
   * 
   * @see Execution#withFilter(Set)
   * 
   * @return the objects that caused the function to be routed to this cluster member
   * @since GemFire 6.0
   */
  public Set<?> getFilter();

  /**
   * Returns the reference to the Region on which the function is executed
   * 
   * @see FunctionService#onRegion(Region)
   * 
   * @return returns the Region on which the function is executed
   * 
   * @since GemFire 6.0
   */
  public <K, V> Region<K, V> getDataSet();
  
}
