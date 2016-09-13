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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.ResultSender;

/**
 * Context available when called using
 * {@link InternalFunctionService#onRegions(Set)}
 * 
 * 
 * @since GemFire 6.5
 * 
 */
public class MultiRegionFunctionContextImpl extends FunctionContextImpl
    implements MultiRegionFunctionContext {

  private Set<Region> regions = null;
  
  private final boolean isPossibleDuplicate;

  public MultiRegionFunctionContextImpl(final String functionId,
      final Object args, ResultSender resultSender, Set<Region> regions,
      boolean isPossibleDuplicate) {
    super(functionId, args, resultSender);
    this.regions = regions;
    this.isPossibleDuplicate = isPossibleDuplicate;
  }

  public Set<Region> getRegions() {
    return regions;
  }
  
  public boolean isPossibleDuplicate() {
    return isPossibleDuplicate;
  }

}
