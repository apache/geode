/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.execute;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;

public interface FunctionExecutionServiceOnRegions {

  /**
   * Returns an {@link Execution} object that can be used to execute a function on the set of {@link
   * Region}s. The function would be executed on the set of members that host data for any of the
   * regions in the set of regions. <br>
   * If the Set provided contains region with : <br>
   * DataPolicy.NORMAL, execute the function on any random member which has DataPolicy.REPLICATE .
   * <br>
   * DataPolicy.EMPTY, execute the function on any random member which has DataPolicy.REPLICATE .
   * <br>
   * DataPolicy.REPLICATE, execute the function locally or any random member which has
   * DataPolicy.REPLICATE .<br>
   * DataPolicy.PARTITION, it executes on members where the primary copy of data is hosted. <br>
   * This API is not supported for cache clients in client server mode
   *
   * <p>
   * For an Execution object obtained from this method, calling the withFilter method throws
   * {@link UnsupportedOperationException}
   *
   * @see org.apache.geode.internal.cache.execute.MultiRegionFunctionContext
   */
  Execution onRegions(Set<Region> regions);
}
