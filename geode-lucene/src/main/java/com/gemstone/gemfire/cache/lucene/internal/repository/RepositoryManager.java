/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.util.Collection;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;

/**
 * {@link RepositoryManager} instances will be used to get {@link IndexRepository} instances hosting index data for
 * {@link Region}s
 */
public interface RepositoryManager {

  IndexRepository getRepository(Region region, Object key, Object callbackArg) throws BucketNotFoundException;

  /**
   * Returns a collection of {@link IndexRepository} instances hosting index data of the input list of bucket ids. The
   * bucket needs to be present on this member.
   * 
   * @return a collection of {@link IndexRepository} instances
   * @throws BucketNotFoundException if any of the requested buckets is not found on this member
   */
  Collection<IndexRepository> getRepositories(RegionFunctionContext context) throws BucketNotFoundException;
}
