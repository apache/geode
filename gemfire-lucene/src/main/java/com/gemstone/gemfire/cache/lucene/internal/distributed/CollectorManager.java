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

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.Collection;

import com.gemstone.gemfire.annotations.Experimental;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;

/**
 * {@link CollectorManager}s create instances of {@link IndexResultCollector} and utility methods to aggregate results
 * collected by individual collectors. The collectors created by this class are primarily used for collecting results
 * from {@link IndexRepository}s. The collectors can also be used for aggregating results of other collectors of same
 * type. Typically search result aggregation is completed in two phases. First at a member level for merging results
 * from local buckets. And then at search coordinator level for merging results from members. Use of same collector in
 * both phases is expected to produce deterministic merge result irrespective of the way buckets are distributed.
 * 
 * @param <C> Type of IndexResultCollector created by this manager
 */
@Experimental
public interface CollectorManager<C extends IndexResultCollector> {
  /**
   * @param name Name/Identifier for this collector. For e.g. region/bucketId.
   * @return a new {@link IndexResultCollector}. This must return a different instance on
   *         each call. A new collector would be created for each bucket on a member node.
   */
  C newCollector(String name);

  /**
   * Reduce the results of individual collectors into a meaningful result. This method must be called after collection
   * is finished on all provided collectors.
   * 
   * @throws IOException
   */
  C reduce(Collection<C> results) throws IOException;
}
