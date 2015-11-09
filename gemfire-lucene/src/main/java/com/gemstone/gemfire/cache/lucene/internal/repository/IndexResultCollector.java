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

import com.gemstone.gemfire.annotations.Experimental;

/**
 * Interface for collection results of a query on
 * an IndexRepository. See {@link IndexRepository#query(org.apache.lucene.search.Query, int, IndexResultCollector)}
 */
@Experimental
public interface IndexResultCollector {
  /**
   * @return Name/identifier of this collector
   */
  public String getName();

  /**
   * @return Number of results collected by this collector
   */
  public int size();

  /**
   * Collect a single document
   * 
   * @param key the gemfire key of the object
   * @param score the lucene score of this object
   */
  void collect(Object key, float score);
}
