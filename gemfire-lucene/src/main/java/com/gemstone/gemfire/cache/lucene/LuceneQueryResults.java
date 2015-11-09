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

package com.gemstone.gemfire.cache.lucene;

import java.util.List;

import com.gemstone.gemfire.annotations.Experimental;

/**
 * <p>
 * Defines the interface for a container of lucene query result collected from function execution.<br>
 * 
 * @author Xiaojian Zhou
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
@Experimental
public interface LuceneQueryResults<K, V> {
  /**
   * @return total number of hits for this query
   */
  public int size();

  /**
   * Returns the maximum score value encountered. Note that in case scores are not tracked, this returns {@link Float#NaN}.
   */
  public float getMaxScore();

  /**
   * Get the next page of results.
   * 
   * @return a page of results, or null if there are no more pages
   */
  public List<LuceneResultStruct<K, V>> getNextPage();

  /**
   *  True if there another page of results. 
   */
  public boolean hasNextPage();
}
