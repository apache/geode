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
package com.gemstone.gemfire.cache.query;

/**
 * Behavior of a user-defined aggregator. Aggregates values and returns a
 * result. In addition to the methods in the interface, implementing classes
 * must have a 0-arg public constructor.
 * 
 * For replicated Regions, it is necessary to implement required functionality in {@link #accumulate(Object)} and {@link #terminate()}
 * 
 * For PartitionedRegions, the aggregator Objects are themselves serialized from the
 * bucket nodes to the query node. On the query node, the aggregators are merged
 * in {@link #merge(Aggregator)}
 * 
 * For PartitionedRegions, the aggregator class needs to be serializable
 * 
 * 
 * @author ashahid
 * @since 9.0
 */
public interface Aggregator {

  /**
   * Accumulate the next scalar value
   * 
   * @param value
   */
  public void accumulate(Object value);

  /**
   * Initialize the Aggregator
   */
  public void init();

  /**
   * 
   * @return Return the result scalar value
   */
  public Object terminate();

  /**
   * Merges the incoming aggregator from bucket nodes with the resultant aggregator
   * on the query node
   * 
   * @param otherAggregator
   */
  public void merge(Aggregator otherAggregator);
}
