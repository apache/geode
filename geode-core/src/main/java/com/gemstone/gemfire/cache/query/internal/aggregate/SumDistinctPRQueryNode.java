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
package com.gemstone.gemfire.cache.query.internal.aggregate;

import java.util.Set;

/**
 * Computes the sum of distinct values on the PR query node.
 * 
 *
 */
public class SumDistinctPRQueryNode extends DistinctAggregator {

  /**
   * The input data is the Set of values(distinct) receieved from each of the
   * bucket nodes.
   */
  @Override
  public void accumulate(Object value) {
    this.distinct.addAll((Set) value);
  }

  @Override
  public Object terminate() {
    double sum = 0;
    for (Object o : this.distinct) {
      sum += ((Number) o).doubleValue();
    }
    return downCast(sum);
  }
}
