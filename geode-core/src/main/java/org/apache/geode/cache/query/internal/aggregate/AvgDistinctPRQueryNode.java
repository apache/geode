/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal.aggregate;

/**
 * Computes the final average of distinct values for the partitioned region based queries. This
 * aggregator is initialized on the PR query node & acts on the results obtained from bucket nodes.
 */
public class AvgDistinctPRQueryNode extends SumDistinctPRQueryNode {

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / distinct.size();
    return downCast(result);
  }
}
