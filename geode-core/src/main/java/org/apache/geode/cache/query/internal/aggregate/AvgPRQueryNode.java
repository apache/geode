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
 * Computes the final non distinct average for a partitioned region based query. This aggregator is
 * instantiated on the PR query node.
 */
public class AvgPRQueryNode extends Sum {
  private long count = 0;

  long getCount() {
    return count;
  }

  /**
   * Takes the input of data received from bucket nodes. The data is of the form of two element
   * array. The first element is the number of values, while the second element is the sum of the
   * values.
   */
  @Override
  public void accumulate(Object value) {
    Object[] array = (Object[]) value;
    count += ((Long) array[0]);
    super.accumulate(array[1]);
  }

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / count;
    return downCast(result);
  }
}
