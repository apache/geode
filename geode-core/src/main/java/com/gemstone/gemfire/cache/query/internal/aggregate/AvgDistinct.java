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

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the average of distinct values for replicated region based queries.
 * 
 *
 */
public class AvgDistinct extends SumDistinct {

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      super.accumulate(value);
    }
  }

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / this.distinct.size();
    return downCast(result);
  }

}
