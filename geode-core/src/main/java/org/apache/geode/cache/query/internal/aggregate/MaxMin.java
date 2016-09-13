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

import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the Max or Min
 * 
 *
 */

public class MaxMin implements Aggregator {
  private final boolean findMax;
  private Comparable currentOptima;

  public MaxMin(boolean findMax) {
    this.findMax = findMax;
  }

  @Override
  public void accumulate(Object value) {
    if (value == null || value == QueryService.UNDEFINED) {
      return;
    }
    Comparable comparable = (Comparable) value;
    
    if (currentOptima == null) {
      currentOptima = comparable;
    } else {
      int compare = currentOptima.compareTo(comparable);
      if (findMax) {
        currentOptima = compare < 0 ? comparable : currentOptima;
      } else {
        currentOptima = compare > 0 ? comparable : currentOptima;
      }
    }

  }

  @Override
  public void init() {
    // TODO Auto-generated method stub

  }

  @Override
  public Object terminate() {
    return currentOptima;
  }

}
