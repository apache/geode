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

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * The class used to hold the distinct values. This will get instantiated on the
 * bucket node as part of distinct queries for sum, count, average.
 * 
 *
 */
public class DistinctAggregator extends AbstractAggregator {
  protected final Set<Object> distinct;

  public DistinctAggregator() {
    this.distinct = new HashSet<Object>();
  }

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      this.distinct.add(value);
    }
  }

  @Override
  public void init() {
    // TODO Auto-generated method stub

  }

  @Override
  public Object terminate() {
    return this.distinct;
  }

}
