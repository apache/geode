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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * The class used to hold the distinct values. This will get instantiated on the
 * bucket node as part of distinct queries for sum, count, average.
 * 
 * 
 *
 */
public class DistinctAggregator extends AbstractAggregator implements DataSerializableFixedID {
  protected HashSet<Object> distinct;

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
  public void init() {}

  @Override
  public Object terminate() {
    return this.distinct;
  }

  @Override
  public void merge(Aggregator otherAgg) {
    this.distinct.addAll(((DistinctAggregator) otherAgg).distinct);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return AGG_FUNC_DISTINCT_AGG;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet(this.distinct, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.distinct = DataSerializer.readHashSet(in);
  }
}
