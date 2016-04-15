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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * Computes the sum for replicated & PR based queries.
 * 
 *
 */
public class Sum extends AbstractAggregator implements DataSerializableFixedID{

  private double result = 0;

  
  public Sum() {   
  }
  
  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      Number number = (Number) value;
      result += number.doubleValue();
    }
  }

  @Override
  public void init() {

  }

  @Override
  public Object terminate() {
    return downCast(result);
  }
  
  @Override
  public void merge(Aggregator aggregator) {
    Sum sumAgg = (Sum)aggregator;
    this.result += sumAgg.result; 
  }

  @Override
  public Version[] getSerializationVersions() {   
    return null;
  }

  @Override
  public int getDSFID() {    
    return AGG_FUNC_SUM;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveDouble(this.result, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {    
    this.result = DataSerializer.readPrimitiveDouble(in);
  }
}
