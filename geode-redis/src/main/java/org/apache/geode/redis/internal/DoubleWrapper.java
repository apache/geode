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
package org.apache.geode.redis.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

/**
 * This is a wrapper class for doubles, similar to {@link ByteArrayWrapper}
 *
 *
 */
public class DoubleWrapper implements DataSerializable, Comparable<Object> {

  private static final long serialVersionUID = 6946858357297398633L;

  public Double score;
  private String toString;

  public DoubleWrapper() {}

  public DoubleWrapper(Double dubs) {
    this.score = dubs;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeDouble(score, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.score = DataSerializer.readDouble(in);
  }

  @Override
  public int compareTo(Object arg0) {
    Double other;
    if (arg0 instanceof DoubleWrapper) {
      other = ((DoubleWrapper) arg0).score;
    } else if (arg0 instanceof Double) {
      other = (Double) arg0;
    } else {
      return 0;
    }
    double diff = this.score - other;
    if (diff > 0) {
      return 1;
    } else if (diff < 0) {
      return -1;
    } else {
      return 0;
    }
  }

  public String toString() {
    if (this.toString == null) {
      this.toString = Coder.doubleToString(score);
    }
    return this.toString;
  }

}
