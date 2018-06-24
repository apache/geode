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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is a wrapper class for doubles, similar to {@link ByteArrayWrapper}
 *
 *
 */
public class StringWrapper implements DataSerializable, Comparable<Object> {

  private static final long serialVersionUID = 6946858357297398633L;

  public String str;

  public StringWrapper() {}

  public StringWrapper(String s) {
    this.str = s;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(str, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.str = DataSerializer.readString(in);
  }

  @Override
  public int compareTo(Object arg0) {
    String other;
    if (arg0 instanceof StringWrapper) {
      other = ((StringWrapper) arg0).str;
    } else if (arg0 instanceof Double) {
      other = (String) arg0;
    } else return 0;

    return this.str.compareTo(other);
  }

  public String toString() {
    return this.str;
  }
}
