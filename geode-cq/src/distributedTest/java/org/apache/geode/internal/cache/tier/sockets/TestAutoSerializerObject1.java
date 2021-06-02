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
package org.apache.geode.internal.cache.tier.sockets;

import java.util.Objects;

import org.apache.geode.internal.PdxSerializerObject;

/**
 * <strong>Explicitly</strong> not serializable by java.io.Serializable,
 * org.apache.geode.DataSerializable, or org.apache.geode.pdx.PdxSerializable.
 */
public class TestAutoSerializerObject1 implements PdxSerializerObject {
  protected String data1;
  protected String data2;
  protected int numData;

  public TestAutoSerializerObject1() {
    this("", "", 0);
  }

  protected TestAutoSerializerObject1(String data1, String data2, int numData) {
    this.data1 = data1;
    this.data2 = data2;
    this.numData = numData;
  }

  public String getData1() {
    return data1;
  }

  public String getData2() {
    return data2;
  }

  public void setData2(String data2) {
    this.data2 = data2;
  }

  public int getNumData() {
    return numData;
  }

  public void setNumData(int numData) {
    this.numData = numData;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    if (data1 != null && !data1.isEmpty()) {
      builder.append(data1);
      builder.append(" (");

      if (data2 != null && !data2.isEmpty()) {
        if (0 < builder.length() && '(' != builder.charAt(builder.length() - 1)) {
          builder.append(", ");
        }
        builder.append("data2: ");
        builder.append(data2);
      }

      if (0 < numData) {
        if (0 < builder.length() && '(' != builder.charAt(builder.length() - 1)) {
          builder.append(", ");
        }
        builder.append("numData: ");
        builder.append(numData);
      }

      builder.append(")");
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestAutoSerializerObject1 test = (TestAutoSerializerObject1) o;
    // field comparison
    return Objects.equals(data1, test.data1)
        && Objects.equals(data2, test.data2) && Objects.equals(numData, test.numData);
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + data1.hashCode();
    result = 31 * result + data2.hashCode();
    result = 31 * result + numData;
    return result;
  }

}
