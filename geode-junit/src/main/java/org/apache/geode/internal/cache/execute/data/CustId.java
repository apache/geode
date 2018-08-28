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
package org.apache.geode.internal.cache.execute.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class CustId implements DataSerializable {
  Integer custId;

  public CustId() {

  }

  public CustId(int i) {
    this.custId = new Integer(i);
  }

  public int hashCode() {
    int i = custId.intValue();
    return i;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.custId = DataSerializer.readInteger(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.custId, out);
  }

  public String toString() {
    return "(CustId:" + this.custId + ")";
  }

  public Integer getCustId() {
    return this.custId;
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof CustId))
      return false;

    CustId otherCustId = (CustId) o;
    return (otherCustId.custId.equals(custId));

  }
}
