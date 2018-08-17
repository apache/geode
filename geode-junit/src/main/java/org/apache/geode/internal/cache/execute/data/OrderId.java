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

public class OrderId implements DataSerializable {
  Integer orderId;

  CustId custId;

  public OrderId() {

  }

  public OrderId(int orderId, CustId custId) {
    this.orderId = new Integer(orderId);
    this.custId = custId;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.orderId = DataSerializer.readInteger(in);
    this.custId = (CustId) DataSerializer.readObject(in);

  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.orderId, out);
    DataSerializer.writeObject(this.custId, out);
  }

  public String toString() {
    return "(OrderId:" + this.orderId + " , " + this.custId + ")";
  }

  public Integer getOrderId() {
    return this.orderId;
  }

  public CustId getCustId() {
    return this.custId;
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof OrderId))
      return false;

    OrderId otherOrderId = (OrderId) o;
    return (otherOrderId.orderId.equals(orderId) && otherOrderId.custId.equals(custId));

  }

  public int hashCode() {
    return custId.hashCode();
  }
}
