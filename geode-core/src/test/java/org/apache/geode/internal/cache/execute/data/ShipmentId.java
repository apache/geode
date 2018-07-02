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

public class ShipmentId implements DataSerializable {
  Integer shipmentId;

  OrderId orderId;

  public ShipmentId() {

  }

  public ShipmentId(int shipmentId, OrderId orderId) {
    this.shipmentId = new Integer(shipmentId);
    this.orderId = orderId;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.shipmentId = DataSerializer.readInteger(in);
    this.orderId = (OrderId) DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.shipmentId, out);
    DataSerializer.writeObject(this.orderId, out);
  }

  public String toString() {
    return "(ShipmentId:" + this.shipmentId + " , " + this.orderId;
  }

  public OrderId getOrderId() {
    return orderId;
  }

  public void setOrderId(OrderId orderId) {
    this.orderId = orderId;
  }

  public Integer getShipmentId() {
    return shipmentId;
  }

  public void setShipmentId(Integer shipmentId) {
    this.shipmentId = shipmentId;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;

    if (obj instanceof ShipmentId) {
      ShipmentId other = (ShipmentId) obj;
      if (orderId.equals(other.orderId) && shipmentId.equals(other.shipmentId)) {
        return true;
      }
    }
    return false;
  }

  public int hashCode() {
    return orderId.getCustId().hashCode();
  }
}
