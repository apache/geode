/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.execute.data;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
    this.orderId = (OrderId)DataSerializer.readObject(in);
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
    if(this == obj)
      return true;
    
    if(obj instanceof ShipmentId){
      ShipmentId other = (ShipmentId)obj;
      if(orderId.equals(other.orderId) && shipmentId.equals(other.shipmentId)){
        return true;
      }
    }
    return false;
  }
  
  public int hashCode() {
    return orderId.getCustId().hashCode();
  }
}
