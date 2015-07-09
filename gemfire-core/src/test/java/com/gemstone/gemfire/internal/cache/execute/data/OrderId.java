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
    this.custId = (CustId)DataSerializer.readObject(in);

  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.orderId, out);
    DataSerializer.writeObject(this.custId, out);
  }

  public String toString() {
    return "(OrderId:" + this.orderId + " , " + this.custId+")";
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

    OrderId otherOrderId = (OrderId)o;
    return (otherOrderId.orderId.equals(orderId) && otherOrderId.custId
        .equals(custId));

  }
    
  public int hashCode() {  
    return custId.hashCode();
  }
}
