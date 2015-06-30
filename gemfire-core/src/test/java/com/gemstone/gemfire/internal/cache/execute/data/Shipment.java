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
import com.gemstone.gemfire.internal.cache.execute.PRColocationDUnitTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Shipment implements DataSerializable {
  String shipmentName;

  public Shipment() {

  }

  public Shipment(String shipmentName) {
    this.shipmentName = shipmentName + PRColocationDUnitTest.getDefaultAddOnString();
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.shipmentName = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.shipmentName, out);
  }

  public String toString() {
    return this.shipmentName;
  }
  
  public boolean equals(Object obj) {
    if(this == obj)
      return true;
    
    if(obj instanceof Shipment){
      Shipment other = (Shipment)obj;
      if(other.shipmentName != null && other.shipmentName.equals(this.shipmentName)){
        return true;
      }
    }
    return false;
  }
}
