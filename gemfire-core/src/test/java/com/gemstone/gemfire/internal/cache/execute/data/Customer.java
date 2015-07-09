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

public class Customer implements DataSerializable {
  String name;

  String address;

  public Customer() {

  }

  public Customer(String name, String address) {
    this.name = name;
    this.address = address + PRColocationDUnitTest.getDefaultAddOnString();
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.address = DataSerializer.readString(in);

  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.address, out);
  }

  @Override
  public String toString() {
    return "Customer { name=" + this.name + " address=" + this.address + "}";
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof Customer))
      return false;

    Customer cust = (Customer)o;
    return (cust.name.equals(name) && cust.address.equals(address));
  }

  @Override
  public int hashCode() {
    return this.name.hashCode() + this.address.hashCode();
  }

}
