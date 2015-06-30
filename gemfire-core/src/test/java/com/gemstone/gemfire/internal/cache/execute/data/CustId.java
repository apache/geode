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
    return "(CustId:" + this.custId+")";
  }

  public Integer getCustId() {
    return this.custId;
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof CustId))
      return false;

    CustId otherCustId = (CustId)o;
    return (otherCustId.custId.equals(custId));

  }
}
