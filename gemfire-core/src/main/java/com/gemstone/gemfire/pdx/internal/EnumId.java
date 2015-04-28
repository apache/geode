/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

public class EnumId implements DataSerializableFixedID {

  private int id;
  
  public EnumId(int id) {
    this.id = id;
  }

  public EnumId() {
  }

  public int getDSFID() {
    return ENUM_ID;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
  }

  public int intValue() {
    return this.id;
  }
  
  public int getDSId() {
    return this.id >> 24;
  }
  
  public int getEnumNum() {
    return this.id & 0x00FFFFFF;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    EnumId other = (EnumId) obj;
    if (id != other.id)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "EnumId[dsid=" + getDSId() + ", enumnum=" + getEnumNum() + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
  
  

}
