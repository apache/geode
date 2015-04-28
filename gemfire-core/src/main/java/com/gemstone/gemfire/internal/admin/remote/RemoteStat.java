/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
//import java.util.*;
import java.io.*;

public class RemoteStat implements Stat, DataSerializable {
  private static final long serialVersionUID = 8263951659282343027L;

  // instance variables

  private String name;
  private byte typeCode;
  private int id;
  private String units;
  private String desc;
  private Number value;
  private boolean isCounter;

  // constructor

  public RemoteStat(Statistics rsrc, StatisticDescriptor stat) {
    this.id = stat.getId();
    this.name = stat.getName();
    this.units = stat.getUnit();
    this.isCounter = stat.isCounter();
    this.desc = stat.getDescription();
    this.typeCode = ((StatisticDescriptorImpl)stat).getTypeCode();
    this.value = rsrc.get(stat);
  }
  
  /**
   * Constructor for <code>DataSerializable</code>
   */
  public RemoteStat() { }

  // Stat methods
  
  public Number getValue() {
    return this.value;
  }
    
  public String getUnits() {
    return this.units;
  }
    
  public boolean isCounter() {
    return this.isCounter;
  }

  // GfObject methods

  public int getID(){
    return this.id;
  }

  public String getName(){
    return this.name;
  }

  public String getType(){
    return StatisticDescriptorImpl.getTypeCodeName(this.typeCode);
  }

  public String getDescription(){
    return this.desc;
  }

  // Object methods

  @Override
  public String toString() {
    return "<STAT name=" + getName() + " type=" + getType() + " units=" + getUnits() +  " isCounter=" + isCounter() +
      " value=" + getValue() + " desc=\"" + getDescription() + "\">";
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    out.writeByte(this.typeCode);
    out.writeInt(this.id);
    DataSerializer.writeString(this.units, out);
    DataSerializer.writeString(this.desc, out);
    DataSerializer.writeObject(this.value, out);
    out.writeBoolean(this.isCounter);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    this.name = DataSerializer.readString(in);
    this.typeCode = in.readByte();
    this.id = in.readInt();
    this.units = DataSerializer.readString(in);
    this.desc = DataSerializer.readString(in);
    this.value = (Number) DataSerializer.readObject(in);
    this.isCounter = in.readBoolean();
  }

}
