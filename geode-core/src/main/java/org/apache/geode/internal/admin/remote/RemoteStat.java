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

package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.statistics.StatisticDescriptorImpl;

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
    this.typeCode = ((StatisticDescriptorImpl) stat).getTypeCode();
    this.value = rsrc.get(stat);
  }

  /**
   * Constructor for <code>DataSerializable</code>
   */
  public RemoteStat() {}

  // Stat methods

  @Override
  public Number getValue() {
    return this.value;
  }

  @Override
  public String getUnits() {
    return this.units;
  }

  @Override
  public boolean isCounter() {
    return this.isCounter;
  }

  // GfObject methods

  @Override
  public int getID() {
    return this.id;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getType() {
    return StatisticDescriptorImpl.getTypeCodeName(this.typeCode);
  }

  @Override
  public String getDescription() {
    return this.desc;
  }

  // Object methods

  @Override
  public String toString() {
    return "<STAT name=" + getName() + " type=" + getType() + " units=" + getUnits() + " isCounter="
        + isCounter() + " value=" + getValue() + " desc=\"" + getDescription() + "\">";
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    out.writeByte(this.typeCode);
    out.writeInt(this.id);
    DataSerializer.writeString(this.units, out);
    DataSerializer.writeString(this.desc, out);
    DataSerializer.writeObject(this.value, out);
    out.writeBoolean(this.isCounter);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    this.name = DataSerializer.readString(in);
    this.typeCode = in.readByte();
    this.id = in.readInt();
    this.units = DataSerializer.readString(in);
    this.desc = DataSerializer.readString(in);
    this.value = (Number) DataSerializer.readObject(in);
    this.isCounter = in.readBoolean();
  }

}
