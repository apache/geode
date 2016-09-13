/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.admin.*;
import java.io.*;

public class RemoteStatResource implements StatResource, DataSerializable {
  private static final long serialVersionUID = -3118720083415516133L;

  // instance variables

  private long rsrcId;
  private long rsrcUniqueId;
  private String name;
  private String typeName;
  private String typeDesc;
  private transient RemoteGemFireVM vm;  
  private String systemName;

  // constructor

  public RemoteStatResource(Statistics rsrc){
    this.rsrcId = rsrc.getNumericId();
    this.rsrcUniqueId = rsrc.getUniqueId();
    this.name = rsrc.getTextId();
    this.typeName = rsrc.getType().getName();
    this.typeDesc = rsrc.getType().getDescription();
  }
  
  /**
   * Constructor for <code>DataSerializable</code>
   */
  public RemoteStatResource() { }

  // StatResource methods
  
  public long getResourceID(){
    return rsrcId;
  }
  public long getResourceUniqueID() {
    return rsrcUniqueId;
  }

  public String getSystemName(){
    if (systemName == null) {
      if (vm == null) {
        systemName = "";
      } else {
        return systemName = vm.toString();
      }
    }
    return systemName;
  }

  public GemFireVM getGemFireVM(){
    return vm;
  }

  public Stat[] getStats(){
    if ( vm != null ) {
      return vm.getResourceStatsByID(this.rsrcUniqueId);
    } else {
      return new RemoteStat[0];
    }
  }

  public Stat getStatByName(String name) {
    Stat[] stats = getStats();
    for (int i=0; i < stats.length; i++) {
      if (name.equals(stats[i].getName())) {
        return stats[i];
      }
    }
    return null;
  }

  // GfObject methods

  public int getID(){
    return -1;
  }

  public String getName(){
    return name;
  }

  public String getType(){
    return typeName;
  }

  public String getDescription(){
    return typeDesc;
  }

  // Object methods

  @Override
  public int hashCode() {
    return (int)this.rsrcUniqueId;
  }
  
  @Override
  public boolean equals(Object other){
    if (other instanceof RemoteStatResource){
      RemoteStatResource rsrc = (RemoteStatResource)other;
      return (this.rsrcUniqueId == rsrc.rsrcUniqueId &&
              this.vm.equals(rsrc.vm));
    } else {
      return false;
    }      
  }

  // other instance methods

  void setGemFireVM( RemoteGemFireVM vm ){
    this.vm = vm;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeLong(this.rsrcId);
    out.writeLong(this.rsrcUniqueId);
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.typeName, out);
    DataSerializer.writeString(this.typeDesc, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    this.rsrcId = in.readLong();
    this.rsrcUniqueId = in.readLong();
    this.name = DataSerializer.readString(in);
    this.typeName = DataSerializer.readString(in);
    this.typeDesc = DataSerializer.readString(in);
  }

}
