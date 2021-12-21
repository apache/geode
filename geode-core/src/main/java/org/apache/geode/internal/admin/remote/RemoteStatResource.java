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
import org.apache.geode.Statistics;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.admin.StatResource;

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

  public RemoteStatResource(Statistics rsrc) {
    rsrcId = rsrc.getNumericId();
    rsrcUniqueId = rsrc.getUniqueId();
    name = rsrc.getTextId();
    typeName = rsrc.getType().getName();
    typeDesc = rsrc.getType().getDescription();
  }

  /**
   * Constructor for <code>DataSerializable</code>
   */
  public RemoteStatResource() {}

  // StatResource methods

  @Override
  public long getResourceID() {
    return rsrcId;
  }

  @Override
  public long getResourceUniqueID() {
    return rsrcUniqueId;
  }

  @Override
  public String getSystemName() {
    if (systemName == null) {
      if (vm == null) {
        systemName = "";
      } else {
        return systemName = vm.toString();
      }
    }
    return systemName;
  }

  @Override
  public GemFireVM getGemFireVM() {
    return vm;
  }

  @Override
  public Stat[] getStats() {
    if (vm != null) {
      return vm.getResourceStatsByID(rsrcUniqueId);
    } else {
      return new RemoteStat[0];
    }
  }

  @Override
  public Stat getStatByName(String name) {
    Stat[] stats = getStats();
    for (int i = 0; i < stats.length; i++) {
      if (name.equals(stats[i].getName())) {
        return stats[i];
      }
    }
    return null;
  }

  // GfObject methods

  @Override
  public int getID() {
    return -1;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return typeName;
  }

  @Override
  public String getDescription() {
    return typeDesc;
  }

  // Object methods

  @Override
  public int hashCode() {
    return (int) rsrcUniqueId;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RemoteStatResource) {
      RemoteStatResource rsrc = (RemoteStatResource) other;
      return (rsrcUniqueId == rsrc.rsrcUniqueId && vm.equals(rsrc.vm));
    } else {
      return false;
    }
  }

  // other instance methods

  void setGemFireVM(RemoteGemFireVM vm) {
    this.vm = vm;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(rsrcId);
    out.writeLong(rsrcUniqueId);
    DataSerializer.writeString(name, out);
    DataSerializer.writeString(typeName, out);
    DataSerializer.writeString(typeDesc, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    rsrcId = in.readLong();
    rsrcUniqueId = in.readLong();
    name = DataSerializer.readString(in);
    typeName = DataSerializer.readString(in);
    typeDesc = DataSerializer.readString(in);
  }

}
