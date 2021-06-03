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
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.admin.RegionSnapshot;

public class RemoteRegionSnapshot implements RegionSnapshot, DataSerializable {
  private static final long serialVersionUID = -2006079857403000280L;
  private String name;
  private RemoteRegionAttributes attributes;
  private RemoteCacheStatistics stats;
  private int entryCount;
  private int subregionCount;
  private Object userAttribute;

  public RemoteRegionSnapshot(Region r) throws CacheException {

    this.name = r.getName();
    RegionAttributes rAttr = r.getAttributes();
    this.attributes = new RemoteRegionAttributes(rAttr);
    if (rAttr.getStatisticsEnabled()) {
      this.stats = new RemoteCacheStatistics(r.getStatistics());
    } else {
      this.stats = new RemoteCacheStatistics();
    }
    this.attributes = new RemoteRegionAttributes(r.getAttributes());
    Set nameSet = r.keySet();
    this.entryCount = nameSet.size();
    Set subRegions = r.subregions(false);
    this.subregionCount = subRegions.size();
    Object attr = r.getUserAttribute();
    if (attr != null) {
      this.userAttribute = attr.getClass().getName() + "\"" + attr.toString() + "\"";
    } else {
      this.userAttribute = null;
    }
  }

  /**
   * This constructor is only for use by the DataSerializableMechanism
   */
  public RemoteRegionSnapshot() {}

  @Override
  public Object getName() {
    return this.name;
  }

  @Override
  public long getLastModifiedTime() {
    return stats.getLastModifiedTime();
  }

  @Override
  public long getLastAccessTime() {
    return stats.getLastAccessedTime();
  }

  @Override
  public long getNumberOfHits() {
    return stats.getHitCount();
  }

  @Override
  public long getNumberOfMisses() {
    return stats.getMissCount();
  }

  @Override
  public float getHitRatio() {
    return stats.getHitRatio();
  }

  @Override
  public RegionAttributes getAttributes() {
    return this.attributes;
    // if (rUserAttributes != null) {
    // Iterator it = rUserAttributes.entrySet().iterator();
    // while (it.hasNext()) {
    // Map.Entry me = (Map.Entry)it.next();
    // result.setUserAttribute(me.getKey(), me.getValue());
    // }
    // }
  }

  public int getEntryCount() {
    return entryCount;
  }

  public int getSubregionCount() {
    return subregionCount;
  }

  @Override
  public Object getUserAttribute() {
    return userAttribute;
  }

  // private String getDesc(Object o) {
  // if (o == null) {
  // return "";
  // } else {
  // return o.getClass().getName();
  // }
  // }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof RemoteRegionSnapshot) {
      RemoteRegionSnapshot snap = (RemoteRegionSnapshot) other;
      return this.name.equals(snap.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

  @Override
  public String toString() {
    return getName().toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeObject(this.stats, out);
    DataSerializer.writeObject(this.attributes, out);
    out.writeInt(this.entryCount);
    out.writeInt(this.subregionCount);
    DataSerializer.writeObject(this.userAttribute, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.stats = (RemoteCacheStatistics) DataSerializer.readObject(in);
    this.attributes = (RemoteRegionAttributes) DataSerializer.readObject(in);
    this.entryCount = in.readInt();
    this.subregionCount = in.readInt();
    this.userAttribute = DataSerializer.readObject(in);
  }
}
