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
package com.gemstone.gemfire.admin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Class <code>RegionSubRegionSnapshot</code> provides information about
 * <code>Region</code>s. This also provides the information about sub regions
 * This class is used by the monitoring tool.
 * 
 * 
 * @since GemFire 5.7
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public class RegionSubRegionSnapshot implements DataSerializable {
  private static final long serialVersionUID = -8052137675270041871L;
  public RegionSubRegionSnapshot() {
    this.parent = null;
    this.subRegionSnapshots = new HashSet();
  }

  public RegionSubRegionSnapshot(Region reg) {
    this();
    this.name = reg.getName();
    if (reg instanceof PartitionedRegion) {
      PartitionedRegion p_reg = (PartitionedRegion)reg;
      this.entryCount = p_reg.entryCount(true);
    }
    else {
      this.entryCount = reg.entrySet().size();
    }
    final LogWriterI18n logger = reg.getCache().getLoggerI18n();
    if((logger != null) && logger.fineEnabled()) {
      logger.fine(
        "RegionSubRegionSnapshot Region entry count =" + this.entryCount
        + " for region =" + this.name);
    }
  }

  /**
   * add the snapshot of sub region
   * 
   * @param snap
   *                snapshot of sub region
   * @return true if operation is successful
   */
  public boolean addSubRegion(RegionSubRegionSnapshot snap) {
    if (subRegionSnapshots.contains(snap)) {
      return true;
    }

    if (subRegionSnapshots.add(snap)) {
      snap.setParent(this);
      return true;
    }

    return false;
  }

  /**
   * @return get entry count of region
   */
  public final int getEntryCount() {
    return entryCount;
  }

  /**
   * @param entryCount
   *                entry count of region
   */
  public final void setEntryCount(int entryCount) {
    this.entryCount = entryCount;
  }

  /**
   * @return name of region
   */
  public final String getName() {
    return name;
  }

  /**
   * @param name
   *                name of region
   */
  public final void setName(String name) {
    this.name = name;
  }

  /**
   * @return subRegionSnapshots of all the sub regions
   */
  public final Set getSubRegionSnapshots() {
    return subRegionSnapshots;
  }

  /**
   * @param subRegionSnapshots
   *                subRegionSnapshots of all the sub regions
   */
  public final void setSubRegionSnapshots(Set subRegionSnapshots) {
    this.subRegionSnapshots = subRegionSnapshots;
  }

  /**
   * @return snapshot of parent region
   */
  public final RegionSubRegionSnapshot getParent() {
    return parent;
  }

  /**
   * @param parent
   *                snapshot of parent region
   */
  public final void setParent(RegionSubRegionSnapshot parent) {
    this.parent = parent;
  }

  /**
   * 
   * @return full path of region
   */
  public String getFullPath() {
    return (getParent() == null ? "/" : getParent().getFullPath()) + getName()
        + "/";
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    out.writeInt(this.entryCount);
    DataSerializer.writeHashSet((HashSet)this.subRegionSnapshots, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.entryCount = in.readInt();
    this.subRegionSnapshots = DataSerializer.readHashSet(in);
    for (Iterator iter = this.subRegionSnapshots.iterator(); iter.hasNext();) {
      ((RegionSubRegionSnapshot)iter.next()).setParent(this);
    }
  }

  @Override
  public String toString() {
    String toStr = "RegionSnapshot [" + "path=" + this.getFullPath()
        + ",parent=" + (this.parent == null ? "null" : this.parent.name)
        + ", entryCount=" + this.entryCount + ", subRegionCount="
        + this.subRegionSnapshots.size() + "<<";

    for (Iterator iter = subRegionSnapshots.iterator(); iter.hasNext();) {
      toStr = toStr + ((RegionSubRegionSnapshot)iter.next()).getName() + ", ";
    }

    toStr = toStr + ">>" + "]";
    return toStr;
  }

  protected String name;

  protected int entryCount;

  protected RegionSubRegionSnapshot parent;

  protected Set subRegionSnapshots;
}
