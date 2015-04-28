/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.admin.*;
//import java.util.*;
import java.io.*;

public class RemoteEntrySnapshot implements EntrySnapshot, DataSerializable {
  private static final long serialVersionUID = 1360498801579593535L;
  private Object name;
  private Object value;
  private Object userAttribute;

  private RemoteCacheStatistics stats;

  public RemoteEntrySnapshot(Region.Entry entry, boolean statsEnabled) throws CacheException {
    Object entryName = entry.getKey();
    if (entryName instanceof String || entryName instanceof Number) {
      name = entryName;
    } else {
      name = new RemoteObjectName(entryName);
    }
    Object val = entry.getValue();
    if (val != null) {
      this.value = val.getClass().getName() + "\"" + val.toString() + "\"";
    } else {
      this.value = null;
    }
    Object attr = entry.getUserAttribute();
    if (attr != null) {
      this.userAttribute = attr.getClass().getName() + "\"" + attr.toString() + "\"";
    } else {
      this.userAttribute = null;
    }
    if (statsEnabled) {
      this.stats = new RemoteCacheStatistics(entry.getStatistics());
    } else {
      this.stats = new RemoteCacheStatistics();
    }
  }
  /**
   * This constructor is only for use by the DataSerializable mechanism
   */
  public RemoteEntrySnapshot() {}

  public Object getName() {
    return this.name;
  }

  public long getLastModifiedTime() {
    return stats.getLastModifiedTime();
  }

  public long getLastAccessTime() {
    return stats.getLastAccessedTime();
  }

  public long getNumberOfHits() {
    return stats.getHitCount();
  }

  public long getNumberOfMisses() {
    return stats.getMissCount();
  }

  public float getHitRatio() {
    return stats.getHitRatio();
  }

  public Object getValue() {
    return value;
  }

  public Object getUserAttribute() {
    return userAttribute;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (other instanceof RemoteEntrySnapshot) {
      RemoteEntrySnapshot snap = (RemoteEntrySnapshot)other;
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
  
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.name, out);  
    DataSerializer.writeObject(this.value, out);
    DataSerializer.writeObject(this.stats, out);
    DataSerializer.writeObject(this.userAttribute, out);
  }

  public void fromData(DataInput in) throws IOException, 
      ClassNotFoundException {
    this.name = DataSerializer.readObject(in);
    this.value = DataSerializer.readObject(in);
    this.stats = (RemoteCacheStatistics)DataSerializer.readObject(in);
    this.userAttribute = DataSerializer.readObject(in);
  }
}
