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
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.admin.EntrySnapshot;

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
      value = val.getClass().getName() + "\"" + val + "\"";
    } else {
      value = null;
    }
    Object attr = entry.getUserAttribute();
    if (attr != null) {
      userAttribute = attr.getClass().getName() + "\"" + attr + "\"";
    } else {
      userAttribute = null;
    }
    if (statsEnabled) {
      stats = new RemoteCacheStatistics(entry.getStatistics());
    } else {
      stats = new RemoteCacheStatistics();
    }
  }

  /**
   * This constructor is only for use by the DataSerializable mechanism
   */
  public RemoteEntrySnapshot() {}

  @Override
  public Object getName() {
    return name;
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
  public Object getValue() {
    return value;
  }

  @Override
  public Object getUserAttribute() {
    return userAttribute;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof RemoteEntrySnapshot) {
      RemoteEntrySnapshot snap = (RemoteEntrySnapshot) other;
      return name.equals(snap.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return getName().toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(name, out);
    DataSerializer.writeObject(value, out);
    DataSerializer.writeObject(stats, out);
    DataSerializer.writeObject(userAttribute, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    name = DataSerializer.readObject(in);
    value = DataSerializer.readObject(in);
    stats = DataSerializer.readObject(in);
    userAttribute = DataSerializer.readObject(in);
  }
}
