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
package org.apache.geode.internal.cache.event;

import static org.apache.geode.internal.serialization.KnownVersion.GEODE_1_15_0;
import static org.apache.geode.internal.serialization.StaticSerialization.getVersionForDataStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.versions.VersionTag;


/**
 * A sequence number tracker to keep events from clients from being re-applied to the cache if
 * they've already been seen.
 *
 * @since GemFire 5.5
 */
public class EventSequenceNumberHolder implements DataSerializable {
  private static final long serialVersionUID = 8137262960763308046L;

  /**
   * event sequence number. These
   */
  private long lastSequenceNumber = -1;

  /**
   * millisecond timestamp
   */
  private transient long endOfLifeTimestamp;

  /**
   * whether this entry is being removed
   */
  private transient boolean removed;

  /**
   * version tag, if any, for the operation
   */
  private VersionTag versionTag;

  private Map<Object, Long> keySequenceIdMap = null;

  private final int limit = 150;

  // for debugging
  // transient Exception context;

  EventSequenceNumberHolder(long id, VersionTag versionTag) {
    this.lastSequenceNumber = id;
    this.versionTag = versionTag;
  }

  EventSequenceNumberHolder(long id, VersionTag versionTag, Object key) {
    this.lastSequenceNumber = id;
    this.versionTag = versionTag;
    if (key != null) {
      keySequenceIdMap = new LinkedHashMap<Object, Long>() {
        protected boolean removeEldestEntry(Map.Entry<Object, Long> eldest) {
          return size() > limit;
        }
      };
      keySequenceIdMap.put(key, id);
    }
  }

  public EventSequenceNumberHolder() {}

  public long getLastSequenceNumber() {
    return lastSequenceNumber;
  }

  public VersionTag getVersionTag() {
    return versionTag;
  }

  public boolean isRemoved() {
    return removed;
  }

  void setRemoved(boolean removed) {
    this.removed = removed;
  }

  void setEndOfLifeTimestamp(long endOfLifeTimestamp) {
    this.endOfLifeTimestamp = endOfLifeTimestamp;
  }

  void setVersionTag(VersionTag versionTag) {
    this.versionTag = versionTag;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("seqNo").append(this.lastSequenceNumber);
    if (this.versionTag != null) {
      result.append(",").append(this.versionTag);
    }
    return result.toString();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    lastSequenceNumber = in.readLong();
    versionTag = (VersionTag) DataSerializer.readObject(in);
    if (getVersionForDataStream(in).isNotOlderThan(GEODE_1_15_0)) {
      final int size = InternalDataSerializer.readArrayLength(in);
      Object key;
      Long value;
      for (int i = 0; i < size; i++) {
        key = DataSerializer.readObject(in);
        value = DataSerializer.readObject(in);
        this.keySequenceIdMap.put(key, value);
      }
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(lastSequenceNumber);
    DataSerializer.writeObject(versionTag, out);
    if (getVersionForDataStream(out)
        .isNotOlderThan(GEODE_1_15_0)) {
      InternalDataSerializer.writeHashMap(this.keySequenceIdMap, out);
    }
  }

  public synchronized boolean expire(long now, long expirationTime) {
    if (endOfLifeTimestamp == 0) {
      endOfLifeTimestamp = now; // a new holder - start the timer
    }
    boolean expire = false;
    if (endOfLifeTimestamp <= expirationTime) {
      removed = true;
      lastSequenceNumber = -1;
      expire = true;
    }
    return expire;
  }

  public void setLastSequenceNumber(long lastSequenceNumber) {
    this.lastSequenceNumber = lastSequenceNumber;
  }

  public int getKeySequenceIdSize() {
    if (keySequenceIdMap == null)
      return 0;
    return keySequenceIdMap.size();
  }

  public Map<Object, Long> getKeySequenceId() {
    return keySequenceIdMap;
  }
}
