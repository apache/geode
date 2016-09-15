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
package org.apache.geode.internal.admin.remote;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.*;
import java.io.*;

/**
 * This class represents a snapshot of a {@link org.apache.geode.cache.CacheStatistics}
 * from a remote vm
 */
public class RemoteCacheStatistics implements CacheStatistics, DataSerializable {
  private static final long serialVersionUID = 53585856563375154L;
  private long lastModified;
  private long lastAccessed;
  private long hitCount;
  private long missCount;
  private float hitRatio;

  public RemoteCacheStatistics(CacheStatistics stats) {
    this.lastModified = stats.getLastModifiedTime();
    this.lastAccessed = stats.getLastAccessedTime();
    this.hitCount = stats.getHitCount();
    this.missCount = stats.getMissCount();
    this.hitRatio = stats.getHitRatio();    
  }

  /**
   * For use only by DataExternalizable mechanism
   */
  public RemoteCacheStatistics() {}

  public long getLastModifiedTime() {
    return lastModified;
  }

  public long getLastAccessedTime() {
    return lastAccessed;
  }

  public long getHitCount() {
    return hitCount;
  }

  public long getMissCount() {
    return missCount;
  }

  public float getHitRatio() {
    return hitRatio;
  }

  public void resetCounts() {
    throw new UnsupportedOperationException();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeLong(lastModified);
    out.writeLong(lastAccessed);
    out.writeLong(hitCount);
    out.writeLong(missCount);
    out.writeFloat(hitRatio);
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    lastModified = in.readLong();
    lastAccessed = in.readLong();
    hitCount = in.readLong();
    missCount = in.readLong();
    hitRatio = in.readFloat();
  }
}
