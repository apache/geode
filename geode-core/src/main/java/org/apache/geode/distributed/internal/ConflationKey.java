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
package org.apache.geode.distributed.internal;

import java.nio.ByteBuffer;

/**
 * Used to uniquely identify a conflatable message. If two messages have keys that are equal then
 * the latter message can replace (i.e. conflate) the earlier message.
 *
 * @since GemFire 4.2.1
 */
public class ConflationKey {
  private final Object entryKey;
  private final String regionPath;
  private final boolean allowsConflation;
  private ByteBuffer buffer;

  /**
   * Create a new conflation key given its entry key and region
   */
  public ConflationKey(Object entryKey, String regionPath, boolean allowsConflation) {
    this.entryKey = entryKey;
    this.regionPath = regionPath;
    this.allowsConflation = allowsConflation;
  }

  @Override
  public int hashCode() {
    // Note: we intentionally leave out buffer and allowsConflation
    return entryKey.hashCode() ^ regionPath.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Note: we intentionally leave out buffer and allowsConflation
    boolean result = false;
    if (obj != null) {
      if (obj instanceof ConflationKey) {
        ConflationKey other = (ConflationKey) obj;
        result = regionPath.equals(other.regionPath) && entryKey.equals(other.entryKey);
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return regionPath + '/' + entryKey;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void setBuffer(ByteBuffer bb) {
    buffer = bb;
  }

  /**
   * Returns true if this key (and the operation that generated it) allows conflation; returns false
   * if it does not support conflation. Retur
   */
  public boolean allowsConflation() {
    return allowsConflation;
  }
}
