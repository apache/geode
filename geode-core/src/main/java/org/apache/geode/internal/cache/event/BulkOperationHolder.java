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

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * A holder for the version tags generated for a bulk operation (putAll or removeAll). These version
 * tags are retrieved when a bulk op is retried.
 *
 * @since GemFire 7.0 protected for test purposes only.
 */
public class BulkOperationHolder {
  /**
   * Whether this object was removed by the cleanup thread.
   */
  private boolean removed;

  /**
   * public for tests only
   */
  private final Map<EventID, VersionTag> entryVersionTags = new HashMap<>();

  /** millisecond timestamp */
  private transient long endOfLifeTimestamp;

  /**
   * creates a new instance to save status of a putAllOperation
   */
  BulkOperationHolder() {
    // do nothing
  }

  void putVersionTag(EventID eventId, VersionTag versionTag) {
    entryVersionTags.put(eventId, versionTag);
    endOfLifeTimestamp = 0;
  }

  public Map<EventID, VersionTag> getEntryVersionTags() {
    return entryVersionTags;
  }

  @Override
  public String toString() {
    return "BulkOperationHolder tags=" + entryVersionTags;
  }

  public synchronized boolean expire(long now, long expirationTime) {
    if (endOfLifeTimestamp == 0) {
      endOfLifeTimestamp = now; // a new holder - start the timer
    }
    boolean expired = false;
    if (endOfLifeTimestamp <= expirationTime) {
      removed = true;
      expired = true;
    }
    return expired;
  }

  public boolean isRemoved() {
    return removed;
  }
}
