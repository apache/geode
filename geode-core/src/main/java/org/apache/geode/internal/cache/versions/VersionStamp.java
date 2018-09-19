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
package org.apache.geode.internal.cache.versions;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionEntry;

public interface VersionStamp<T extends VersionSource<T>> extends VersionHolder<T>, RegionEntry {

  /**
   * Sets the time stamp from the given clock value
   */
  void setVersionTimeStamp(long time);

  /**
   * Sets the version information with what is in the tag
   *
   * @param tag the entryVersion to set
   */
  void setVersions(VersionTag<T> tag);

  /**
   * @param memberID the memberID to set
   */
  void setMemberID(VersionSource memberID);

  /**
   * Returns a VersionTag carrying this stamps information. This is used for transmission of the
   * stamp in initial image transfer
   */
  VersionTag<T> asVersionTag();

  @Override
  default void checkForConcurrencyConflict(EntryEvent<?, ?> event) {
    processVersionTag(event);
  }

  /**
   * Perform a versioning check with the given GII information. Throws a
   * ConcurrentCacheModificationException if there is a problem.
   *
   * @param region the region being modified
   * @param tag the version info for the modification
   * @param isTombstoneFromGII it's a tombstone
   * @param hasDelta it has delta
   * @param versionSource this cache's DM identifier
   * @param sender the identifier of the member providing the entry
   * @param checkConflicts true if conflict checks should be performed
   */
  void processVersionTag(InternalRegion region, VersionTag<T> tag, boolean isTombstoneFromGII,
      boolean hasDelta, VersionSource<T> versionSource, InternalDistributedMember sender,
      boolean checkConflicts);

  /**
   * Returns true if this stamp has valid entry/region version information, false if not
   */
  boolean hasValidVersion();

  @Override
  default boolean needsToCheckForConflict(InternalRegion region) {
    return true;
  }

}
