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
package org.apache.geode.internal.sequencelog;

import java.util.regex.Pattern;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;

/**
 * A Wrapper around the graph logger that logs region level events.
 *
 */
public class RegionLogger {

  private static final SequenceLogger GRAPH_LOGGER = SequenceLoggerImpl.getInstance();

  /**
   * Log the creation of a region. This should only be called if the region was not recovered from
   * disk or GII'd from another member.
   *
   */
  public static void logCreate(String regionName, InternalDistributedMember source) {
    GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "create", "created", source, source);
  }

  public static void logGII(String regionName, InternalDistributedMember source,
      InternalDistributedMember dest, PersistentMemberID persistentMemberID) {
    GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "GII", "created", source, dest);
    if (persistentMemberID != null) {
      GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "persist", "persisted", dest,
          persistentMemberID.getDiskStoreId());
    }
  }

  /**
   * Log the persistence of a region
   */
  public static void logPersistence(String regionName, InternalDistributedMember source,
      PersistentMemberID disk) {
    GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "persist", "persisted", source,
        disk.getDiskStoreId());
  }

  /**
   * Log the recovery of a persistent region.
   */
  public static void logRecovery(String regionName, PersistentMemberID disk,
      InternalDistributedMember memberId) {
    GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "recover", "created",
        disk.getDiskStoreId(),
        memberId);

  }

  public static void logDestroy(String regionName, InternalDistributedMember memberId,
      PersistentMemberID persistentID, boolean isClose) {
    if (isEnabled()) {
      final Pattern ALL_REGION_KEYS = Pattern.compile(regionName + ".*");
      GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "destroy", "destroyed", memberId,
          memberId);
      GRAPH_LOGGER.logTransition(GraphType.KEY, ALL_REGION_KEYS, "destroy", "destroyed", memberId,
          memberId);
      if (!isClose && persistentID != null) {
        GRAPH_LOGGER.logTransition(GraphType.REGION, regionName, "destroy", "destroyed", memberId,
            persistentID.getDiskStoreId());
        GRAPH_LOGGER.logTransition(GraphType.KEY, ALL_REGION_KEYS, "destroy", "destroyed", memberId,
            persistentID.getDiskStoreId());
      }
    }
  }

  public static boolean isEnabled() {
    return GRAPH_LOGGER.isEnabled(GraphType.REGION);
  }
}
