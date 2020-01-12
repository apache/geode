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
package org.apache.geode.internal.logging.log4j;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import org.apache.geode.annotations.Immutable;

/**
 * This collection of {@link org.apache.logging.log4j.Marker} objects offer finer control of
 * logging. The majority of these markers are concerned with TRACE level logging, the intent of
 * which should be consistently clear by ending in *_VERBOSE. Additionally, these markers, even
 * with the TRACE level active, will be disabled in the default Log4j2 configuration
 * provided by {@code main/resources/log4j2.xml}
 *
 * <p>
 * Some additional markers exist for log levels more coarse than TRACE or DEBUG.
 * These markers end in *_MARKER for clear distinction from the *_VERBOSE markers.
 */
public interface LogMarker {

  // Non-verbose parent markers
  @Immutable
  Marker CONFIG_MARKER = MarkerManager.getMarker("CONFIG_MARKER");
  @Immutable
  Marker DISK_STORE_MONITOR_MARKER = MarkerManager.getMarker("DISK_STORE_MONITOR_MARKER");
  @Immutable
  Marker DISTRIBUTION_MARKER = MarkerManager.getMarker("DISTRIBUTION_MARKER");
  @Immutable
  Marker DLS_MARKER = MarkerManager.getMarker("DLS_MARKER");
  @Immutable
  Marker DM_MARKER = MarkerManager.getMarker("DM").addParents(DISTRIBUTION_MARKER);
  @Immutable
  Marker SERIALIZER_MARKER = MarkerManager.getMarker("SERIALIZER_MARKER");
  @Immutable
  Marker STATISTICS_MARKER = MarkerManager.getMarker("STATISTICS_MARKER");

  // Verbose parent markers
  /** @deprecated Use GEODE_VERBOSE */
  @Immutable
  Marker GEMFIRE_VERBOSE = MarkerManager.getMarker("GEMFIRE_VERBOSE");
  @Immutable
  Marker GEODE_VERBOSE = MarkerManager.getMarker("GEODE_VERBOSE").setParents(GEMFIRE_VERBOSE);

  // Verbose child markers. Every marker below should
  // (a) end in *_VERBOSE and (b) have at least one *_VERBOSE parent
  @Immutable
  Marker BRIDGE_SERVER_VERBOSE =
      MarkerManager.getMarker("BRIDGE_SERVER_VERBOSE").addParents(GEODE_VERBOSE);

  @Immutable
  Marker CACHE_XML_PARSER_VERBOSE =
      MarkerManager.getMarker("CACHE_XML_PARSER_VERBOSE").addParents(GEODE_VERBOSE);

  @Immutable
  Marker DISK_STORE_MONITOR_VERBOSE = MarkerManager.getMarker("DISK_STORE_MONITOR_VERBOSE")
      .addParents(DISK_STORE_MONITOR_MARKER, GEODE_VERBOSE);

  @Immutable
  Marker DLS_VERBOSE = MarkerManager.getMarker("DLS_VERBOSE").addParents(DLS_MARKER, GEODE_VERBOSE);

  @Immutable
  Marker PERSIST_VERBOSE = MarkerManager.getMarker("PERSIST_VERBOSE").addParents(GEODE_VERBOSE);
  @Immutable
  Marker PERSIST_ADVISOR_VERBOSE =
      MarkerManager.getMarker("PERSIST_ADVISOR_VERBOSE").addParents(PERSIST_VERBOSE);
  @Immutable
  Marker PERSIST_RECOVERY_VERBOSE =
      MarkerManager.getMarker("PERSIST_RECOVERY_VERBOSE").addParents(PERSIST_VERBOSE);
  @Immutable
  Marker PERSIST_WRITES_VERBOSE =
      MarkerManager.getMarker("PERSIST_WRITES_VERBOSE").addParents(PERSIST_VERBOSE);

  @Immutable
  Marker RVV_VERBOSE = MarkerManager.getMarker("RVV_VERBOSE").addParents(GEODE_VERBOSE);

  @Immutable
  Marker TOMBSTONE_VERBOSE = MarkerManager.getMarker("TOMBSTONE_VERBOSE").addParents(GEODE_VERBOSE);
  @Immutable
  Marker TOMBSTONE_COUNT_VERBOSE =
      MarkerManager.getMarker("TOMBSTONE_COUNT_VERBOSE").addParents(TOMBSTONE_VERBOSE);

  @Immutable
  Marker LRU_VERBOSE = MarkerManager.getMarker("LRU_VERBOSE").addParents(GEODE_VERBOSE);
  @Immutable
  Marker LRU_CLOCK_VERBOSE = MarkerManager.getMarker("LRU_CLOCK_VERBOSE").addParents(LRU_VERBOSE);
  @Immutable
  Marker LRU_TOMBSTONE_COUNT_VERBOSE = MarkerManager.getMarker("LRU_TOMBSTONE_COUNT_VERBOSE")
      .addParents(LRU_VERBOSE, TOMBSTONE_COUNT_VERBOSE);

  @Immutable
  Marker SERIALIZER_VERBOSE =
      MarkerManager.getMarker("SERIALIZER_VERBOSE").addParents(SERIALIZER_MARKER, GEODE_VERBOSE);
  @Immutable
  Marker SERIALIZER_ANNOUNCE_TYPE_WRITTEN_VERBOSE = MarkerManager
      .getMarker("SERIALIZER_ANNOUNCE_TYPE_WRITTEN_VERBOSE").addParents(SERIALIZER_VERBOSE);
  @Immutable
  Marker SERIALIZER_WRITE_DSFID_VERBOSE =
      MarkerManager.getMarker("SERIALIZER_WRITE_DSFID_VERBOSE").addParents(SERIALIZER_VERBOSE);

  @Immutable
  Marker STATISTICS_VERBOSE =
      MarkerManager.getMarker("STATISTICS_VERBOSE").addParents(STATISTICS_MARKER, GEODE_VERBOSE);

  @Immutable
  Marker STATE_FLUSH_OP_VERBOSE =
      MarkerManager.getMarker("STATE_FLUSH_OP_VERBOSE").addParents(GEODE_VERBOSE);

  @Immutable
  Marker DISTRIBUTION_STATE_FLUSH_VERBOSE =
      MarkerManager.getMarker("DISTRIBUTION_STATE_FLUSH_VERBOSE").addParents(DISTRIBUTION_MARKER,
          STATE_FLUSH_OP_VERBOSE);
  @Immutable
  Marker DISTRIBUTION_BRIDGE_SERVER_VERBOSE =
      MarkerManager.getMarker("DISTRIBUTION_BRIDGE_SERVER_VERBOSE").addParents(DISTRIBUTION_MARKER,
          BRIDGE_SERVER_VERBOSE);
  @Immutable
  Marker DISTRIBUTION_ADVISOR_VERBOSE = MarkerManager.getMarker("DISTRIBUTION_ADVISOR_VERBOSE")
      .addParents(DISTRIBUTION_MARKER, GEODE_VERBOSE);

  @Immutable
  Marker DM_VERBOSE =
      MarkerManager.getMarker("DM_VERBOSE").addParents(DISTRIBUTION_MARKER, GEODE_VERBOSE);
  @Immutable
  Marker DM_BRIDGE_SERVER_VERBOSE = MarkerManager.getMarker("DM_BRIDGE_SERVER_VERBOSE")
      .addParents(DM_VERBOSE, BRIDGE_SERVER_VERBOSE);
  @Immutable
  Marker EVENT_ID_TO_STRING_VERBOSE =
      MarkerManager.getMarker("EVENT_ID_TO_STRING_VERBOSE").addParents(DM_BRIDGE_SERVER_VERBOSE);

  @Immutable
  Marker INITIAL_IMAGE_VERBOSE =
      MarkerManager.getMarker("INITIAL_IMAGE_VERBOSE").addParents(GEODE_VERBOSE);
  @Immutable
  Marker INITIAL_IMAGE_VERSIONED_VERBOSE =
      MarkerManager.getMarker("INITIAL_IMAGE_VERSIONED_VERBOSE").addParents(INITIAL_IMAGE_VERBOSE);

  @Immutable
  Marker MANAGED_ENTITY_VERBOSE =
      MarkerManager.getMarker("MANAGED_ENTITY_VERBOSE").addParents(GEODE_VERBOSE);

  @Immutable
  Marker VERSION_TAG_VERBOSE =
      MarkerManager.getMarker("VERSION_TAG_VERBOSE").addParents(GEODE_VERBOSE);
  @Immutable
  Marker VERSIONED_OBJECT_LIST_VERBOSE =
      MarkerManager.getMarker("VERSIONED_OBJECT_LIST_VERBOSE").addParents(GEODE_VERBOSE);
}
