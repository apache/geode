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

import java.io.DataOutput;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import org.apache.geode.DataSerializable;

public interface LogMarker {

  /**
   * @deprecated GEMFIRE_VERBOSE is deprecated in favor of GEODE_VERBOSE
   */
  @Deprecated
  Marker GEMFIRE_VERBOSE = MarkerManager.getMarker("GEMFIRE_VERBOSE");

  /**
   * GEODE_VERBOSE is a parent to all other markers so that they can all be turned off with<br>
   * &ltMarkerFilter marker="GEODE_VERBOSE" onMatch="DENY" onMismatch="NEUTRAL"/&gt
   */
  Marker GEODE_VERBOSE = MarkerManager.getMarker("GEODE_VERBOSE").setParents(GEMFIRE_VERBOSE);

  Marker BRIDGE_SERVER = MarkerManager.getMarker("BRIDGE_SERVER").addParents(GEODE_VERBOSE);
  Marker DLS = MarkerManager.getMarker("DLS").addParents(GEODE_VERBOSE);

  Marker PERSIST = MarkerManager.getMarker("PERSIST").addParents(GEODE_VERBOSE);
  Marker PERSIST_VIEW = MarkerManager.getMarker("PERSIST_VIEW").addParents(PERSIST);
  Marker PERSIST_ADVISOR = MarkerManager.getMarker("PERSIST_ADVISOR").addParents(PERSIST);
  Marker PERSIST_RECOVERY = MarkerManager.getMarker("PERSIST_RECOVERY").addParents(PERSIST);
  Marker PERSIST_WRITES = MarkerManager.getMarker("PERSIST_WRITES").addParents(PERSIST);

  Marker TOMBSTONE = MarkerManager.getMarker("TOMBSTONE").addParents(GEODE_VERBOSE);
  Marker TOMBSTONE_COUNT = MarkerManager.getMarker("TOMBSTONE_COUNT").addParents(TOMBSTONE);

  Marker LRU = MarkerManager.getMarker("LRU").addParents(GEODE_VERBOSE);
  Marker LRU_TOMBSTONE_COUNT =
      MarkerManager.getMarker("LRU_TOMBSTONE_COUNT").addParents(LRU, TOMBSTONE_COUNT);
  Marker LRU_CLOCK = MarkerManager.getMarker("LRU_CLOCK").addParents(LRU);

  Marker RVV = MarkerManager.getMarker("RVV").addParents(GEODE_VERBOSE);
  Marker VERSION_TAG = MarkerManager.getMarker("VERSION_TAG").addParents(GEODE_VERBOSE); // gemfire.VersionTag.DEBUG
  Marker VERSIONED_OBJECT_LIST =
      MarkerManager.getMarker("VERSIONED_OBJECT_LIST").addParents(GEODE_VERBOSE); // gemfire.VersionedObjectList.DEBUG

  // cache.tier.sockets
  Marker OBJECT_PART_LIST = MarkerManager.getMarker("OBJECT_PART_LIST").addParents(GEODE_VERBOSE); // gemfire.ObjectPartList.DEBUG

  Marker SERIALIZER = MarkerManager.getMarker("SERIALIZER").addParents(GEODE_VERBOSE); // DataSerializer.DEBUG
  /**
   * If the <code>"DataSerializer.DUMP_SERIALIZED"</code> system property is set, the class names of
   * the objects that are serialized by
   * {@link org.apache.geode.DataSerializer#writeObject(Object, DataOutput)} using standard Java
   * serialization are logged to {@linkplain System#out standard out}. This aids in determining
   * which classes should implement {@link DataSerializable} (or should be special cased by a custom
   * <code>DataSerializer</code>).
   */
  Marker DUMP_SERIALIZED = MarkerManager.getMarker("DUMP_SERIALIZED").addParents(SERIALIZER); // DataSerializer.DUMP_SERIALIZED
  Marker TRACE_SERIALIZABLE = MarkerManager.getMarker("TRACE_SERIALIZABLE").addParents(SERIALIZER); // DataSerializer.TRACE_SERIALIZABLE
  Marker DEBUG_DSFID = MarkerManager.getMarker("DEBUG_DSFID").addParents(SERIALIZER); // DataSerializer.DEBUG_DSFID

  Marker STATISTICS = MarkerManager.getMarker("STATISTICS").addParents(GEODE_VERBOSE);
  Marker STATE_FLUSH_OP = MarkerManager.getMarker("STATE_FLUSH_OP").addParents(GEODE_VERBOSE);

  Marker DISTRIBUTION = MarkerManager.getMarker("DISTRIBUTION").addParents(GEODE_VERBOSE);
  Marker DISTRIBUTION_STATE_FLUSH_OP = MarkerManager.getMarker("DISTRIBUTION_STATE_FLUSH_OP")
      .addParents(DISTRIBUTION, STATE_FLUSH_OP);
  Marker DISTRIBUTION_BRIDGE_SERVER =
      MarkerManager.getMarker("DISTRIBUTION_BRIDGE_SERVER").addParents(DISTRIBUTION, BRIDGE_SERVER);
  Marker DISTRIBUTION_VIEWS =
      MarkerManager.getMarker("DISTRIBUTION_VIEWS").addParents(DISTRIBUTION);
  Marker DM = MarkerManager.getMarker("DM").addParents(DISTRIBUTION);
  Marker DM_BRIDGE_SERVER = MarkerManager.getMarker("DM_BRIDGE").addParents(BRIDGE_SERVER, DM);
  Marker DA = MarkerManager.getMarker("DA").addParents(DISTRIBUTION);

  Marker GII = MarkerManager.getMarker("GII").addParents(GEODE_VERBOSE);
  Marker GII_VERSIONED_ENTRY = MarkerManager.getMarker("GII_VERSION_ENTRY").addParents(GII);

  Marker JGROUPS = MarkerManager.getMarker("JGROUPS").addParents(GEODE_VERBOSE);

  Marker QA = MarkerManager.getMarker("QA").addParents(GEODE_VERBOSE);

  Marker P2P = MarkerManager.getMarker("P2P").addParents(GEODE_VERBOSE);

  Marker CONFIG = MarkerManager.getMarker("CONFIG");

  Marker PERSISTENCE = MarkerManager.getMarker("PERSISTENCE").addParents(GEODE_VERBOSE);
  Marker DISK_STORE_MONITOR = MarkerManager.getMarker("DISK_STORE_MONITOR").addParents(PERSISTENCE);
  Marker SOPLOG = MarkerManager.getMarker("SOPLOG").addParents(PERSISTENCE);

  Marker MANAGED_ENTITY = MarkerManager.getMarker("MANAGED_ENTITY").addParents(GEODE_VERBOSE);

  Marker CACHE_XML = MarkerManager.getMarker("CACHE_XML").addParents(GEODE_VERBOSE);
  Marker CACHE_XML_PARSER =
      MarkerManager.getMarker("CACHE_XML_PARSER").addParents(GEODE_VERBOSE, CACHE_XML);
}
