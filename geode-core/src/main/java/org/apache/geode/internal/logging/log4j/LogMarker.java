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
package org.apache.geode.internal.logging.log4j;

import java.io.DataOutput;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import org.apache.geode.DataSerializable;

public interface LogMarker {
  /**
   * GEMFIRE_VERBOSE is a parent to all other markers so that they can all be turned off with<br>
   * &ltMarkerFilter marker="GEMFIRE_VERBOSE" onMatch="DENY" onMismatch="NEUTRAL"/&gt
   */
  public static final Marker GEMFIRE_VERBOSE = MarkerManager.getMarker("GEMFIRE_VERBOSE");
  
  public static final Marker BRIDGE_SERVER = MarkerManager.getMarker("BRIDGE_SERVER").addParents(GEMFIRE_VERBOSE);
  public static final Marker DLS = MarkerManager.getMarker("DLS").addParents(GEMFIRE_VERBOSE);
  
  public static final Marker PERSIST = MarkerManager.getMarker("PERSIST").addParents(GEMFIRE_VERBOSE);
  public static final Marker PERSIST_VIEW = MarkerManager.getMarker("PERSIST_VIEW").addParents(PERSIST);
  public static final Marker PERSIST_ADVISOR = MarkerManager.getMarker("PERSIST_ADVISOR").addParents(PERSIST);
  public static final Marker PERSIST_RECOVERY = MarkerManager.getMarker("PERSIST_RECOVERY").addParents(PERSIST);
  public static final Marker PERSIST_WRITES = MarkerManager.getMarker("PERSIST_WRITES").addParents(PERSIST);
  
  public static final Marker TOMBSTONE = MarkerManager.getMarker("TOMBSTONE").addParents(GEMFIRE_VERBOSE);
  public static final Marker TOMBSTONE_COUNT = MarkerManager.getMarker("TOMBSTONE_COUNT").addParents(TOMBSTONE);
  
  public static final Marker LRU = MarkerManager.getMarker("LRU").addParents(GEMFIRE_VERBOSE);
  public static final Marker LRU_TOMBSTONE_COUNT = MarkerManager.getMarker("LRU_TOMBSTONE_COUNT").addParents(LRU, TOMBSTONE_COUNT);
  public static final Marker LRU_CLOCK = MarkerManager.getMarker("LRU_CLOCK").addParents(LRU);
  
  public static final Marker RVV = MarkerManager.getMarker("RVV").addParents(GEMFIRE_VERBOSE);
  public static final Marker VERSION_TAG = MarkerManager.getMarker("VERSION_TAG").addParents(GEMFIRE_VERBOSE); // gemfire.VersionTag.DEBUG
  public static final Marker VERSIONED_OBJECT_LIST = MarkerManager.getMarker("VERSIONED_OBJECT_LIST").addParents(GEMFIRE_VERBOSE); // gemfire.VersionedObjectList.DEBUG

  // cache.tier.sockets
  public static final Marker OBJECT_PART_LIST = MarkerManager.getMarker("OBJECT_PART_LIST").addParents(GEMFIRE_VERBOSE); // gemfire.ObjectPartList.DEBUG
  
  public static final Marker SERIALIZER = MarkerManager.getMarker("SERIALIZER").addParents(GEMFIRE_VERBOSE); // DataSerializer.DEBUG
  /** If the <code>"DataSerializer.DUMP_SERIALIZED"</code> system
   * property is set, the class names of the objects that are
   * serialized by {@link org.apache.geode.DataSerializer#writeObject(Object, DataOutput)} using standard Java
   * serialization are logged to {@linkplain System#out standard out}.
   * This aids in determining which classes should implement {@link
   * DataSerializable} (or should be special cased by a custom
   * <code>DataSerializer</code>). */
  public static final Marker DUMP_SERIALIZED = MarkerManager.getMarker("DUMP_SERIALIZED").addParents(SERIALIZER); // DataSerializer.DUMP_SERIALIZED
  public static final Marker TRACE_SERIALIZABLE = MarkerManager.getMarker("TRACE_SERIALIZABLE").addParents(SERIALIZER); // DataSerializer.TRACE_SERIALIZABLE
  public static final Marker DEBUG_DSFID = MarkerManager.getMarker("DEBUG_DSFID").addParents(SERIALIZER); // DataSerializer.DEBUG_DSFID
  
  public static final Marker STATISTICS = MarkerManager.getMarker("STATISTICS").addParents(GEMFIRE_VERBOSE);
  public static final Marker STATE_FLUSH_OP = MarkerManager.getMarker("STATE_FLUSH_OP").addParents(GEMFIRE_VERBOSE);
  
  public static final Marker DISTRIBUTION = MarkerManager.getMarker("DISTRIBUTION").addParents(GEMFIRE_VERBOSE);
  public static final Marker DISTRIBUTION_STATE_FLUSH_OP = MarkerManager.getMarker("DISTRIBUTION_STATE_FLUSH_OP").addParents(DISTRIBUTION, STATE_FLUSH_OP);
  public static final Marker DISTRIBUTION_BRIDGE_SERVER = MarkerManager.getMarker("DISTRIBUTION_BRIDGE_SERVER").addParents(DISTRIBUTION, BRIDGE_SERVER);
  public static final Marker DISTRIBUTION_VIEWS = MarkerManager.getMarker("DISTRIBUTION_VIEWS").addParents(DISTRIBUTION);
  public static final Marker DM = MarkerManager.getMarker("DM").addParents(DISTRIBUTION);
  public static final Marker DM_BRIDGE_SERVER = MarkerManager.getMarker("DM_BRIDGE").addParents(BRIDGE_SERVER, DM);
  public static final Marker DA = MarkerManager.getMarker("DA").addParents(DISTRIBUTION);

  public static final Marker GII = MarkerManager.getMarker("GII").addParents(GEMFIRE_VERBOSE);
  public static final Marker GII_VERSIONED_ENTRY = MarkerManager.getMarker("GII_VERSION_ENTRY").addParents(GII);
  
  public static final Marker JGROUPS = MarkerManager.getMarker("JGROUPS").addParents(GEMFIRE_VERBOSE);
  
  public static final Marker QA = MarkerManager.getMarker("QA").addParents(GEMFIRE_VERBOSE);
  
  public static final Marker P2P = MarkerManager.getMarker("P2P").addParents(GEMFIRE_VERBOSE);
  
  public static final Marker CONFIG = MarkerManager.getMarker("CONFIG");
  
  public static final Marker PERSISTENCE = MarkerManager.getMarker("PERSISTENCE").addParents(GEMFIRE_VERBOSE);
  public static final Marker DISK_STORE_MONITOR = MarkerManager.getMarker("DISK_STORE_MONITOR").addParents(PERSISTENCE);
  public static final Marker SOPLOG = MarkerManager.getMarker("SOPLOG").addParents(PERSISTENCE);
  
  public static final Marker MANAGED_ENTITY = MarkerManager.getMarker("MANAGED_ENTITY").addParents(GEMFIRE_VERBOSE);
  
  public static final Marker CACHE_XML = MarkerManager.getMarker("CACHE_XML").addParents(GEMFIRE_VERBOSE);
  public static final Marker CACHE_XML_PARSER = MarkerManager.getMarker("CACHE_XML_PARSER").addParents(GEMFIRE_VERBOSE, CACHE_XML);
}
