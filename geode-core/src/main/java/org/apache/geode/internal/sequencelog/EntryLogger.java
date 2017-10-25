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

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.entries.DiskEntry.RecoveredEntry;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;


/**
 * A wrapper around the graph logger that logs entry level events.
 *
 *
 * TODO - I think I need some options to choose to deserialize a key to record states.
 */
public class EntryLogger {
  private static final SequenceLogger GRAPH_LOGGER = SequenceLoggerImpl.getInstance();
  private static ThreadLocal<String> SOURCE = new ThreadLocal<String>();
  private static ThreadLocal<String> SOURCE_TYPE = new ThreadLocal<String>();

  public static final String TRACK_VALUES_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "EntryLogger.TRACK_VALUES";
  private static final boolean TRACK_VALUES = Boolean.getBoolean(TRACK_VALUES_PROPERTY);

  public static void clearSource() {
    if (isEnabled()) {
      SOURCE.set(null);
      SOURCE_TYPE.set(null);
    }
  }

  public static void setSource(Object source, String sourceType) {
    if (isEnabled()) {
      SOURCE.set(source.toString());
      SOURCE_TYPE.set(sourceType);
    }
  }

  public static void logPut(EntryEventImpl event) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("put"),
          processValue(event.getRawNewValue()), getSource(), getDest());
    }
  }

  private static String getEdgeName(String transition) {
    String sourceType = SOURCE_TYPE.get();
    if (sourceType == null) {
      sourceType = "";
    } else {
      sourceType = sourceType + " ";
    }
    return sourceType + transition;
  }

  public static void logInvalidate(EntryEventImpl event) {
    if (isEnabled()) {
      final String invalidationType = event.getOperation().isLocal() ? "local_invalid" : "invalid";
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("invalidate"),
          invalidationType, getSource(), getDest());
    }
  }

  public static void logDestroy(EntryEventImpl event) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("destroy"),
          "destroyed", getSource(), getDest());
    }
  }

  public static void logRecovery(Object owner, Object key, RecoveredEntry value) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), "recovery",
          processValue(value.getValue()), getSource(), getDest());
    }
  }

  public static void logPersistPut(String name, Object key, DiskStoreID diskStoreID) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(name, key), "persist", "persisted",
          getDest(), diskStoreID);
    }
  }

  public static void logPersistDestroy(String name, Object key, DiskStoreID diskStoreID) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(name, key), "persist_destroy",
          "destroy", getDest(), diskStoreID);
    }
  }

  public static void logInitialImagePut(Object owner, Object key, Object newValue) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), "GII",
          processValue(newValue), getSource(), getDest());
    }
  }

  public static void logTXDestroy(Object owner, Object key) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key),
          getEdgeName("txdestroy"), "destroyed", getSource(), getDest());
    }
  }

  public static void logTXInvalidate(Object owner, Object key) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key),
          getEdgeName("txinvalidate"), "invalid", getSource(), getDest());
    }
  }

  public static void logTXPut(Object owner, Object key, Object nv) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key),
          getEdgeName("txput"), processValue(nv), getSource(), getDest());
    }
  }

  public static boolean isEnabled() {
    return GRAPH_LOGGER.isEnabled(GraphType.KEY);
  }

  private static Object getDest() {
    return InternalDistributedSystem.getAnyInstance().getMemberId();
  }

  private static Object getSource() {
    Object source = SOURCE.get();
    if (source == null) {
      source = InternalDistributedSystem.getAnyInstance().getMemberId();
    }
    return source;
  }

  private static Object processValue(@Unretained Object rawNewValue) {
    if (rawNewValue != null && Token.isInvalid(rawNewValue)) {
      return "invalid";
    }

    if (!TRACK_VALUES) {
      return "present";
    }
    if (rawNewValue instanceof StoredObject) {
      return "off-heap";
    }
    if (rawNewValue instanceof CachedDeserializable) {
      rawNewValue = ((CachedDeserializable) rawNewValue).getDeserializedForReading();
    }
    if (rawNewValue instanceof byte[]) {
      return "serialized:" + hash((byte[]) rawNewValue);
    }

    return rawNewValue;
  }

  private static Object hash(byte[] rawNewValue) {
    int hash = 17;
    int length = rawNewValue.length;
    if (length > 100) {
      length = 100;
    }
    for (int i = 0; i < length; i++) {
      hash = 31 * hash + rawNewValue[i];
    }

    return Integer.valueOf(hash);
  }

  private static String getGraphName(EntryEventImpl event) {
    return getGraphName(event.getRegion().getFullPath(), event.getKey());
  }

  private static String getGraphNameFromOwner(Object owner, Object key) {
    String ownerName;
    if (owner instanceof LocalRegion) {
      ownerName = ((LocalRegion) owner).getFullPath();
    } else if (owner instanceof PlaceHolderDiskRegion) {
      ownerName = ((PlaceHolderDiskRegion) owner).getName();
    } else {
      ownerName = owner.toString();
    }
    return getGraphName(ownerName, key);
  }

  private static String getGraphName(String ownerName, Object key) {
    return ownerName + ":" + key;
  }

  public static void logUpdateEntryVersion(EntryEventImpl event) {
    if (isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("update-version"),
          "version-updated", getSource(), getDest());
    }
  }

}
