/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

import java.util.UUID;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.DiskEntry.RecoveredEntry;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PlaceHolderDiskRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;


/**
 * A wrapper around the graph logger that logs entry level events.
 * @author dsmith
 *
 *
 *TODO 
 *  - I think I need some options to choose to deserialize a key to record states.
 */
public class EntryLogger {
  private static final SequenceLogger GRAPH_LOGGER = SequenceLoggerImpl.getInstance();
  private static ThreadLocal<String> SOURCE = new ThreadLocal<String>();
  private static ThreadLocal<String> SOURCE_TYPE = new ThreadLocal<String>();
  
  public static final String TRACK_VALUES_PROPERTY = "gemfire.EntryLogger.TRACK_VALUES";
  private static final boolean TRACK_VALUES = Boolean.getBoolean(TRACK_VALUES_PROPERTY);
  
  public static void clearSource() {
    if(isEnabled()) {
      SOURCE.set(null);
      SOURCE_TYPE.set(null);
    }
  }

  public static void setSource(Object source, String sourceType) {
    if(isEnabled()) {
      SOURCE.set(source.toString());
      SOURCE_TYPE.set(sourceType);
    }
  }

  public static void logPut(EntryEventImpl event) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("put"), processValue(event.getRawNewValue()), getSource(), getDest());
    }
  }

  private static String getEdgeName(String transition) {
    String sourceType = SOURCE_TYPE.get();
    if(sourceType == null) {
      sourceType = "";
    } else {
      sourceType = sourceType + " ";
    }
    return sourceType + transition;
  }

  public static void logInvalidate(EntryEventImpl event) {
    if(isEnabled()) {
      final String invalidationType = event.getOperation().isLocal() ? "local_invalid" : "invalid";
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("invalidate"), invalidationType, getSource(), getDest());
    }
  }
  
  public static void logDestroy(EntryEventImpl event) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("destroy"), "destroyed", getSource(), getDest());
    }
  }

  public static void logRecovery(Object owner, Object key, RecoveredEntry value) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), "recovery", processValue(value.getValue()), getSource(), getDest());
    }
  }
  
  public static void logPersistPut(String name, Object key, DiskStoreID diskStoreID) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(name, key), "persist", "persisted", getDest(), diskStoreID);
    }
  }

  public static void logPersistDestroy(String name, Object key, DiskStoreID diskStoreID) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(name, key), "persist_destroy", "destroy", getDest(), diskStoreID);
    }
  }

  public static void logInitialImagePut(Object owner, Object key, Object newValue) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), "GII", processValue(newValue), getSource(), getDest());
    }
  }
  
  public static void logTXDestroy(Object owner, Object key) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), getEdgeName("txdestroy"), "destroyed", getSource(), getDest());
    }
  }

  public static void logTXInvalidate(Object owner, Object key) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), getEdgeName("txinvalidate"), "invalid", getSource(), getDest());
    }
  }

  public static void logTXPut(Object owner, Object key, Object nv) {
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphNameFromOwner(owner, key), getEdgeName("txput"), processValue(nv), getSource(), getDest());
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
    if(source == null) {
      source = InternalDistributedSystem.getAnyInstance().getMemberId();
    }
    return source;
  }

  private static Object processValue(Object rawNewValue) {
    if(rawNewValue != null && Token.isInvalid(rawNewValue)) {
      return "invalid";
    }
    
    if(!TRACK_VALUES) {
      return "present";
    }
    if(rawNewValue instanceof CachedDeserializable) {
      rawNewValue = ((CachedDeserializable) rawNewValue).getDeserializedForReading();
    }
    if(rawNewValue instanceof byte[]) {
      return "serialized:" + hash((byte[])rawNewValue);
    }
    
    return rawNewValue;
  }

  private static Object hash(byte[] rawNewValue) {
    int hash = 17;
    int length = rawNewValue.length;
    if(length > 100) {
      length = 100;
    }
    for(int i =0; i < length; i++) {
      hash = 31 * hash + rawNewValue[i]; 
    }
    
    return Integer.valueOf(hash);
  }
  
  private static String getGraphName(EntryEventImpl event) {
    return getGraphName(event.getRegion().getFullPath(), event.getKey());
  }
 
  private static String getGraphNameFromOwner(Object owner, Object key) {
    String ownerName;
    if(owner instanceof LocalRegion) {
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
    if(isEnabled()) {
      GRAPH_LOGGER.logTransition(GraphType.KEY, getGraphName(event), getEdgeName("update-version"), "version-updated", getSource(), getDest());
    }
  }

}