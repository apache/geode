/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Class <code>BridgeEventConflator</code> handles bridge event conflation.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
public class BridgeEventConflator {

  private static final Logger logger = LogService.getLogger();

  /**
   * The mapping of latest values for region and key.
   */
  protected Map _latestValues;

  /**
   * An object used for synchronizing the latest values map
   */
  protected Object _latestValuesLock = new Object();

  /**
   * Constructor.
   */
  public BridgeEventConflator() {
    this._latestValues = new HashMap();
  }

  /**
   * Stores the latest value in the latest values map.
   *
   * @param object The <code>Conflatable</code> whose
   * value to add to the latest values map.
   *
   * @return Whether to add the <code>Conflatable</code> to the queue
   * of the caller. The object should be added to the queue if either
   * (a) it should not be conflated or (b) the latest values
   * map does not already contain an entry for the object's key.
   */
  public boolean storeLatestValue(Conflatable object) {
    boolean shouldAddToQueue = true;
    if (object.shouldBeConflated()) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Conflating {}", this, object);
      }

      // Determine whether this region / key combination is already in the
      // latest values. Set the shouldAddToQueue flag accordingly.
      String regionName = object.getRegionToConflate();
      Object key = object.getKeyToConflate();
      synchronized (this._latestValuesLock) {
        Map latestValuesForRegion = (Map) this._latestValues.get(regionName);
        if (latestValuesForRegion == null) {
          latestValuesForRegion = new HashMap();
          this._latestValues.put(regionName, latestValuesForRegion);
          // Since the latest values is a new map, the object should
          // be added to the queue
          shouldAddToQueue = true;
        } else {
          // If the key is not contained in the latest values map, then
          // the object should be added to the queue;
          // otherwise it should not be added to the queue.
          shouldAddToQueue = !latestValuesForRegion.containsKey(key);
        }

        // Put the object's value into the latest values
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Replacing value={} with value={} for key={} in latest values map for {}", this,
              deserialize(latestValuesForRegion.get(key)), deserialize(object.getValueToConflate()), key, regionName);
        }
        latestValuesForRegion.put(key, object.getValueToConflate());
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Not conflating {}", this, object);
      }
      shouldAddToQueue = true;
    }
    return shouldAddToQueue;
  }

  /**
   * Updates the input <code>Conflatable</code>'s latest value from the
   * latest values map.
   *
   * @param object The <code>Conflatable</code> whose latest value to
   * retrieve and update.
   */
  public byte[] getLatestValue(Conflatable object) {
    // If the object should not be conflated, it already contains
    // the latest value.
    if (!object.shouldBeConflated()) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} already has latest value", this, object);
      }
      // Return the object's current value
      return (byte[]) object.getValueToConflate();
    }

    // Otherwise, get the latest value from the latest values map.
    String regionName = object.getRegionToConflate();
    Object key = object.getKeyToConflate();
    byte[] latestValue = null;
    synchronized (this._latestValuesLock) {
      // Get the latest values map for the region and the latest value
      // for the key. The latest values map can be null if no objects
      // for the region are to be conflated.
      Map latestValuesForRegion = (Map) this._latestValues.get(regionName);
      if (latestValuesForRegion != null) {
        // Get the latest value. If it is not null, replace the object's
        // original value with the latest one. If it is null, it means that
        // the object should not be conflated. Keep the original value.
        latestValue = (byte[]) latestValuesForRegion.remove(key);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: {} using latest value: {}", this, object, deserialize(latestValue));
        }
        if (latestValue == null) {
          latestValue = (byte[]) object.getValueToConflate();
        }
      }
    }
    return latestValue;
  }

  @Override
  public String toString() {
    return "BridgeEventConflator";
  }

  protected Object deserialize(Object value) {
    Object deserializedObject = value;
    // This is a debugging method so ignore all exceptions like ClassNotFoundException
    try {
      if (value instanceof byte[]) {
        byte[] serializedBytes = (byte[]) value;
        deserializedObject = EntryEventImpl.deserialize(serializedBytes);
      }
    } catch (Exception e) {}
    return deserializedObject;
  }
}
