/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.io.Serializable;

import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.EventID;

/**
 * Implementing class for <code>Conflatable</code> interface. Objects of this
 * class will be add to the queue
 * 
 * @author Dinesh Patel
 * 
 */
public class ConflatableObject implements Conflatable, Serializable
{

  /** The key for this entry */
  private Object key;

  /** The value for this entry */
  private Object value;

  /** The unique <code>EventID</code> object for this entry */
  private EventID id;

  /** boolean to indicate whether this entry should be conflated or not */
  private boolean conflate;

  /** The region to which this entry belongs */
  private String regionname;

  public ConflatableObject() {

  }

  /**
   * Constructor
   * 
   * @param key -
   *          The key for this entry
   * @param value -
   *          The value for this entry
   * @param eventid -
   *          eventID object for this entry
   * @param conflate -
   *          conflate it true
   * @param regionname -
   *          The region to which this entry belongs
   */
  public ConflatableObject(Object key, Object value, EventID eventId,
      boolean conflate, String regionname) {
    this.key = key;
    this.value = value;
    this.id = eventId;
    this.conflate = conflate;
    this.regionname = regionname;
  }

  /**
   * Returns whether the object should be conflated
   * 
   * @return whether the object should be conflated
   */
  public boolean shouldBeConflated()
  {
    return this.conflate;
  }

  /**
   * Returns the name of the region for this <code>Conflatable</code>
   * 
   * @return the name of the region for this <code>Conflatable</code>
   */
  public String getRegionToConflate()
  {
    return this.regionname;
  }

  /**
   * Returns the key for this <code>Conflatable</code>
   * 
   * @return the key for this <code>Conflatable</code>
   */
  public Object getKeyToConflate()
  {
    return this.key;
  }

  /**
   * Returns the value for this <code>Conflatable</code>
   * 
   * @return the value for this <code>Conflatable</code>
   */
  public Object getValueToConflate()
  {
    return this.value;
  }

  /**
   * Sets the latest value for this <code>Conflatable</code>
   * 
   * @param value
   *          The latest value
   */
  public void setLatestValue(Object value)
  {
     throw new UnsupportedOperationException("setLatestValue should not be used");
  }

  /**
   * Return this event's identifier
   * 
   * @return this event's identifier
   */
  public EventID getEventId()
  {
    return this.id;
  }

  /**
   * @return Returns the conflate.
   */
  final boolean isConflate()
  {
    return conflate;
  }

  /**
   * @param conflate
   *          The conflate to set.
   */
  final void setConflate(boolean conflate)
  {
    this.conflate = conflate;
  }

  /**
   * @return Returns the id.
   */
  final EventID getId()
  {
    return id;
  }

  /**
   * @param id
   *          The id to set.
   */
  final void setId(EventID id)
  {
    this.id = id;
  }

  /**
   * @return Returns the key.
   */
  final Object getKey()
  {
    return key;
  }

  /**
   * @param key
   *          The key to set.
   */
  final void setKey(Object key)
  {
    this.key = key;
  }

  /**
   * @return Returns the regionname.
   */
  final String getRegionname()
  {
    return regionname;
  }

  /**
   * @param regionname
   *          The regionname to set.
   */
  final void setRegionname(String regionname)
  {
    this.regionname = regionname;
  }

  /**
   * @return Returns the value.
   */
  final Object getValue()
  {
    return value;
  }

  /**
   * @param value
   *          The value to set.
   */
  final void setValue(Object value)
  {
    this.value = value;
  }
}
