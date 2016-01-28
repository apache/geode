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
package com.gemstone.gemfire.cache.query.internal.cq;

import java.io.Serializable;

import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Implementing class for <code>Conflatable</code> interface. Objects of this
 * class will be add to the queue
 * 
 * @author Dinesh Patel
 * 
 */
public class CqConflatable implements Conflatable, Serializable
{
  private static final long serialVersionUID = -7215022132135862557L;

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

  public CqConflatable() {

  }

  /**
   * Constructor
   * 
   * @param key -
   *          The key for this entry
   * @param value -
   *          The value for this entry
   * @param eventId -
   *          eventID object for this entry
   * @param conflate -
   *          conflate it true
   * @param regionname -
   *          The region to which this entry belongs
   */
  public CqConflatable(Object key, Object value, EventID eventId,
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
     throw new UnsupportedOperationException(LocalizedStrings.CqConflatable_SETLATESTVALUE_SHOULD_NOT_BE_USED.toLocalizedString());
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
