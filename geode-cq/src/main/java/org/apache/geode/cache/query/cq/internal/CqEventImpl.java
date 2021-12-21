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
package org.apache.geode.cache.query.cq.internal;


/**
 * Interface for CqEvent. Offers methods to get information from CqEvent.
 *
 * @since GemFire 5.5
 */

import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.client.internal.QueueManager;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.internal.cache.EventID;

public class CqEventImpl implements CqEvent {

  private final CqQuery cQuery;
  private final Operation baseOp;
  private final Operation queryOp;
  private final Object newValue;
  private final Object key;
  private final byte[] delta;
  private Throwable throwable = null;
  private final QueueManager qManager;
  private final EventID eventId;

  CqEventImpl(CqQuery cQuery, Operation baseOp, Operation cqOp, Object key, Object value,
      byte[] deltaVal, QueueManager qManager, EventID eventId) {
    this.cQuery = cQuery;
    queryOp = cqOp;
    this.baseOp = baseOp;
    this.key = key;
    newValue = value;
    delta = deltaVal;
    // Handle Exception event.
    if (queryOp == null) {
      setException();
    }
    this.qManager = qManager;
    this.eventId = eventId;
  }

  @Override
  public CqQuery getCq() {
    return cQuery;
  }

  /**
   * Get the operation on the base region that triggered this event.
   */
  @Override
  public Operation getBaseOperation() {
    return baseOp;
  }

  /**
   * Get the the operation on the query results. Supported operations include update, create, and
   * destroy.
   */
  @Override
  public Operation getQueryOperation() {
    return queryOp;
  }

  /**
   * Get the key relating to the event.
   *
   * @return Object key.
   */
  @Override
  public Object getKey() {
    return key;
  }

  /**
   * Get the old value that was modified. If there is no old value because this is an insert, then
   * return null.
   */
  /*
   * public Object getOldValue() { return this.oldValue; }
   */

  /**
   * Get the new value of the modification. If there is no new value because this is a delete, then
   * return null.
   */
  @Override
  public Object getNewValue() {
    if (newValue == null && delta != null) {
      throw new InvalidDeltaException();
    }
    return newValue;
  }

  /**
   * If an error occurred, return the Throwable, otherwise return null. If an error occurred, then
   * this event will be passed to the <code>onError</code> method of the CqListener instead of the
   * <code>onEvent</code> method.
   */
  @Override
  public Throwable getThrowable() {
    return throwable;
  }

  @Override
  public byte[] getDeltaValue() {
    return delta;
  }

  /**
   * Checks if the Cq Event is for an exception. If so constructs the exception.
   */
  public void setException() {
    // Needs to be changed.
    throwable = new Throwable(
        "Exception occurred while applying query on a cache event.");
  }

  public void setException(String exceptionText) {
    throwable = new Throwable(exceptionText);
  }

  public QueueManager getQueueManager() {
    return qManager;
  }

  public EventID getEventID() {
    return eventId;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CqEvent [").append("CqName=").append(cQuery.getName())
        .append("; base operation=").append(baseOp).append("; cq operation=")
        .append(queryOp).append("; key=").append(key).append("; value=")
        .append(newValue).append("]");
    return buffer.toString();
  }
}
