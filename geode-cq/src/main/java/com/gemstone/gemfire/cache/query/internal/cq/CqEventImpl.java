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


/**
 * Interface for CqEvent. Offers methods to get information from
 * CqEvent.
 *
 * @author anil
 * @since 5.5
 */

import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.client.internal.QueueManager;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

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
  
  CqEventImpl(CqQuery cQuery, Operation baseOp, Operation cqOp, Object key, 
      Object value, byte[] deltaVal, QueueManager qManager, EventID eventId) {
    this.cQuery = cQuery;
    this.queryOp = cqOp; 
    this.baseOp = baseOp;
    this.key = key;
    this.newValue = value;
    this.delta = deltaVal;
    // Handle Exception event.
    if (this.queryOp == null) {
      setException();
    }
    this.qManager = qManager;
    this.eventId = eventId;
  }
    
  public CqQuery getCq() {
    return this.cQuery;
  }
    
  /** 
   * Get the operation on the base region that triggered this event.
   */
  public Operation getBaseOperation() {
    return this.baseOp;
  }
  
  /** 
   * Get the the operation on the query results. Supported operations include
   * update, create, and destroy.
   */
  public Operation getQueryOperation() {
    return this.queryOp;
  }
  
  /**
   * Get the key relating to the event.
   * @return Object key. 
   */
  public Object getKey() {
    return this.key;
  }

  /** 
   * Get the old value that was modified.
   * If there is no old value because this is an insert, then
   * return null. 
   */
  /*
  public Object getOldValue() {
    return this.oldValue;
  }
  */
  
  /** 
   * Get the new value of the modification.
   *  If there is no new value because this is a delete, then
   *  return null. 
   */
  public Object getNewValue() {
    if (this.newValue == null && this.delta != null) {
      throw new InvalidDeltaException();
    }
    return this.newValue;
  }
  
  /** 
   * If an error occurred, return the Throwable, otherwise return null.
   * If an error occurred, then this event will be passed to the
   * <code>onError</code> method of the CqListener instead of the
   * <code>onEvent</code> method.
   */
  public Throwable getThrowable() {
    return this.throwable;
  }
  
  public byte[] getDeltaValue() {
    return this.delta;
  }

  /**
   * Checks if the Cq Event is for an exception. If so constructs the
   * exception. 
   */
  public void setException() {
    // Needs to be changed.
    this.throwable = new Throwable(LocalizedStrings.CqEventImpl_EXCEPTION_OCCURED_WHILE_APPLYING_QUERY_ON_A_CACHE_EVENT.toLocalizedString());  
  }
  
  public void setException(String exceptionText) {
    this.throwable = new Throwable(exceptionText);  
  }
  
  public QueueManager getQueueManager() {
    return this.qManager;
  }
  
  public EventID getEventID() {
    return this.eventId;  
  }
  
  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CqEvent [").append(
     "CqName=").append(this.cQuery.getName()).append(
     "; base operation=").append(this.baseOp).append(
     "; cq operation=").append(this.queryOp).append(
     "; key=").append(this.key).append(
     "; value=").append(this.newValue).append(
     "]");
    return buffer.toString();
  }
}

