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

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.Operation;


/**
 * This interface provides methods to get all the information sent from the server 
 * about the CQ event.  
 * The CqEvent is passed to the CQs CqListener methods. It can be used to retrieve 
 * such information as the region operation, CQ operation associated with the event, 
 * the new key and value from the event, and the CqQuery object associated with the 
 * event. 
 * The CqEvent is not an extension of CacheEvent. 
 * 
 * @since GemFire 5.5
 */
public interface CqEvent {
  
  /**
   * Get the CqQuery object of this event.
   * @see CqQuery
   * @return CqQuery object.
   */
  public CqQuery getCq();
  
  /** 
   * Get the operation on the base region that triggered this event.
   * @return Operation operation on the base region (on which CQ is created). 
   */
  public Operation getBaseOperation();
  
  /** 
   * Get the operation on the query results. Supported operations 
   * include update, create, destroy, region clear and region invalidate.
   * @return Operation operation with respect to CQ.
   */
  public Operation getQueryOperation();
  
  /**
   * Get the key relating to the event.
   * In case of REGION_CLEAR and REGION_INVALIDATE operation, the key will be null.
   * @return Object key. 
   */
  public Object getKey();
   
  /**
   * Get the new value of the modification.
   * If there is no new value returns null, this will happen during delete 
   * operation.
   * May throw <code>InvalidDeltaException</code>, if value is null and Delta
   * Propagation is enabled.
   * 
   * @return Object new/modified value.
   */
  public Object getNewValue();
  
  /** 
   * If an error occurred, return the Throwable, otherwise return null.
   * If an error occurred, then this event will be passed to the
   * <code>onError</code> method of the CqListener instead of the
   * <code>onEvent</code> method.
   * @return Throwable exception related to error.
   */
  public Throwable getThrowable();

  /**
   * Get the delta modification.
   * If there is no delta, returns null. New value may still be available.
   * 
   * @return byte[] delta value.
   * @since GemFire 6.5
   */
  public byte[] getDeltaValue();
}

