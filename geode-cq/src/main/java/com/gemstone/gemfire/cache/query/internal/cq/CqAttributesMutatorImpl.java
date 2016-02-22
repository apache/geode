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

import com.gemstone.gemfire.cache.query.CqAttributesMutator;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Supports modification of certain cq attributes after the cq has been
 * created. 
 * The setter methods all return the previous value of the attribute. 
 *
 * @author anil 
 * @since 5.5
 */
public class CqAttributesMutatorImpl implements CqAttributesMutator {
  
  /**
   * Adds a cq listener to the end of the list of cq listeners on this CqQuery.
   * @param aListener the user defined cq listener to add to the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   */
  public void addCqListener(CqListener aListener) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqAttributesMutatorImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  }
  
  /**
   * Removes a cq listener from the list of cq listeners on this CqQuery.
   * Does nothing if the specified listener has not been added.
   * If the specified listener has been added then {@link com.gemstone.gemfire.cache.CacheCallback#close} will
   * be called on it; otherwise does nothing.
   * @param aListener the cq listener to remove from the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   */
  public void removeCqListener(CqListener aListener) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqAttributesMutatorImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  }
  
  /**
   * Removes all cq listeners, calling on each of them, and then adds each listener in the specified array.
   * @param newListeners a possibly null or empty array of listeners to add to this CqQuery.
   * @throws IllegalArgumentException if the <code>newListeners</code> array has a null element
   */
  public void initCqListeners(CqListener[] newListeners) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqAttributesMutatorImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  } 
}
