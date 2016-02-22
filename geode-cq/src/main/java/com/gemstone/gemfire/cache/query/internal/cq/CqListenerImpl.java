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

import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * CqListener class, implements CqListener interface methods. 
 * Offers callback methods for the CqQuery.
 *
 * @author anil 
 * @since 5.5
 */
public class CqListenerImpl implements CqListener {
  
  /**
   * An event occurred that modifies the results of the query.
   * This event does not contain an error.
   */
  public void onEvent(CqEvent aCqEvent) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqListenerImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  }
  
  /** 
   * An error occurred in the processing of a CQ.
   * This event does contain an error. The newValue and oldValue in the
   * may or may not be available, and will be null if not available.
   */
  public void onError(CqEvent aCqEvent) {
    if(true) {
      throw new IllegalStateException(LocalizedStrings.CqListenerImpl_NOT_YET_SUPPORTED.toLocalizedString());
    }
  }
  
  /** CQ is being closed, do any cleanup here */
  public void close() {
  }
}
