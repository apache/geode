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

package com.gemstone.gemfire.cache.asyncqueue;

import java.util.List;

import com.gemstone.gemfire.cache.CacheCallback;

/**
 * A callback for events passing through the <code>AsyncEventQueue</code> to which this
 * listener is attached. Implementers of interface <code>AsyncEventListener</code> process
 * batches of <code>AsyncEvent</code> delivered by the corresponding <code>AsyncEventQueue</code>.
 * <br>
 * A sample implementation of this interface is as follows: <br>
 * 
 * <pre>
 * public class MyEventListener implements AsyncEventListener {
 *      
 *      public boolean processEvents(List<AsyncEvent> events) {
 *          for (Iterator i = events.iterator(); i.hasNext();) {
 *              AsyncEvent event = (AsyncEvent)i.next();
 *              
 *              String originalRegionName = event.getRegion().getName();
 *              //For illustration purpose, use the event to update the duplicate of above region.
 *              final Region duplicateRegion = CacheHelper.getCache().getRegion(originalRegionName + "_DUP");
 *               
 *              final Object key = event.getKey();
 *              final Object value = event.getDeserializedValue();
 *              final Operation op = event.getOperation();
 *              
 *              if (op.isCreate()) {
 *                  duplicateRegion.create(key, value);
 *              } else if (op.isUpdate()) {
 *                  duplicateRegion.put(key, value);
 *              } else if (op.isDestroy()) {
 *                  duplicateRegion.destroy(key);
 *              }
 *              
 *          }
 *      }
 * }
 * </pre>
 * 
 * @since GemFire 7.0
 */
public interface AsyncEventListener extends CacheCallback {

  /**
   * Process the list of <code>AsyncEvent</code>s. This method will
   * asynchronously be called when events are queued to be processed.
   * The size of the list will be up to batch size events where batch
   * size is defined in the <code>AsyncEventQueueFactory</code>.
   *
   * @param events The list of <code>AsyncEvent</code> to process
   *
   * @return boolean    True represents whether the events were successfully processed,
   *                    false otherwise.
   */
  public boolean processEvents(List<AsyncEvent> events);
}
