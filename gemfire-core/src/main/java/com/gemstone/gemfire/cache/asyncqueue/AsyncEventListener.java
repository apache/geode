/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author pdeole
 * @since 7.0
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
