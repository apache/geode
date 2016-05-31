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
package com.gemstone.gemfire.cache.wan;

import com.gemstone.gemfire.cache.CacheCallback;

/**
 * Callback for users to filter out events before dispatching to remote
 * distributed system
 * 
 * 
 * @since GemFire 7.0
 */
public interface GatewayEventFilter extends CacheCallback {

  /**
   * It will be invoked before enqueuing event into GatewaySender's queue. <br>
   * This callback is synchronous with the thread which is enqueuing the event
   * into GatewaySender's queue.
   * 
   * @param event
   * @return true if event should be enqueued otherwise return false.
   */
  public boolean beforeEnqueue(GatewayQueueEvent event);

  /**
   * It will be invoked before dispatching event to remote GatewayReceiver <br>
   * This callback is asynchronous with the thread which is enqueuing the event
   * into GatewaySender's queue.<br>
   * This callback will always be called from the thread which is dispatching
   * events to remote distributed systems
   * 
   * @param event
   * @return true if event should be dispatched otherwise return false.
   */
  public boolean beforeTransmit(GatewayQueueEvent event);

  /**
   * It will be invoked once GatewaySender receives an ack from remote
   * GatewayReceiver <br>
   * This callback will always be called from the thread which is dispatching
   * events to remote distributed systems
   * 
   * @param event
   */
  public void afterAcknowledgement(GatewayQueueEvent event);

}
