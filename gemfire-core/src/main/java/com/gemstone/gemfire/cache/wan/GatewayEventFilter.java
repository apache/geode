/*
 * =========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.cache.wan;

import com.gemstone.gemfire.cache.CacheCallback;

/**
 * Callback for users to filter out events before dispatching to remote
 * distributed system
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 7.0
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
