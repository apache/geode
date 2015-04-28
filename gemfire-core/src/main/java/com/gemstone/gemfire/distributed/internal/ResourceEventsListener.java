/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

/**
 * Internal interface to be implemented to catch various resource events
 * 
 * @author rishim
 * 
 */
public interface ResourceEventsListener {

  /**
   * Handles various GFE resource life-cycle methods vis-a-vis Management and
   * Monitoring
   * 
   * @param event
   *          Resource events for which invocation has happened
   * @param resource
   *          the GFE resource type
   */
  void handleEvent(ResourceEvent event, Object resource);

}
