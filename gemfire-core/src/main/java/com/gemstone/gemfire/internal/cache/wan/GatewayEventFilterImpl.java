/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;

public class GatewayEventFilterImpl implements GatewayEventFilter {

  public void close() {
    // TODO Auto-generated method stub

  }

  public boolean beforeEnqueue(GatewayQueueEvent event) {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean beforeTransmit(GatewayQueueEvent event) {
    // TODO Auto-generated method stub
    return false;
  }

  public void afterAcknowledgement(GatewayQueueEvent event) {
    // TODO Auto-generated method stub

  }

}
