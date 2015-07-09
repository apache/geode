/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.util.GatewayEvent;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class MyGatewayEventFilter2 implements GatewayEventFilter, Declarable2{


  public void close() {
    
  }

  public Properties getConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.wan.GatewayEventFilter#afterAcknowledgement(com.gemstone.gemfire.cache.util.GatewayEvent)
   */
  public void afterAcknowledgement(GatewayEvent event) {
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