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
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class MyGatewayEventFilter1 implements GatewayEventFilter, Declarable2{
  
  private final Properties resolveProps;
  
  public MyGatewayEventFilter1() {
    this.resolveProps = new Properties();
  }

  public void close() {
    
  }

  public Properties getConfig() {
    return this.resolveProps;
  }

  public void init(Properties props) {
    this.resolveProps.putAll(props);
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
