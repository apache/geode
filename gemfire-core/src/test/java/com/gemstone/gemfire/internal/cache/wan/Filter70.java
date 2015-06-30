/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.Serializable;

import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;

public class Filter70 implements GatewayEventFilter, Serializable {
  String Id = "Filter70";
  public int eventEnqued = 0;

  public int eventTransmitted = 0;

  public boolean beforeEnqueue(GatewayQueueEvent event) {
    if ((Long)event.getKey() >= 0 && (Long)event.getKey() < 500) {
      return false;
    }
    return true;
  }

  public boolean beforeTransmit(GatewayQueueEvent event) {
    eventEnqued++;
    return true;
  }

  public void close() {

  }

  @Override
  public String toString() {
    return Id;
  }

  public void afterAcknowledgement(GatewayQueueEvent event) {
  }
  
  @Override
  public boolean equals(Object obj){
    if(this == obj){
      return true;
    }
    if ( !(obj instanceof Filter70) ) return false;
    Filter70 filter = (Filter70)obj;
    return this.Id.equals(filter.Id);
  }
}