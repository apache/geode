/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class MyGatewayTransportFilter1 implements GatewayTransportFilter, Serializable {

  String Id = "MyGatewayTransportFilter1";
  public InputStream getInputStream(InputStream stream) {
    // TODO Auto-generated method stub
    return null;
  }

  public OutputStream getOutputStream(OutputStream stream) {
    // TODO Auto-generated method stub
    return null;
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public String toString() {
    return Id;
  }
  
  @Override
  public boolean equals(Object obj){
    if(this == obj){
      return true;
    }
    if ( !(obj instanceof MyGatewayTransportFilter1) ) return false;
    MyGatewayTransportFilter1 filter = (MyGatewayTransportFilter1)obj;
    return this.Id.equals(filter.Id);
  }
}