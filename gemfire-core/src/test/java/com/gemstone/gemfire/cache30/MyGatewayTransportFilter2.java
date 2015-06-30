/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class MyGatewayTransportFilter2 implements GatewayTransportFilter, Declarable2, Serializable{


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

  public Properties getConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }


}
