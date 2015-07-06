/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.internal.util;

import java.io.IOException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;


/**
 * 
 * @author rishim
 *
 */
public class ConnectionUtil {

  
  
  public static SocketFactory getSocketFactory(boolean usessl)
    throws IOException
  {
    if(usessl){
      return (SSLSocketFactory)SSLSocketFactory.getDefault();
    }else{
      return SocketFactory.getDefault();
    }    
  }
  
  
}
