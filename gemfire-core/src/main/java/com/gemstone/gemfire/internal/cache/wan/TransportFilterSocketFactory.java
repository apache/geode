/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.ClientSocketFactory;

public class TransportFilterSocketFactory implements ClientSocketFactory{

  private List<GatewayTransportFilter> gatewayTransportFilters;
  
  public TransportFilterSocketFactory setGatewayTransportFilters(List<GatewayTransportFilter> transportFilters){
    this.gatewayTransportFilters = transportFilters;
    return this;
  }
  
  public Socket createSocket(InetAddress address, int port) throws IOException {
    return new TransportFilterSocket(gatewayTransportFilters, address, port);
  }

}
