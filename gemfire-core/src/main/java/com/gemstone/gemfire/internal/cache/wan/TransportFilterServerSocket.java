/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class TransportFilterServerSocket extends ServerSocket {
  
  private List<GatewayTransportFilter> gatewayTransportFilters;
  
  public TransportFilterServerSocket(List<GatewayTransportFilter> transportFilters) throws IOException {
    super();
    this.gatewayTransportFilters = transportFilters;
  }

  public Socket accept() throws IOException {
    Socket s = new TransportFilterSocket(this.gatewayTransportFilters);
    implAccept(s);
    return s;
  }
}
