/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class TransportFilterSocket extends Socket {

  /* InputStream used by socket */
  private InputStream in;

  /* OutputStream used by socket */
  private OutputStream out;

  private List<GatewayTransportFilter> gatewayTransportFilters;

  public TransportFilterSocket(List<GatewayTransportFilter> transportFilters) {
    super();
    this.gatewayTransportFilters = transportFilters;
  }

  public TransportFilterSocket(List<GatewayTransportFilter> transportFilters,
      InetAddress host, int port) throws IOException {
    super(host, port);
    this.gatewayTransportFilters = transportFilters;
  }

  public InputStream getInputStream() throws IOException {
    if (in == null) {
      in = super.getInputStream();
      for (GatewayTransportFilter filter : this.gatewayTransportFilters) {
        in = filter.getInputStream(in);
      }
    }
    return in;
  }

  public OutputStream getOutputStream() throws IOException {
    if (out == null) {
      out = super.getOutputStream();
      for (GatewayTransportFilter filter : this.gatewayTransportFilters) {
        out = filter.getOutputStream(out);
      }
    }
    return out;
  }

  /*
   * Flush the OutputStream before closing the socket.
   */
  public synchronized void close() throws IOException {
    OutputStream o = getOutputStream();
    o.flush();
    super.close();
  }
}
