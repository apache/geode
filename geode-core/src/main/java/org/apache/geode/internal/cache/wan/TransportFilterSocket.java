/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.wan;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

import org.apache.geode.cache.wan.GatewayTransportFilter;

public class TransportFilterSocket extends Socket {

  /* InputStream used by socket */
  private InputStream in;

  /* OutputStream used by socket */
  private OutputStream out;

  private final List<GatewayTransportFilter> gatewayTransportFilters;

  public TransportFilterSocket(List<GatewayTransportFilter> transportFilters) {
    super();
    gatewayTransportFilters = transportFilters;
  }

  public TransportFilterSocket(List<GatewayTransportFilter> transportFilters, InetAddress host,
      int port) throws IOException {
    super(host, port);
    gatewayTransportFilters = transportFilters;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    if (in == null) {
      in = super.getInputStream();
      for (GatewayTransportFilter filter : gatewayTransportFilters) {
        in = filter.getInputStream(in);
      }
    }
    return in;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    if (out == null) {
      out = super.getOutputStream();
      for (GatewayTransportFilter filter : gatewayTransportFilters) {
        out = filter.getOutputStream(out);
      }
    }
    return out;
  }

  /*
   * Flush the OutputStream before closing the socket.
   */
  @Override
  public synchronized void close() throws IOException {
    OutputStream o = getOutputStream();
    o.flush();
    super.close();
  }
}
