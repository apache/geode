/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
