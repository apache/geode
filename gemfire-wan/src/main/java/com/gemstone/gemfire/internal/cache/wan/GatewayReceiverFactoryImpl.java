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
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.GatewayReceiverCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 7.0
 */
public class GatewayReceiverFactoryImpl implements GatewayReceiverFactory {

  private int startPort = GatewayReceiver.DEFAULT_START_PORT;
  
  private int endPort = GatewayReceiver.DEFAULT_END_PORT;
  
  private int timeBetPings = GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;

  private int socketBuffSize = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;

  private String bindAdd= GatewayReceiver.DEFAULT_BIND_ADDRESS; 
  
  private String hostnameForSenders = GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS;  
  
  private boolean manualStart = GatewayReceiver.DEFAULT_MANUAL_START;

  private List<GatewayTransportFilter> filters = new ArrayList<GatewayTransportFilter>();
  
  private Cache cache;

  public GatewayReceiverFactoryImpl() {
    
  }
  public GatewayReceiverFactoryImpl(Cache cache) {
   this.cache = cache;
  }
  
  public GatewayReceiverFactory addGatewayTransportFilter(
      GatewayTransportFilter filter) {
    this.filters.add(filter);
    return this;
  }

  public GatewayReceiverFactory removeGatewayTransportFilter(
      GatewayTransportFilter filter) {
    this.filters.remove(filter);
    return this;
  }

  public GatewayReceiverFactory setMaximumTimeBetweenPings(int time) {
    this.timeBetPings = time;
    return this;
  }

  public GatewayReceiverFactory setStartPort(int port) {
    this.startPort = port;
    return this;
  }
  
  public GatewayReceiverFactory setEndPort(int port) {
    this.endPort = port;
    return this;
  }
  
  public GatewayReceiverFactory setSocketBufferSize(int size) {
    this.socketBuffSize = size;
    return this;
  }

  public GatewayReceiverFactory setBindAddress(String address) {
    this.bindAdd = address;
    return this;
  }
  
  public GatewayReceiverFactory setHostnameForSenders(String address) {
    this.hostnameForSenders = address;
    return this;
  } 

  public GatewayReceiverFactory setManualStart(boolean start) {
    this.manualStart = start;
    return this;
  }
  
  public GatewayReceiver create() {
    if (this.startPort > this.endPort) {
      throw new IllegalStateException(
          "Please specify either start port a value which is less than end port.");
    }
    GatewayReceiver recv = null;
    if (this.cache instanceof GemFireCacheImpl) {
      recv = new GatewayReceiverImpl(this.cache, this.startPort, this.endPort,
          this.timeBetPings, this.socketBuffSize, this.bindAdd, this.filters,
          this.hostnameForSenders, this.manualStart);
      ((GemFireCacheImpl)cache).addGatewayReceiver(recv);
      InternalDistributedSystem system = (InternalDistributedSystem) this.cache
      .getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_CREATE, recv);
      if (!this.manualStart) {
        try {
          recv.start();
        }
        catch (IOException ioe) {
          throw new GatewayReceiverException(
              LocalizedStrings.GatewayReceiver_EXCEPTION_WHILE_STARTING_GATEWAY_RECEIVER
                  .toLocalizedString(), ioe);
        }
      }
    } else if (this.cache instanceof CacheCreation) {
      recv = new GatewayReceiverCreation(this.cache, this.startPort, this.endPort,
          this.timeBetPings, this.socketBuffSize, this.bindAdd, this.filters,
          this.hostnameForSenders, this.manualStart);
      ((CacheCreation)cache).addGatewayReceiver(recv);
    }
    return recv;
  }

}
