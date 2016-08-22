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
package com.gemstone.gemfire.cache.wan;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import java.io.IOException;
import java.util.List;

/**
 * A GatewayReceiver that receives the events from a <code>GatewaySender</code>.
 * GatewayReceiver is used in conjunction with a {@link GatewaySender} to
 * connect two distributed-systems. This GatewayReceiver will receive all the
 * events originating in distributed-systems that has a
 * <code>GatewaySender<code> connected to this distributed-system.
 * 
 * 
 */
public interface GatewayReceiver {

  public static final String RECEIVER_GROUP = "__recv__group";
  /**
   * The default maximum amount of time between client pings. This value
   * is used by the <code>ClientHealthMonitor</code> to determine the
   * health of this <code>GatewayReceiver</code>'s clients.
   */
  public static final  int DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS  = 60000;

  /**
   * Default start value of the port range from which the <code>GatewayReceiver</code>'s port will be chosen
   */
  public static final int DEFAULT_START_PORT = 5000;
  
  /**
   * Default end value of the port range from which the <code>GatewayReceiver</code>'s port will be chosen
   */
  public static final int DEFAULT_END_PORT = 5500;
  
  /**
   * The default buffer size for socket buffers for the <code>GatewayReceiver</code>.
   */
  public static final int DEFAULT_SOCKET_BUFFER_SIZE = 524288;
  
  /**
   * The default ip address or host name that the receiver's socket will
   * listen on for client connections.
   * The current default is an empty string.
   */
  public static final String DEFAULT_BIND_ADDRESS = ""; 
  
  public static final String DEFAULT_HOSTNAME_FOR_SENDERS = "";
  
  /**
   * The default value (true) for manually starting a
   * <code>GatewayReceiver</code>.
   * @since GemFire 8.1
   */
  public static final boolean DEFAULT_MANUAL_START = false;
  
  /**
   * If the batch already seen by this receiver, arrives again then whether it is to be
   * re-applied or not is decided by this attribute.  
   */
  public static final boolean APPLY_RETRIES = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "GatewayReceiver.ApplyRetries");

  /**
   * Starts this receiver.  Once the receiver is running, its
   * configuration cannot be changed.
   * 
   * @throws IOException
   *         If an error occurs while starting the receiver
   */
  public void start() throws IOException;
  
  /**
   * Stops this receiver. Note that the
   * <code>GatewayReceiver</code> can be reconfigured and restarted if
   * desired.
   */
  public void stop();

  /**
   * Returns whether or not this receiver is running
   */
  public boolean isRunning();
  
  /**
   * Returns the list of <code>GatewayTransportFilter</code> added to this
   * GatewayReceiver.
   * 
   * @return the list of <code>GatewayTransportFilter</code> added to this
   *         GatewayReceiver.
   */
  public List<GatewayTransportFilter> getGatewayTransportFilters();

  /**
   * Returns the maximum amount of time between client pings. This value is
   * used by the <code>ClientHealthMonitor</code> to determine the health
   * of this <code>GatewayReceiver</code>'s clients (i.e. the GatewaySenders). 
   * The default is 60000 ms.
   * @return the maximum amount of time between client pings.
   */
  public int getMaximumTimeBetweenPings();

  /**
   * Returns the port on which this <code>GatewayReceiver</code> listens for clients.
   */
  public int getPort();
  
  /**
   * Returns start value of the port range from which the <code>GatewayReceiver</code>'s port will be chosen.
   */
  public int getStartPort();
  
  /**
   * Returns end value of the port range from which the <code>GatewayReceiver</code>'s port will be chosen.
   */
  public int getEndPort();
  
  /**
   * Returns a string representing the ip address or host name that server locators
   * will tell clients (<code>GatewaySender</code>s in this case) that this receiver is listening on.
   * @return the ip address or host name to give to clients so they can connect
   *         to this receiver
   */
  public String getHost();

  /**
   * Returns the configured buffer size of the socket connection for this
   * <code>GatewayReceiver</code>. The default is 524288 bytes.
   * @return the configured buffer size of the socket connection for this
   * <code>GatewayReceiver</code>
   */
  public int getSocketBufferSize();

  /**
   * Returns a string representing the ip address or host name that this server
   * will listen on.
   * @return the ip address or host name that this server is to listen on
   * @see #DEFAULT_BIND_ADDRESS
   */
  public String getBindAddress();
  
  /**
   * Returns the manual start boolean property for this GatewayReceiver.
   * Default is true i.e. the GatewayReceiver will not automatically start once created.
   * 
   * @return the manual start boolean property for this GatewayReceiver
   * 
   */
  public boolean isManualStart();
  
  /**
   * Return the underlying Cacheserver 
   */
  public CacheServer getServer();
  
}
