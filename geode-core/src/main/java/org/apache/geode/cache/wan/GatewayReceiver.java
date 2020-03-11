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
package org.apache.geode.cache.wan;

import java.io.IOException;
import java.util.List;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.wan.GatewayReceiverException;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A GatewayReceiver that receives the events from a {@code GatewaySender}. GatewayReceiver is
 * used in conjunction with a {@link GatewaySender} to connect two distributed-systems. This
 * GatewayReceiver will receive all the events originating in distributed-systems that has a
 * {@code GatewaySender} connected to this distributed-system.
 */
public interface GatewayReceiver {

  String RECEIVER_GROUP = "__recv__group";
  /**
   * The default maximum amount of time between client pings. This value is used by the
   * {@code ClientHealthMonitor} to determine the health of this {@code GatewayReceiver}'s
   * clients.
   */
  int DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS = 60000;

  /**
   * Default start value of the port range from which the {@code GatewayReceiver}'s port will
   * be chosen
   */
  int DEFAULT_START_PORT = 5000;

  /**
   * Default end value of the port range from which the {@code GatewayReceiver}'s port will be
   * chosen
   */
  int DEFAULT_END_PORT = 5500;

  /**
   * The default buffer size for socket buffers for the {@code GatewayReceiver}.
   */
  int DEFAULT_SOCKET_BUFFER_SIZE = 524288;

  /**
   * The default ip address or host name that the receiver's socket will listen on for client
   * connections. The current default is an empty string.
   */
  String DEFAULT_BIND_ADDRESS = "";

  String DEFAULT_HOSTNAME_FOR_SENDERS = "";

  /**
   * The default value for manually starting a {@code GatewayReceiver}.
   *
   * @since GemFire 8.1
   */
  boolean DEFAULT_MANUAL_START = false;

  /**
   * If the batch already seen by this receiver, arrives again then whether it is to be re-applied
   * or not is decided by this attribute.
   */
  boolean APPLY_RETRIES =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "GatewayReceiver.ApplyRetries");

  /**
   * Starts this receiver. Once the receiver is running, its configuration cannot be changed.
   *
   * @throws IOException If an error occurs while starting the receiver
   */
  void start() throws IOException;

  /**
   * Stops this receiver. Note that the {@code GatewayReceiver} can be reconfigured and
   * restarted if desired.
   */
  void stop();

  /**
   * Destroys this {@code GatewayReceiver}and removes the {@code GatewayReceiverMBean}
   * associated with this {@code GatewayReceiver}. This method does not remove
   * the {@code GatewayReceiver} from cluster configuration.
   * The {@link #stop() stop} method should be called before calling destroy}
   *
   *
   * @throws GatewayReceiverException if {@code GatewayReceiver} has not been stopped before
   *         calling destroy
   */
  void destroy();

  /**
   * Returns whether or not this receiver is running
   */
  boolean isRunning();

  /**
   * Returns the list of {@code GatewayTransportFilter} added to this GatewayReceiver.
   *
   * @return the list of {@code GatewayTransportFilter} added to this GatewayReceiver.
   */
  List<GatewayTransportFilter> getGatewayTransportFilters();

  /**
   * Returns the maximum amount of time between client pings. This value is used by the
   * {@code ClientHealthMonitor} to determine the health of this {@code GatewayReceiver}'s
   * clients (i.e. the GatewaySenders). The default is 60000 ms.
   *
   * @return the maximum amount of time between client pings.
   */
  int getMaximumTimeBetweenPings();

  /**
   * Returns the port on which this {@code GatewayReceiver} listens for clients.
   */
  int getPort();

  /**
   * Returns start value of the port range from which the {@code GatewayReceiver}'s port will
   * be chosen.
   */
  int getStartPort();

  /**
   * Returns end value of the port range from which the {@code GatewayReceiver}'s port will be
   * chosen.
   */
  int getEndPort();

  /**
   * Returns a string representing the ip address or host name that server locators will tell
   * clients ({@code GatewaySender}s in this case) that this receiver is listening on.
   *
   * @return the ip address or host name to give to clients so they can connect to this receiver
   */
  String getHost();

  /**
   * Returns the hostname configured by {@link GatewayReceiverFactory#setHostnameForSenders(String)}
   */
  String getHostnameForSenders();

  /**
   * Returns the configured buffer size of the socket connection for this
   * {@code GatewayReceiver}. The default is 524288 bytes.
   *
   * @return the configured buffer size of the socket connection for this
   *         {@code GatewayReceiver}
   */
  int getSocketBufferSize();

  /**
   * Returns a string representing the ip address or host name that this server will listen on.
   *
   * @return the ip address or host name that this server is to listen on
   * @see #DEFAULT_BIND_ADDRESS
   */
  String getBindAddress();

  /**
   * Returns the manual start boolean property for this GatewayReceiver. Default is true i.e. the
   * GatewayReceiver will not automatically start once created.
   *
   * @return the manual start boolean property for this GatewayReceiver
   *
   */
  boolean isManualStart();

  /**
   * Return the underlying Cacheserver
   */
  CacheServer getServer();
}
