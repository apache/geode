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

package org.apache.geode.cache.wan;

/**
 * 
 * @since GemFire 7.0
 */
public interface GatewayReceiverFactory {
  /**
   * Sets the start port for the <code>GatewayReceiver</code>.
   * If set the GatewayReceiver will start at one of the port between startPort
   * and endPort.
   * The default startPort 50505.
   * @param startPort
   */
  public GatewayReceiverFactory setStartPort(int startPort);

  /**
   * Sets the end port for the GatewayReceiver.
   * If set the GatewayReceiver will start at one of the port between startPort
   * and endPort.
   * The default endPort 50605.
   * @param endPort
   */
  public GatewayReceiverFactory setEndPort(int endPort);
  
  /**
   * Sets the buffer size in bytes of the socket connection for this
   * <code>GatewayReceiver</code>. The default is 32768 bytes.
   * 
   * @param socketBufferSize
   *          The size in bytes of the socket buffer
   */
  public GatewayReceiverFactory setSocketBufferSize(int socketBufferSize);

  /**
   * Sets the ip address or host name that this <code>GatewayReceiver</code> is to listen on
   * for GatewaySender Connection
   * 
   * @param address
   *          String representing ip address or host name
   */
  public GatewayReceiverFactory setBindAddress(String address);

  /**
   * Adds a <code>GatewayTransportFilter</code>
   * 
   * @param filter
   *          GatewayTransportFilter
   */
  public GatewayReceiverFactory addGatewayTransportFilter(
      GatewayTransportFilter filter);

  /**
   * Removes a <code>GatewayTransportFilter</code>
   * 
   * @param filter
   *          GatewayTransportFilter
   */
  public GatewayReceiverFactory removeGatewayTransportFilter(
      GatewayTransportFilter filter);

  /**
   * Sets the maximum amount of time between client pings.The default is 60000
   * ms.
   * 
   * @param time
   *          The maximum amount of time between client pings
   */
  public GatewayReceiverFactory setMaximumTimeBetweenPings(int time);
  
  /**
   * Sets the ip address or host name that server locators will tell
   * GatewaySenders that this GatewayReceiver is listening on.
   * 
   * @param address
   *          String representing ip address or host name
   */
  public GatewayReceiverFactory setHostnameForSenders(String address);
  
  /**
   * Sets the manual start boolean property for this
   * <code>GatewayReceiver</code>. 
   * @since GemFire 8.1
   * Default is true i.e. the <code>GatewayReceiver</code> will not start automatically once created.
   * Ideal default value should be false to match with GatewaySender
   * counterpart. But to not to break the existing functionality default value
   * is set to true. For next major releases, default value will be changed to
   * false.
   * 
   * @param start
   *          the manual start boolean property for this
   *          <code>GatewayReceiver</code>
   */
  public GatewayReceiverFactory setManualStart(boolean start);
  
  /**
   * Creates and returns an instance of <code>GatewayReceiver</code>
   * 
   * @return instance of GatewayReceiver
   */
  public GatewayReceiver create();

}
