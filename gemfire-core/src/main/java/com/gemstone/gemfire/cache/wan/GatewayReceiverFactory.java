/*
 * =========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.cache.wan;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 7.0
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
   * @since 8.1
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
