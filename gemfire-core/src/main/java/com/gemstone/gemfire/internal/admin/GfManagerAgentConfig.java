/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.DisconnectListener;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * Used to create and configure a {@link GfManagerAgent}.
 */
public class GfManagerAgentConfig {

  /** 
   * Constructs a GfManagerAgentConfig given the transport it should
   * use to connect to the remote systems and the LogWriterI18n to use for
   * logging messages.
   */
  // LOG: saves LogWriterLogger from AdminDistributedSystemImpl for RemoteGfManagerAgentConfig
  public GfManagerAgentConfig(String displayName, TransportConfig transport,
                              InternalLogWriter logWriter, int level,
                              AlertListener listener,DisconnectListener  disconnectListener) {
    this.displayName = displayName;
    this.transport = transport;
    this.logWriter = logWriter;
    this.alertLevel = level;
    this.alertListener = listener;
    this.disconnectListener = disconnectListener;
  }

  /**
   * Returns the communication transport configuration.
   */
  public TransportConfig getTransport() {
    return this.transport;
  }
  /**
   * Returns the log writer
   */
  // LOG: get LogWriter from the AdminDistributedSystemImpl -- used by RemoteGfManagerAgent for AuthenticationFailedException
  public InternalLogWriter getLogWriter(){
    return logWriter;
  }
  /**
   * Returns the alert level
   */
  public int getAlertLevel() {
    return this.alertLevel;
  }
  /**
   * Returns the alert listener
   */
  public AlertListener getAlertListener() {
    return this.alertListener;
  }
  /**
   * Returns the display name
   */
  public String getDisplayName() {
    return this.displayName;
  }
  
  public DisconnectListener getDisconnectListener() {
	return disconnectListener;
  }
  
  private TransportConfig transport;
  private InternalLogWriter logWriter;
  private int alertLevel;
  private AlertListener alertListener;
  private String displayName;
  private DisconnectListener  disconnectListener;
}
