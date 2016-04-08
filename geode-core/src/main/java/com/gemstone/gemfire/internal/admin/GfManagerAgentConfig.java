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
