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

package org.apache.geode.internal.cache.tier;

import java.io.IOException;

import org.apache.geode.internal.Version;

/**
 * Defines the message listener/acceptor interface which is the GemFire Bridge Server. Multiple
 * communication stacks may provide implementations for the interfaces defined in this package
 *
 * @since GemFire 2.0.2
 */
public abstract class Acceptor {

  /**
   * The GFE version of the server.
   * 
   * @since GemFire 5.7
   */
  public static final Version VERSION = Version.CURRENT.getGemFireVersion();

  /**
   * Listens for a client to connect and establishes a connection to that client.
   */
  public abstract void accept() throws Exception;

  /**
   * Starts this acceptor thread
   */
  public abstract void start() throws IOException;

  /**
   * Returns the port on which this acceptor listens for connections from clients.
   */
  public abstract int getPort();

  /**
   * returns the server's name string, including the inet address and port that the server is
   * listening on
   */
  public abstract String getServerName();

  /**
   * Closes this acceptor thread
   */
  public abstract void close();

  /**
   * Is this acceptor running (handling connections)?
   */
  public abstract boolean isRunning();

}
