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

package com.gemstone.gemfire.internal.cache.tier;

import java.io.IOException;

import com.gemstone.gemfire.internal.Version;

/**
 * Defines the message listener/acceptor interface which is the GemFire Bridge
 * Server. Multiple communication stacks may provide implementations for the
 * interfaces defined in this package
 *
 * @since GemFire 2.0.2
 */
public abstract class Acceptor
{
  /**
   * Byte meaning that the Socket is being used for 'client to server'
   * communication.
   */
  public static final byte CLIENT_TO_SERVER = (byte)100;

  /**
   * Byte meaning that the Socket is being used for 'primary server to client'
   * communication.
   */
  public static final byte PRIMARY_SERVER_TO_CLIENT = (byte)101;

  /**
   * Byte meaning that the Socket is being used for 'seconadry server to client'
   * communication.
   */
  public static final byte SECONDARY_SERVER_TO_CLIENT = (byte)102;

  /**
   * Byte meaning that the Socket is being used for 'gateway to gateway'
   * communication.
   */
  public static final byte GATEWAY_TO_GATEWAY = (byte)103;

  /**
   * Byte meaning that the Socket is being used for 'monitor to gateway'
   * communication.
   */
  public static final byte MONITOR_TO_SERVER = (byte)104;

  /**
   * Byte meaning that the connection between the server and client was
   * successful.
   */
  public static final byte SUCCESSFUL_SERVER_TO_CLIENT = (byte)105;

  /**
   * Byte meaning that the connection between the server and client was
   * unsuccessful.
   */
  public static final byte UNSUCCESSFUL_SERVER_TO_CLIENT = (byte)106;
  
  /**
   * Byte meaning that the Socket is being used for 'client to server'
   * messages related to a client queue (register interest, create cq, etc.).
   */
  public static final byte CLIENT_TO_SERVER_FOR_QUEUE = (byte)107;

  /**
   * The GFE version of the server.
   * @since GemFire 5.7
   */
  public static final Version VERSION = Version.CURRENT.getGemFireVersion();

  /**
   * Listens for a client to connect and establishes a connection to that
   * client.
   */
  public abstract void accept() throws Exception;

  /**
   * Starts this acceptor thread
   */
  public abstract void start() throws IOException;

  /**
   * Returns the port on which this acceptor listens for connections from
   * clients.
   */
  public abstract int getPort();

  /**
   * Closes this acceptor thread
   */
  public abstract void close();

  /**
   * Is this acceptor running (handling connections)?
   */
  public abstract boolean isRunning();
  
}
