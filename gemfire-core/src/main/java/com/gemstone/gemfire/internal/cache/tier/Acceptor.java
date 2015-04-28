/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier;

import java.io.IOException;

import com.gemstone.gemfire.internal.Version;

/**
 * Defines the message listener/acceptor interface which is the GemFire Bridge
 * Server. Multiple communication stacks may provide implementations for the
 * interfaces defined in this package
 *
 * @author Sudhir Menon
 * @since 2.0.2
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
   * @since 5.7
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
