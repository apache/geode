/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.tcp;

import java.io.IOException;
import java.util.List;

import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Base interface for {@link MsgStreamer} and {@link MsgStreamerList} to send a
 * message over a list of connections to one or more peers.
 * 
 * @author swale
 * @since 7.1
 */
public interface BaseMsgStreamer {

  public void reserveConnections(long startTime, long ackTimeout,
      long ackSDTimeout);

  /**
   * Returns a list of the Connections that the message was sent to. Call this
   * after {@link #writeMessage}.
   */
  public List<?> getSentConnections();

  /**
   * Returns an exception the describes which cons the message was not sent to.
   * Call this after {@link #writeMessage}.
   */
  public ConnectExceptions getConnectExceptions();

  /**
   * Writes the message to the connected streams and returns the number of bytes
   * written.
   * 
   * @throws IOException
   *           if serialization failure
   */
  public int writeMessage() throws IOException;

  /**
   * Close this streamer.
   * 
   * @throws IOException
   *           on exception
   */
  public void close() throws IOException;
}
