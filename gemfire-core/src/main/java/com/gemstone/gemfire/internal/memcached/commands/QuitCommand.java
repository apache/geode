/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached.commands;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.memcached.Reply;
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * This command is currently no-op
 * 
 * "quit" is a command with no arguments:
 * quit\r\n<br/><br/>
 * 
 * Upon receiving this command, the server closes the
 * connection. However, the client may also simply close the connection
 * when it no longer needs it, without issuing this command.
 * 
 * @author Swapnil Bawaskar
 *
 */
public class QuitCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return asciiCharset.encode(Reply.OK.toString());
    }
    request.getRequest().position(HEADER_LENGTH);
    return isQuiet() ? null : request.getResponse();
  }

  /**
   * Overridden by Q command
   */
  protected boolean isQuiet() {
    return false;
  }
}
