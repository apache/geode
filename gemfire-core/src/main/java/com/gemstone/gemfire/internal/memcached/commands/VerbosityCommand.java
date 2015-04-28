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
 * "verbosity" is a command with a numeric argument. It always succeeds,
 * and the server sends "OK\r\n" in response (unless "noreply" is given
 * as the last parameter). Its effect is to set the verbosity level of
 * the logging output.
 * 
 * 
 * @author Swapnil Bawaskar
 *
 */
public class VerbosityCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return asciiCharset.encode(Reply.OK.toString());
    }
    return request.getResponse();
  }

}
