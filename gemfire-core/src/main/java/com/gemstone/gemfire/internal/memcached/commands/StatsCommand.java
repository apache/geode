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
 * This command is currently no-op.<br/>
 * 
 * The command "stats" is used to query the server about statistics it
 * maintains and other internal data. It has two forms. Without
 * arguments:
 * 
 * stats\r\n
 * it causes the server to output general-purpose statistics
 * 
 * @author Swapnil Bawaskar
 *
 */
public class StatsCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return asciiCharset.encode(Reply.END.toString());
    }
    request.getRequest().position(HEADER_LENGTH);
    return request.getResponse();
  }

}
