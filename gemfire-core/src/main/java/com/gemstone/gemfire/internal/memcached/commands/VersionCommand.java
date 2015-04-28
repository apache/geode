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
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * Sends current version of gemcached server to the client
 * 
 * @author Swapnil Bawaskar
 *
 */
public class VersionCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    String version = GemFireMemcachedServer.version;
    if (protocol == Protocol.ASCII) {
      return asciiCharset.encode("VERSION "+version+"\r\n");
    }
    ByteBuffer response = request.getResponse(HEADER_LENGTH + version.length());
    response.putInt(TOTAL_BODY_LENGTH_INDEX, version.length());
    response.position(HEADER_LENGTH);
    response.put(asciiCharset.encode(version));
    response.rewind();
    request.getRequest().position(HEADER_LENGTH);
    return response;
  }

}
