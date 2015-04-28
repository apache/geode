/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached.commands;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.memcached.Reply;
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.internal.memcached.ResponseStatus;
import com.gemstone.gemfire.internal.memcached.ValueWrapper;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

public class FlushAllCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return processAsciiCommand(request.getRequest(), cache);
    }
    return processBinaryCommand(request, cache);
  }

  private ByteBuffer processAsciiCommand(ByteBuffer buffer, Cache cache) {
    CharBuffer flb = getFirstLineBuffer();
    getAsciiDecoder().reset();
    getAsciiDecoder().decode(buffer, flb, false);
    flb.flip();
    String firstLine = getFirstLine();
    String[] firstLineElements = firstLine.split(" ");
    
    assert "flush_all".equals(stripNewline(firstLineElements[0]));
    boolean noReply = false;
    int delay = 0;
    if (firstLineElements.length == 2) {
      if ("noreply".equals(stripNewline(firstLineElements[1]))) {
        noReply = true;
      } else {
        delay = Integer.parseInt(stripNewline(firstLineElements[1]));
      }
    } else if (firstLineElements.length == 3) {
      delay = Integer.parseInt(stripNewline(firstLineElements[1]));
      noReply = true;
    }
    
    final Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    if (delay == 0) {
      r.destroyRegion();
    } else {
      StorageCommand.getExpiryExecutor().schedule(new Runnable() {
        public void run() {
          r.destroyRegion();
        }
      }, delay, TimeUnit.SECONDS);
    }
    
    CharBuffer retVal = CharBuffer.wrap(Reply.OK.toString());
    
    return noReply ? null : asciiCharset.encode(retVal);
  }
  
  private ByteBuffer processBinaryCommand(RequestReader request, Cache cache) {
    ByteBuffer buffer = request.getRequest();
    final Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    
    int delay = 0;
    int extraLength = buffer.get(EXTRAS_LENGTH_INDEX);
    buffer.position(HEADER_LENGTH);
    if (extraLength != 0) {
      delay = buffer.getInt();
    }
    
    if (getLogger().fineEnabled()) {
      cache.getLogger().fine("flush:delay:"+delay);
    }
    
    if (delay == 0) {
      try {
        r.destroyRegion();
      } catch (Exception e) {
        return handleBinaryException("", request, request.getResponse(), "flushall", e);
      }
    } else {
      StorageCommand.getExpiryExecutor().schedule(new Runnable() {
        @Override
        public void run() {
          r.destroyRegion();
        }
      }, delay, TimeUnit.SECONDS);
    }
    ByteBuffer response = request.getResponse();
    response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
    return isQuiet() ? null : response;
  }
  
  /**
   * Overridden by Q command
   */
  protected boolean isQuiet() {
    return false;
  }
}
