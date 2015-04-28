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
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.memcached.KeyWrapper;
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.internal.memcached.ResponseStatus;
import com.gemstone.gemfire.internal.memcached.ValueWrapper;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class TouchCommand extends AbstractCommand {

  private static final int EXTRAS_LENGTH = 4;

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol,
      Cache cache) {
    assert protocol == Protocol.BINARY;
    int newExpTime = 0;
    
    ByteBuffer buffer = request.getRequest();
    ByteBuffer response = null;
    int extrasLength = buffer.get(EXTRAS_LENGTH_INDEX);
    buffer.position(HEADER_LENGTH);

    if (extrasLength > 0) {
      assert extrasLength == 4;
      newExpTime = buffer.getInt();
    }
    
    KeyWrapper key = getKey(buffer, HEADER_LENGTH + extrasLength);

    if (newExpTime > 0) {
      StorageCommand.rescheduleExpiration(cache, key, newExpTime);
    }
    if (sendValue()) {
      Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
      ValueWrapper val = null;
      try {
        val = r.get(key);
      } catch (Exception e) {
        return handleBinaryException(key, request, response, "touch", e);
      }
      if (val == null) {
        response = request.getResponse();
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_NOT_FOUND.asShort());
      } else {
        if (isQuiet()) {
          return null;
        }
        byte[] realValue = val.getValue();
        int responseLength = HEADER_LENGTH + realValue.length;
        response = request.getResponse(responseLength);
        response.limit(responseLength);
        response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
        response.put(EXTRAS_LENGTH_INDEX, (byte) EXTRAS_LENGTH);
        response.putInt(TOTAL_BODY_LENGTH_INDEX, EXTRAS_LENGTH + realValue.length);
        response.putLong(POSITION_CAS, val.getVersion());
        response.position(HEADER_LENGTH);
        response.putInt(val.getFlags());
        response.put(realValue);
        
        response.flip();
      }
    }
    return response;
  }

  /**
   * Overridden by GAT and GATQ
   */
  protected boolean sendValue() {
    return false;
  }

  /**
   * Overridden by GATQ
   */
  protected boolean isQuiet() {
    return false;
  }
}
