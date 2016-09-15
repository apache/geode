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
package org.apache.geode.internal.memcached.commands;

import java.nio.ByteBuffer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.memcached.KeyWrapper;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * 
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
