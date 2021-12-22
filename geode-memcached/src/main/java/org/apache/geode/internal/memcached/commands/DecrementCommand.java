/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.memcached.commands;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.memcached.KeyWrapper;
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * decr <key> <value> [noreply]\r\n
 *
 * value is the amount by which the client wants to increase/decrease the item. It is a decimal
 * representation of a 64-bit unsigned integer.
 *
 * The data for the item is treated as decimal representation of a 64-bit unsigned integer. Also,
 * the item must already exist for incr/decr to work; these commands won't pretend that a
 * non-existent key exists with value 0; instead, they will fail.
 *
 *
 */
public class DecrementCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return processAsciiCommand(request.getRequest(), cache);
    }
    return processBinaryProtocol(request, cache);
  }

  private ByteBuffer processAsciiCommand(ByteBuffer buffer, Cache cache) {
    CharBuffer flb = getFirstLineBuffer();
    getAsciiDecoder().reset();
    getAsciiDecoder().decode(buffer, flb, false);
    flb.flip();
    String firstLine = getFirstLine();
    String[] firstLineElements = firstLine.split(" ");

    assert "decr".equals(firstLineElements[0]);
    String key = firstLineElements[1];
    String decrByStr = stripNewline(firstLineElements[2]);
    long decrBy = Long.parseLong(decrByStr);
    boolean noReply = firstLineElements.length > 3;

    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    String reply = Reply.NOT_FOUND.toString();
    ByteBuffer newVal = ByteBuffer.allocate(8);
    while (true) {
      ValueWrapper oldValWrapper = r.get(key);
      if (oldValWrapper == null) {
        break;
      }
      newVal.clear();
      byte[] oldVal = oldValWrapper.getValue();
      long oldLong = getLongFromByteArray(oldVal);
      long newLong = oldLong - decrBy;
      newVal.putLong(newLong);
      ValueWrapper newValWrapper = ValueWrapper.getWrappedValue(newVal.array(), 0/* flags */);
      if (r.replace(key, oldValWrapper, newValWrapper)) {
        reply = newLong + "\r\n";
        break;
      }
    }
    return noReply ? null : asciiCharset.encode(reply);
  }

  private static final int LONG_LENGTH = 8;

  private ByteBuffer processBinaryProtocol(RequestReader request, Cache cache) {
    ByteBuffer buffer = request.getRequest();
    int extrasLength = buffer.get(EXTRAS_LENGTH_INDEX);
    final KeyWrapper key = getKey(buffer, HEADER_LENGTH + extrasLength);

    long decrBy = buffer.getLong(HEADER_LENGTH);
    long initialVal = buffer.getLong(HEADER_LENGTH + LONG_LENGTH);
    int expiration = buffer.getInt(HEADER_LENGTH + LONG_LENGTH + LONG_LENGTH);

    final Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    ByteBuffer newVal = ByteBuffer.allocate(8);
    boolean notFound = false;
    ValueWrapper newValWrapper = null;

    try {
      while (true) {
        ValueWrapper oldValWrapper = r.get(key);
        if (oldValWrapper == null) {
          if (expiration == -1) {
            notFound = true;
          } else {
            newVal.putLong(0, initialVal);
            newValWrapper = ValueWrapper.getWrappedValue(newVal.array(), 0/* flags */);
            r.put(key, newValWrapper);
          }
          break;
        }
        byte[] oldVal = oldValWrapper.getValue();
        long oldLong = getLongFromByteArray(oldVal);
        long newLong = oldLong - decrBy;
        if (newLong < 0) {
          newLong = 0;
        }
        newVal.putLong(0, newLong);
        newValWrapper = ValueWrapper.getWrappedValue(newVal.array(), 0/* flags */);
        if (r.replace(key, oldValWrapper, newValWrapper)) {
          break;
        }
      }
    } catch (Exception e) {
      return handleBinaryException(key, request, request.getResponse(), "decrement", e);
    }

    if (expiration > 0) {
      StorageCommand.getExpiryExecutor().schedule(() -> {
        r.destroy(key);
      }, expiration, TimeUnit.SECONDS);
    }

    if (getLogger().fineEnabled()) {
      getLogger().fine("decr:key:" + key + " decrBy:" + decrBy + " initVal:" + initialVal + " exp:"
          + expiration + " notFound:" + notFound);
    }

    ByteBuffer response = null;
    if (notFound) {
      response = request.getResponse();
      response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_NOT_FOUND.asShort());
    } else {
      if (isQuiet()) {
        return null;
      }
      response = request.getResponse(HEADER_LENGTH + LONG_LENGTH);
      response.putInt(TOTAL_BODY_LENGTH_INDEX, LONG_LENGTH);
      response.putLong(HEADER_LENGTH, newVal.getLong(0));
      response.putLong(POSITION_CAS, newValWrapper.getVersion());
    }
    return response;
  }

  /**
   * Overridden by Q command
   */
  protected boolean isQuiet() {
    return false;
  }
}
