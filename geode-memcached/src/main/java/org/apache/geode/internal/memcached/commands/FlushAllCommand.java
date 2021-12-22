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
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

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
      StorageCommand.getExpiryExecutor().schedule(() -> r.destroyRegion(), delay, TimeUnit.SECONDS);
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
      cache.getLogger().fine("flush:delay:" + delay);
    }

    if (delay == 0) {
      try {
        r.destroyRegion();
      } catch (Exception e) {
        return handleBinaryException("", request, request.getResponse(), "flushall", e);
      }
    } else {
      StorageCommand.getExpiryExecutor().schedule(() -> r.destroyRegion(), delay, TimeUnit.SECONDS);
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
