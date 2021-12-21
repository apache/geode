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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.memcached.KeyWrapper;
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 *
 * The retrieval commands "get" and "gets" operates like this:<br/>
 * <code>
 * get &lt;key&gt;*\r\n<br/>
 * gets &lt;key&gt;*\r\n
 * </code>
 * <p>
 * Each item sent by the server looks like this:<br/>
 * <code>
 * VALUE &lt;key&gt; &lt;flags&gt; &lt;bytes&gt; [&lt;cas unique&gt;]\r\n<br/>
 * &lt;data block&gt;\r\n
 * </code>
 *
 *
 */
public class GetCommand extends AbstractCommand {

  private static final String VALUE = "VALUE";
  private static final String W_SPACE = " ";
  private static final String RN = "\r\n";
  @Immutable
  private static final byte[] RN_BUF = toEncodedArray(RN);
  @Immutable
  private static final byte[] END_BUF = toEncodedArray(Reply.END.toString());

  private static final byte[] toEncodedArray(String string) {
    ByteBuffer buffer = asciiCharset.encode(string);
    buffer.rewind();
    byte[] result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }

  /**
   * buffer used to compose one line of reply
   */
  private static final ThreadLocal<CharBuffer> lineBuffer = new ThreadLocal<>();

  /**
   * defaults to the default send buffer size on socket
   */
  private static final int REPLY_BUFFER_CAPACITY =
      Integer.getInteger("replyBufferCapacity", 146988);

  /**
   * buffer for sending get replies, one per thread
   */
  private static final ThreadLocal<ByteBuffer> replyBuffer = new ThreadLocal<>();

  private static final int EXTRAS_LENGTH = 4;

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return processAsciiCommand(request, cache);
    }
    return processBinaryCommand(request.getRequest(), request, cache, request.getResponse());
  }

  protected ByteBuffer processBinaryCommand(ByteBuffer buffer, RequestReader request, Cache cache,
      ByteBuffer response) {
    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);

    KeyWrapper key = getKey(buffer, HEADER_LENGTH);
    ValueWrapper val = null;
    try {
      val = r.get(key);
    } catch (Exception e) {
      return handleBinaryException(key, request, response, "get", e);
    }
    if (getLogger().fineEnabled()) {
      getLogger().fine("get:key:" + key + " val:" + val);
    }
    if (val == null) {
      if (isQuiet()) {
        return null;
      }
      response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_NOT_FOUND.asShort());
    } else {
      byte[] realValue = val.getValue();
      int responseLength = HEADER_LENGTH + realValue.length + EXTRAS_LENGTH
          + (sendKeysInResponse() ? key.getKey().length : 0);
      if (response.capacity() < responseLength) {
        response = request.getResponse(responseLength);
      }
      response.limit(responseLength);
      response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
      if (sendKeysInResponse()) {
        response.putShort(KEY_LENGTH_INDEX, (short) key.getKey().length);
      }
      response.put(EXTRAS_LENGTH_INDEX, (byte) EXTRAS_LENGTH);
      response.putInt(TOTAL_BODY_LENGTH_INDEX,
          EXTRAS_LENGTH + realValue.length + (sendKeysInResponse() ? key.getKey().length : 0));
      response.putLong(POSITION_CAS, val.getVersion());
      response.position(HEADER_LENGTH);
      response.putInt(val.getFlags());
      if (sendKeysInResponse()) {
        response.put(key.getKey());
      }
      response.put(realValue);

      response.flip();
    }
    return response;
  }

  /**
   * Overridden by GetQ and getKQ to not send reply on cache miss
   */
  protected boolean isQuiet() {
    return false;
  }

  /**
   * Overridden by GetK command
   */
  protected boolean sendKeysInResponse() {
    return false;
  }

  private ByteBuffer processAsciiCommand(RequestReader request, Cache cache) {
    ByteBuffer buffer = request.getRequest();
    CharBuffer flb = getFirstLineBuffer();
    getAsciiDecoder().reset();
    getAsciiDecoder().decode(buffer, flb, false);
    flb.flip();
    String firstLine = getFirstLine();
    String[] firstLineElements = firstLine.split(" ");

    boolean isGets = firstLineElements[0].equals("gets");
    Set<String> keys = new HashSet<>();
    for (int i = 1; i < firstLineElements.length; i++) {
      keys.add(stripNewline(firstLineElements[i]));
    }

    Region<Object, ValueWrapper> r = getMemcachedRegion(cache);
    Map<Object, ValueWrapper> results = r.getAll(keys);

    return composeReply(results, isGets);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_PARAM_DEREF",
      justification = "findbugs complains that v is null while putting into buffer, but it is not")
  private ByteBuffer composeReply(Map<Object, ValueWrapper> results, boolean isGets) {
    Iterator<Entry<Object, ValueWrapper>> it = results.entrySet().iterator();
    ByteBuffer buffer = getReplyBuffer();
    while (it.hasNext()) {
      Entry<Object, ValueWrapper> e = it.next();
      if (getLogger().fineEnabled()) {
        getLogger().fine("get compose reply:" + e);
      }
      ValueWrapper valWrapper = e.getValue();
      if (valWrapper != null) {
        byte[] v = valWrapper.getValue();
        CharBuffer reply = getLineBuffer();
        reply.put(VALUE).put(W_SPACE);
        reply.put(e.getKey().toString()).put(W_SPACE);
        reply.put(Integer.toString(valWrapper.getFlags())).put(W_SPACE); // flags

        String valBytes = v == null ? Integer.toString(0) : Integer.toString(v.length);
        reply.put(valBytes);
        if (isGets) {
          // send the version for gets command
          reply.put(W_SPACE);
          reply.put(Long.toString(valWrapper.getVersion()));
        }
        reply.put(RN);
        reply.flip();
        getAsciiEncoder().encode(reply, buffer, false);
        // put the actual value
        buffer.put(v);
        buffer.put(RN_BUF);
      }
    }
    buffer.put(END_BUF);
    buffer.flip();
    return buffer;
  }

  private ByteBuffer getReplyBuffer() {
    ByteBuffer retVal = replyBuffer.get();
    if (retVal == null) {
      retVal = ByteBuffer.allocate(REPLY_BUFFER_CAPACITY);
      replyBuffer.set(retVal);
    }
    retVal.clear();
    return retVal;
  }

  private CharBuffer getLineBuffer() {
    CharBuffer retVal = lineBuffer.get();
    if (retVal == null) {
      retVal = CharBuffer.allocate(1024);
      lineBuffer.set(retVal);
    }
    retVal.clear();
    return retVal;
  }
}
