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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.geode.LogWriter;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.memcached.CommandProcessor;
import org.apache.geode.internal.memcached.KeyWrapper;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.internal.memcached.ValueWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * Abstract class with utility methods for all Command classes.
 *
 *
 */
public abstract class AbstractCommand implements CommandProcessor {

  protected static final char N = '\n';

  @Immutable
  protected static final Charset asciiCharset = StandardCharsets.US_ASCII;

  protected static final int POSITION_RESPONSE_STATUS = 6;
  protected static final int POSITION_CAS = 16;

  private final ThreadLocal<CharsetDecoder> asciiDecoder = new ThreadLocal<CharsetDecoder>() {
    @Override
    protected CharsetDecoder initialValue() {
      return asciiCharset.newDecoder();
    }
  };

  private final ThreadLocal<CharsetEncoder> asciiEncoder = new ThreadLocal<CharsetEncoder>() {
    @Override
    protected CharsetEncoder initialValue() {
      return asciiCharset.newEncoder();
    }
  };

  private LogWriter logger;

  /**
   * A buffer to read and decode the first line.
   */
  protected static final ThreadLocal<CharBuffer> firstLineBuffer = new ThreadLocal<CharBuffer>();

  @Override
  public abstract ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache);

  public static final int KEY_LENGTH_INDEX = 2;
  public static final int EXTRAS_LENGTH_INDEX = 4;
  public static final int TOTAL_BODY_LENGTH_INDEX = 8;
  public static final int HEADER_LENGTH = 24;

  protected KeyWrapper getKey(ByteBuffer buffer, int keyStartIndex) {
    int keyLength = buffer.getShort(KEY_LENGTH_INDEX);
    if (getLogger().finerEnabled()) {
      getLogger().finer("keyLength:" + keyLength);
    }
    byte[] key = new byte[keyLength];
    buffer.position(keyStartIndex);
    buffer.get(key);
    // for (int i=0; i<keyLength; i++) {
    // key[i] = buffer.get();
    // }
    if (getLogger().finerEnabled()) {
      getLogger().finer("key:" + Arrays.toString(key));
    }
    return KeyWrapper.getWrappedKey(key);
  }

  protected byte[] getValue(ByteBuffer buffer) {
    int extrasLength = buffer.get(EXTRAS_LENGTH_INDEX);
    int totalBodyLength = buffer.getInt(TOTAL_BODY_LENGTH_INDEX);
    int keyLength = buffer.getShort(KEY_LENGTH_INDEX);
    int valueLength = totalBodyLength - (keyLength + extrasLength);
    byte[] value = new byte[valueLength];
    buffer.position(HEADER_LENGTH + totalBodyLength - valueLength);
    if (getLogger().finerEnabled()) {
      getLogger().finer("val: totalBody:" + totalBodyLength + " valLen:" + valueLength);
    }
    buffer.get(value);
    // for (int i=0; i<valueLength; i++) {
    // value[i] = buffer.get();
    // }
    if (getLogger().finerEnabled()) {
      getLogger().finer("val:" + Arrays.toString(value) + " totalBody:" + totalBodyLength
          + " valLen:" + valueLength);
    }
    return value;
  }

  protected String getFirstLine() {
    CharBuffer buffer = firstLineBuffer.get();
    StringBuilder builder = new StringBuilder();
    try {
      char c = buffer.get();
      for (;;) {
        builder.append(c);
        if (c == N) {
          break;
        }
        c = buffer.get();
      }
    } catch (BufferUnderflowException e) {
      throw new ClientError("error reading command:" + builder);
    }
    String firstLine = builder.toString();
    if (getLogger().fineEnabled()) {
      getLogger().fine("gemcached command:" + firstLine);
    }
    return firstLine;
  }

  protected static Region<Object, ValueWrapper> getMemcachedRegion(Cache cache) {
    Region<Object, ValueWrapper> r = cache.getRegion(GemFireMemcachedServer.REGION_NAME);
    if (r == null) {
      synchronized (AbstractCommand.class) {
        r = cache.getRegion(GemFireMemcachedServer.REGION_NAME);
        if (r == null) {
          RegionFactory<Object, ValueWrapper> rf =
              cache.createRegionFactory(RegionShortcut.PARTITION);
          r = rf.create(GemFireMemcachedServer.REGION_NAME);
        }
      }
    }
    return r;
  }

  protected CharBuffer getFirstLineBuffer() {
    CharBuffer buffer = firstLineBuffer.get();
    if (buffer == null) {
      buffer = CharBuffer.allocate(256);
      firstLineBuffer.set(buffer);
    }
    buffer.clear();
    return buffer;
  }

  protected String stripNewline(String str) {
    int indexOfR = str.indexOf("\r");
    if (indexOfR != -1) {
      return str.substring(0, indexOfR);
    }
    return str;
  }

  protected long getLongFromByteArray(byte[] bytes) {
    long value = 0;
    for (int i = 0; i < bytes.length; i++) {
      value = (value << 8) + (bytes[i] & 0xff);
    }
    return value;
  }

  protected LogWriter getLogger() {
    if (logger == null) {
      Cache cache = CacheFactory.getAnyInstance();
      if (cache != null) {
        logger = cache.getLogger();
      } else {
        throw new IllegalStateException("Could not initialize logger");
      }
    }
    return logger;
  }

  protected CharsetDecoder getAsciiDecoder() {
    return asciiDecoder.get();
  }

  protected CharsetEncoder getAsciiEncoder() {
    return asciiEncoder.get();
  }

  /**
   * Used to handle exceptions thrown by the region callbacks.
   */
  protected ByteBuffer handleBinaryException(Object key, RequestReader request, ByteBuffer response,
      String operation, Exception e) {
    getLogger().info("Exception occurred while processing " + operation + " :" + key, e);
    String errStr = e.getMessage() == null ? "SERVER ERROR" : e.getMessage();
    byte[] errMsg = errStr.getBytes(asciiCharset);
    int responseLength = HEADER_LENGTH + errMsg.length;
    if (response.capacity() < responseLength) {
      response = request.getResponse(responseLength);
    }
    response.limit(responseLength);
    response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.INTERNAL_ERROR.asShort());
    response.putInt(TOTAL_BODY_LENGTH_INDEX, errMsg.length);
    response.position(HEADER_LENGTH);
    response.put(errMsg);
    return response;
  }
}
