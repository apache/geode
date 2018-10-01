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
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.logging.LoggingThreadFactory;
import org.apache.geode.internal.memcached.KeyWrapper;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * general format of the command is:<br/>
 * <code>
 * &lt;command name&gt; &lt;key&gt; &lt;flags&gt; &lt;exptime&gt; &lt;bytes&gt; [noreply]\r\n
 * </code><br/>
 * After this line, the client sends the data block:<br/>
 * <code>
 * &lt;data block&gt;\r\n
 * </code>
 *
 */

public abstract class StorageCommand extends AbstractCommand {

  /**
   * thread pool for scheduling expiration tasks
   */
  private static ScheduledExecutorService expiryExecutor =
      new ScheduledThreadPoolExecutor(1, new LoggingThreadFactory("memcached-expiryExecutor"));

  private static ConcurrentMap<Object, ScheduledFuture> expiryFutures =
      new ConcurrentHashMap<Object, ScheduledFuture>();

  /**
   * number of seconds in 30 days
   */
  private static final long secsIn30Days = 60 * 60 * 24 * 30;

  @Override
  public ByteBuffer processCommand(RequestReader reader, Protocol protocol, Cache cache) {
    ByteBuffer buffer = reader.getRequest();
    if (protocol == Protocol.ASCII) {
      return processAsciiCommand(buffer, cache);
    }
    return processBinaryComand(reader, cache);
  }

  private ByteBuffer processAsciiCommand(ByteBuffer buffer, Cache cache) {
    CharBuffer flb = getFirstLineBuffer();
    getAsciiDecoder().decode(buffer, flb, false);
    flb.flip();
    String firstLine = getFirstLine();
    String[] firstLineElements = firstLine.split(" ");
    String key = firstLineElements[1];
    int flags = Integer.parseInt(firstLineElements[2]);
    long expTime = Long.parseLong(firstLineElements[3]);
    int numBytes = Integer.parseInt(stripNewline(firstLineElements[4]));
    boolean noReply = false;
    if (firstLineElements.length > 5) {
      noReply = true;
    }
    byte[] value = new byte[numBytes];
    buffer.position(firstLine.length());
    try {
      for (int i = 0; i < numBytes; i++) {
        value[i] = buffer.get();
      }
    } catch (BufferUnderflowException e) {
      throw new ClientError("error reading value");
    }
    if (getLogger().fineEnabled()) {
      getLogger().fine("key:" + key);
      getLogger().fine("value:" + Arrays.toString(value));
    }
    ByteBuffer retVal = processStorageCommand(key, value, flags, cache);
    if (expTime > 0) {
      scheduleExpiration(key, expTime, cache);
    }
    return noReply ? null : retVal;
  }

  private ByteBuffer processBinaryComand(RequestReader request, Cache cache) {
    ByteBuffer buffer = request.getRequest();
    int extrasLength = buffer.get(EXTRAS_LENGTH_INDEX);
    int flags = 0, expTime = 0;

    KeyWrapper key = getKey(buffer, HEADER_LENGTH + extrasLength);

    if (extrasLength > 0) {
      assert extrasLength == 8;
      buffer.position(HEADER_LENGTH);
      flags = buffer.getInt();
      expTime = buffer.getInt();
    }

    byte[] value = getValue(buffer);

    long cas = buffer.getLong(POSITION_CAS);

    ByteBuffer retVal = processBinaryStorageCommand(key, value, cas, flags, cache, request);
    if (expTime > 0) {
      scheduleExpiration(key, expTime, cache);
    }
    if (getLogger().fineEnabled()) {
      getLogger().fine("key:" + key);
      getLogger().fine("value:" + Arrays.toString(value));
    }
    return retVal;
  }

  /**
   * Schedules the entry to expire based on the following: the expiration time sent may either be
   * Unix time (number of seconds since January 1, 1970, as a 32-bit value), or a number of seconds
   * starting from current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that,
   * the server will consider it to be real Unix time value rather than an offset from current time.
   *
   */
  private void scheduleExpiration(final Object key, long p_expTime, final Cache cache) {
    long expTime = p_expTime;
    assert expTime > 0;
    if (p_expTime > secsIn30Days) {
      expTime = p_expTime - System.currentTimeMillis();
      if (expTime < 0) {
        getLogger().info("Invalid expiration time passed, key:" + key + " will not expire");
        return;
      }
    }
    ScheduledFuture f =
        expiryExecutor.schedule(new ExpiryTask(cache, key), expTime, TimeUnit.SECONDS);
    expiryFutures.put(key, f);
  }

  public abstract ByteBuffer processStorageCommand(String key, byte[] value, int flags,
      Cache cache);

  public abstract ByteBuffer processBinaryStorageCommand(Object key, byte[] value, long cas,
      int flags, Cache cache, RequestReader request);

  protected static ScheduledExecutorService getExpiryExecutor() {
    return expiryExecutor;
  }

  /**
   * reschedules expiration for a key only if one was previously scheduled
   *
   * @return true if successfully rescheduled, false otherwise
   */
  public static boolean rescheduleExpiration(Cache cache, Object key, int newExpTime) {
    ScheduledFuture f = expiryFutures.get(key);
    if (f != null) {
      if (f.cancel(false)) {
        ScheduledFuture f2 =
            expiryExecutor.schedule(new ExpiryTask(cache, key), newExpTime, TimeUnit.SECONDS);
        expiryFutures.put(key, f2);
        return true;
      }
    }
    return false;
  }

  /**
   * Removes key from the cache and expiryFuture
   */
  public static class ExpiryTask implements Runnable {
    private final Cache cache;
    private final Object key;

    public ExpiryTask(Cache cache, Object key) {
      this.cache = cache;
      this.key = key;
    }

    @Override
    public void run() {
      getMemcachedRegion(cache).remove(key);
      expiryFutures.remove(key);
      if (cache.getLogger().fineEnabled()) {
        cache.getLogger().fine("expiration removed key:" + key);
      }
    }
  }
}
