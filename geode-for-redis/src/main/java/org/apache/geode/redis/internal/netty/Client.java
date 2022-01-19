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
 *
 */

package org.apache.geode.redis.internal.netty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.pubsub.PubSub;


public class Client {
  private static final Logger logger = LogService.getLogger();

  private final Channel channel;
  private final ByteBufAllocator byteBufAllocator;

  private byte[] name;
  /**
   * The subscription sets do not need to be thread safe
   * because they are only used by a single thread as it
   * does pubsub operations for a particular Client.
   */
  private final Set<byte[]> channelSubscriptions =
      new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
  private final Set<byte[]> patternSubscriptions =
      new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);

  public Client(Channel remoteAddress, PubSub pubsub) {
    channel = remoteAddress;
    byteBufAllocator = channel.alloc();
    channel.closeFuture().addListener(future -> pubsub.clientDisconnect(this));
  }

  public String getRemoteAddress() {
    return channel.remoteAddress().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Client client = (Client) o;
    return Objects.equals(channel, client.channel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(channel);
  }

  public String toString() {
    return channel.toString();
  }

  public boolean hasSubscriptions() {
    return !channelSubscriptions.isEmpty() || !patternSubscriptions.isEmpty();
  }

  public long getSubscriptionCount() {
    return channelSubscriptions.size() + patternSubscriptions.size();
  }

  public void clearSubscriptions() {
    channelSubscriptions.clear();
    patternSubscriptions.clear();
  }

  public boolean addChannelSubscription(byte[] channel) {
    return channelSubscriptions.add(channel);
  }

  public boolean addPatternSubscription(byte[] pattern) {
    return patternSubscriptions.add(pattern);
  }

  public boolean removeChannelSubscription(byte[] channel) {
    return channelSubscriptions.remove(channel);
  }

  public boolean removePatternSubscription(byte[] pattern) {
    return patternSubscriptions.remove(pattern);
  }

  public List<byte[]> getChannelSubscriptions() {
    if (channelSubscriptions.isEmpty()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(channelSubscriptions);
  }

  public List<byte[]> getPatternSubscriptions() {
    if (patternSubscriptions.isEmpty()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(patternSubscriptions);
  }

  public ByteBuf getChannelWriteBuffer() {
    return byteBufAllocator.ioBuffer();
  }

  public ChannelFuture writeBufferToChannel(ByteBuf buffer) {
    if (!logger.isDebugEnabled()) {
      return channel.writeAndFlush(buffer);
    } else {
      // snapshot buffer before netty reuses it
      final byte[] bufferBytes = getBufferBytes(buffer);
      return channel.writeAndFlush(buffer)
          .addListener(
              (ChannelFutureListener) f -> logResponse(bufferBytes, channel.remoteAddress(),
                  f.cause()));
    }
  }

  @VisibleForTesting
  byte[] getBufferBytes(ByteBuf buffer) {
    int size = buffer.readableBytes();
    byte[] result = new byte[size];
    buffer.getBytes(0, result);
    return result;
  }

  public ChannelFuture writeToChannel(RedisResponse response) {
    if (!logger.isDebugEnabled() && !response.hasAfterWriteCallback()) {
      return channel.writeAndFlush(response.encode(byteBufAllocator));
    } else {
      return channel.writeAndFlush(response.encode(byteBufAllocator))
          .addListener((ChannelFutureListener) f -> {
            response.afterWrite();
            logResponse(response, channel.remoteAddress(), f.cause());
          });
    }
  }

  private void logResponse(RedisResponse response, Object extraMessage, Throwable cause) {
    if (logger.isDebugEnabled() && response != null) {
      ByteBuf buf = response.encode(new UnpooledByteBufAllocator(false));
      if (cause == null) {
        logger.debug("Redis command returned: {} - {}",
            Command.getHexEncodedString(buf.array(), buf.readableBytes()), extraMessage);
      } else {
        logger.debug("Redis command FAILED to return: {} - {}",
            Command.getHexEncodedString(buf.array(), buf.readableBytes()), extraMessage, cause);
      }
    }
  }

  private void logResponse(byte[] bytes, Object extraMessage, Throwable cause) {
    if (logger.isDebugEnabled()) {
      if (cause == null) {
        logger.debug("Redis command returned: {} - {}",
            Command.getHexEncodedString(bytes, bytes.length), extraMessage);
      } else {
        logger.debug("Redis command FAILED to return: {} - {}",
            Command.getHexEncodedString(bytes, bytes.length), extraMessage, cause);
      }
    }
  }

  public void setName(byte[] name) {
    if (name.length == 0) {
      this.name = null;
    } else {
      this.name = name;
    }
  }

  public byte[] getName() {
    return name;
  }
}
