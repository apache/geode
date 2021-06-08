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

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.stream.Collectors;

import io.netty.channel.ChannelHandlerContext;

import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.RedisResponse;

/**
 * The command class is used in holding a received Redis command. Each sent command resides in an
 * instance of this class. This class is designed to be used strictly by getter and setter methods.
 */
public class Command {

  private final List<byte[]> commandElems;
  private final RedisCommandType commandType;
  private String key;
  private byte[] bytes;

  /**
   * Constructor used to create a static marker Command for shutting down executors.
   */
  Command() {
    commandElems = null;
    commandType = null;
  }

  /**
   * Constructor for {@link Command}. Must initialize Command with a {@link SocketChannel} and a
   * {@link List} of command elements
   *
   * @param commandElems List of elements in command
   */
  public Command(List<byte[]> commandElems) {
    if (commandElems == null || commandElems.isEmpty()) {
      throw new IllegalArgumentException(
          "List of command elements cannot be empty -> List:" + commandElems);
    }
    this.commandElems = commandElems;

    RedisCommandType type;
    try {
      byte[] charCommand = commandElems.get(0);
      String commandName = Coder.bytesToString(charCommand).toUpperCase();
      type = RedisCommandType.valueOf(commandName);
    } catch (Exception e) {
      type = RedisCommandType.UNKNOWN;
    }
    this.commandType = type;
  }

  public boolean isSupported() {
    return commandType.isSupported();
  }

  public boolean isUnsupported() {
    return commandType.isUnsupported();
  }

  public boolean isUnknown() {
    return commandType.isUnknown();
  }

  /**
   * Used to get the command element list
   *
   * @return List of command elements in form of {@link List}
   */
  public List<byte[]> getProcessedCommand() {
    return this.commandElems;
  }

  /**
   * Used to get the command element list when every argument is also a key
   *
   * @return List of command elements in form of {@link List}
   */
  public List<RedisKey> getProcessedCommandKeys() {
    return this.commandElems.stream().map(RedisKey::new).collect(Collectors.toList());
  }

  /**
   * Getter method for the command type
   *
   * @return The command type
   */
  public RedisCommandType getCommandType() {
    return this.commandType;
  }

  /**
   * Convenience method to get a String representation of the key in a Redis command, always at the
   * second position in the sent command array
   *
   * @return Returns the second element in the parsed command list, which is always the key for
   *         commands indicating a key
   */
  public String getStringKey() {
    if (this.commandElems.size() > 1) {
      if (this.bytes == null) {
        this.bytes = this.commandElems.get(1);
        this.key = Coder.bytesToString(this.bytes);
      } else if (this.key == null) {
        this.key = Coder.bytesToString(this.bytes);
      }
      return this.key;
    } else {
      return null;
    }
  }

  public RedisKey getKey() {
    if (this.commandElems.size() > 1) {
      if (this.bytes == null) {
        this.bytes = this.commandElems.get(1);
      }
      return new RedisKey(this.bytes);
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    for (byte[] rawCommand : this.commandElems) {
      b.append(getHexEncodedString(rawCommand));
      b.append(' ');
    }
    return b.toString();
  }

  public static String getHexEncodedString(byte[] data) {
    return getHexEncodedString(data, data.length);
  }

  public static String getHexEncodedString(byte[] data, int size) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < size; i++) {
      byte aByte = data[i];
      if (aByte > 31 && aByte < 127) {
        builder.append((char) aByte);
      } else {
        if (aByte == 0x0a) {
          builder.append("\\n");
        } else if (aByte == 0x0d) {
          builder.append("\\r");
        } else {
          builder.append(String.format("\\x%02x", aByte));
        }
      }
    }

    return builder.toString();
  }

  public RedisResponse execute(ExecutionHandlerContext executionHandlerContext) throws Exception {
    RedisCommandType type = getCommandType();
    return type.executeCommand(this, executionHandlerContext);
  }

  public boolean isOfType(RedisCommandType type) {
    return type == getCommandType();
  }

  public String wrongNumberOfArgumentsErrorMessage() {
    String result;
    result = String.format("wrong number of arguments for '%s' command",
        getCommandType().toString().toLowerCase());
    return result;
  }

  private long asyncStartTime;

  public void setAsyncStartTime(long start) {
    asyncStartTime = start;
  }

  public long getAsyncStartTime() {
    return asyncStartTime;
  }

  private ChannelHandlerContext channelHandlerContext;

  public void setChannelHandlerContext(ChannelHandlerContext ctx) {
    channelHandlerContext = ctx;
  }

  public ChannelHandlerContext getChannelHandlerContext() {
    return channelHandlerContext;
  }
}
