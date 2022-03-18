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
package org.apache.geode.redis.internal.commands;

import static org.apache.geode.redis.internal.RedisConstants.WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.stream.Collectors;

import io.netty.channel.ChannelHandlerContext;

import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * The command class is used in holding a received Redis command. Each sent command resides in an
 * instance of this class. This class is designed to be used strictly by getter and setter methods.
 */
public class Command {

  private final List<byte[]> commandElems;
  private final RedisCommandType commandType;
  private String keyString;
  private byte[] keyBytes;

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
      String commandName = bytesToString(charCommand).toUpperCase();
      type = RedisCommandType.valueOf(commandName);
    } catch (Exception e) {
      type = RedisCommandType.UNKNOWN;
    }
    commandType = type;
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
    return commandElems;
  }

  /**
   * returns the list of arguments given to this command
   * Note: this is always the {@link #getProcessedCommand} result without its first element.
   */
  public List<byte[]> getCommandArguments() {
    return commandElems.subList(1, commandElems.size());
  }

  /**
   * Used to get the command element list when every argument is also a key
   *
   * @return List of command elements in form of {@link List}
   */
  public List<RedisKey> getProcessedCommandKeys() {
    return commandElems.stream().map(RedisKey::new).collect(Collectors.toList());
  }

  /**
   * Getter method for the command type
   *
   * @return The command type
   */
  public RedisCommandType getCommandType() {
    return commandType;
  }

  /**
   * Convenience method to get the byte array representation of the key in a Redis command, always
   * at the second position in the sent command array
   *
   * @return Returns the second element in the parsed command list, which is always the key for
   *         commands indicating a key
   */
  public byte[] getBytesKey() {
    if (commandElems.size() > 1) {
      if (keyBytes == null) {
        keyBytes = commandElems.get(1);
      }
      return keyBytes;
    } else {
      return null;
    }
  }

  /**
   * Convenience method to get a String representation of the key in a Redis command, always at the
   * second position in the sent command array
   *
   * @return Returns the second element in the parsed command list, which is always the key for
   *         commands indicating a key
   */
  public String getStringKey() {
    if (commandElems.size() > 1) {
      if (keyBytes == null) {
        keyBytes = commandElems.get(1);
        keyString = bytesToString(keyBytes);
      } else if (keyString == null) {
        keyString = bytesToString(keyBytes);
      }
      return keyString;
    } else {
      return null;
    }
  }

  public RedisKey getKey() {
    if (commandElems.size() > 1) {
      if (keyBytes == null) {
        keyBytes = commandElems.get(1);
      }
      return new RedisKey(keyBytes);
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    if (commandType.equals(RedisCommandType.AUTH)) {
      return "AUTH command with " + (commandElems.size() - 1) + " argument(s)";
    } else {
      StringBuilder b = new StringBuilder();
      for (byte[] rawCommand : commandElems) {
        b.append(getHexEncodedString(rawCommand));
        b.append(' ');
      }
      return b.toString();
    }
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
    result = String.format(WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND,
        getCommandType().toString().toLowerCase());
    return result;
  }

  private ChannelHandlerContext channelHandlerContext;

  public void setChannelHandlerContext(ChannelHandlerContext ctx) {
    channelHandlerContext = ctx;
  }

  public ChannelHandlerContext getChannelHandlerContext() {
    return channelHandlerContext;
  }
}
