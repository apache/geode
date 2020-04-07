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
package org.apache.geode.redis.internal;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;

/**
 * The command class is used in holding a received Redis command. Each sent command resides in an
 * instance of this class. This class is designed to be used strictly by getter and setter methods.
 */
public class Command {

  private final List<byte[]> commandElems;
  private final RedisCommandType commandType;
  private ByteBuf response;
  private String key;
  private ByteArrayWrapper bytes;

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
    this.response = null;

    RedisCommandType type;
    String commandName = null;
    try {
      byte[] charCommand = commandElems.get(0);
      commandName = Coder.bytesToString(charCommand).toUpperCase();
      type = RedisCommandType.valueOf(commandName);
    } catch (Exception e) {
      type = RedisCommandType.UNKNOWN;
    }
    this.commandType = type;

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
   * Used to get the command element list
   *
   * @return List of command elements in form of {@link List}
   */
  public List<ByteArrayWrapper> getProcessedCommandWrappers() {
    return this.commandElems.stream().map(ByteArrayWrapper::new).collect(Collectors.toList());
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
   * Getter method to get the response to be sent
   *
   * @return The response
   */
  public ByteBuf getResponse() {
    return response;
  }

  /**
   * Setter method to set the response to be sent
   *
   * @param response The response to be sent
   */
  public void setResponse(ByteBuf response) {
    this.response = response;
  }

  public boolean hasError() {
    if (response == null) {
      return false;
    }

    if (response.getByte(0) == Coder.ERROR_ID) {
      return true;
    }

    return false;
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
        this.bytes = new ByteArrayWrapper(this.commandElems.get(1));
        this.key = this.bytes.toString();
      } else if (this.key == null) {
        this.key = this.bytes.toString();
      }
      return this.key;
    } else {
      return null;
    }
  }

  public ByteArrayWrapper getKey() {
    if (this.commandElems.size() > 1) {
      if (this.bytes == null) {
        this.bytes = new ByteArrayWrapper(this.commandElems.get(1));
      }
      return this.bytes;
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    for (byte[] bs : this.commandElems) {
      b.append(Coder.bytesToString(bs));
      b.append(' ');
    }
    return b.toString();
  }
}
