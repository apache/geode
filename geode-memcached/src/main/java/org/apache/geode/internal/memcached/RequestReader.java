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
package org.apache.geode.internal.memcached;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.memcached.commands.AbstractCommand;
import org.apache.geode.internal.memcached.commands.ClientError;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * Reads the first line from the request and interprets the {@link Command} from the memcached
 * client
 *
 *
 */
public class RequestReader {

  @Immutable
  private static final Charset charsetASCII = StandardCharsets.US_ASCII;

  private static final ThreadLocal<CharsetDecoder> asciiDecoder =
      new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
          return charsetASCII.newDecoder();
        }
      };

  private ByteBuffer buffer;

  private ByteBuffer response;

  private static final int RESPONSE_HEADER_LENGTH = 24;

  private static final byte RESPONSE_MAGIC = (byte) 0x81;

  private static final int HEADER_LENGTH = 24;

  private static final byte REQUEST_MAGIC = (byte) 0x80;

  private static final int POSITION_OPCODE = 1;

  private static final int POSITION_OPAQUE = 12;

  private final Socket socket;

  private final Protocol protocol;

  private final CharBuffer commandBuffer = CharBuffer.allocate(11); // no command exceeds 9 chars

  public RequestReader(Socket socket, Protocol protocol) {
    buffer = ByteBuffer.allocate(getBufferSize(socket.getChannel()));
    // set position to limit so that first read attempt
    // returns hasRemaining() false
    buffer.position(buffer.limit());
    this.socket = socket;
    this.protocol = protocol;
  }

  public Command readCommand() throws IOException {
    if (protocol == Protocol.ASCII) {
      return readAsciiCommand();
    }
    return readBinaryCommand();
  }

  private Command readBinaryCommand() throws IOException {
    SocketChannel channel = socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot read from channel");
    }
    Command cmd = null;
    boolean done = false;
    boolean read = false;
    while (!done) {
      if (!buffer.hasRemaining()) {
        buffer.clear();
        read = true;
      } else if (!read) {
        // compact is meant for partial writes, but we want to use
        // it partial reads, so the new limit should be the position
        // after the compact operation and we want to start reading
        // from the beginning
        buffer.compact();
        buffer.limit(buffer.position());
        buffer.position(0);
      }
      if (read) {
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
          throw new IOException("EOF");
        }
        buffer.flip();
      }
      // read again if we did not read enough bytes
      if (buffer.limit() < HEADER_LENGTH) {
        buffer.compact();
        read = true;
        continue;
      }
      int requestLength = buffer.remaining();
      byte magic = buffer.get();
      if (magic != REQUEST_MAGIC) {
        throw new IllegalStateException("Not a valid request, magic byte incorrect");
      }
      byte opCode = buffer.get();
      if (ConnectionHandler.getLogger().finerEnabled()) {
        String str = Command.buffertoString(buffer);
        ConnectionHandler.getLogger().finer("Request:" + buffer + str);
      }
      int bodyLength = buffer.getInt(AbstractCommand.TOTAL_BODY_LENGTH_INDEX);
      if ((HEADER_LENGTH + bodyLength) > requestLength) {
        // set the position back to the start of the request
        buffer.position(buffer.position() - 2 /* since we read two bytes */);
        buffer.compact();
        // ensure that the buffer is big enough
        if (buffer.capacity() < (HEADER_LENGTH + bodyLength)) {
          // allocate bigger buffer and copy the bytes to the bigger buffer
          ByteBuffer oldBuffer = buffer;
          oldBuffer.position(0);
          buffer = ByteBuffer.allocate(HEADER_LENGTH + bodyLength);
          buffer.put(oldBuffer);
        }
        read = true;
        continue;
      }
      cmd = Command.getCommandFromOpCode(opCode);
      done = true;
    }
    if (ConnectionHandler.getLogger().fineEnabled()) {
      ConnectionHandler.getLogger().fine("read command " + cmd);
    }
    return cmd;
  }

  private Command readAsciiCommand() throws IOException {
    SocketChannel channel = socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot read from channel");
    }
    buffer.clear();
    int bytesRead = channel.read(buffer);
    if (bytesRead == -1) {
      throw new IOException("EOF");
    }
    buffer.flip();
    return Command.valueOf(readCommand(buffer));
  }

  private String readCommand(ByteBuffer buffer) throws CharacterCodingException {
    commandBuffer.clear();
    asciiDecoder.get().decode(buffer, commandBuffer, false);
    commandBuffer.flip();
    return trimCommand(commandBuffer.toString()).toUpperCase();
  }

  private String trimCommand(String str) {
    int indexOfSpace = str.indexOf(' ');
    String retVal = str;
    if (indexOfSpace != -1) {
      retVal = str.substring(0, indexOfSpace);
    }
    int indexOfR = retVal.indexOf("\r");
    if (indexOfR != -1) {
      retVal = retVal.substring(0, indexOfR);
    }
    if (retVal.equals("")) {
      if (ConnectionHandler.getLogger().infoEnabled()) {
        // TODO i18n
        ConnectionHandler.getLogger().info("Unknown command. ensure client protocol is ASCII");
      }
      throw new IllegalArgumentException("Unknown command. ensure client protocol is ASCII");
    }
    return retVal;
  }

  private int getBufferSize(SocketChannel channel) {
    int size = 1024;
    try {
      size = channel.socket().getReceiveBufferSize();
    } catch (SocketException e) {
      // use default size
    }
    return size;
  }

  public ByteBuffer getRequest() {
    buffer.rewind();
    return buffer;
  }

  public ByteBuffer getResponse() {
    return getResponse(RESPONSE_HEADER_LENGTH);
  }

  /**
   * Returns an initialized byteBuffer for sending the reply
   *
   * @param size size of ByteBuffer
   * @return the initialized response buffer
   */
  public ByteBuffer getResponse(int size) {
    if (response == null || response.capacity() < size) {
      response = ByteBuffer.allocate(size);
    }
    clear(response);
    response.put(RESPONSE_MAGIC);
    response.rewind();
    response.limit(size);
    return response;
  }

  private void clear(ByteBuffer response) {
    response.position(0);
    response.limit(response.capacity());
    response.put(getCleanByteArray());
    while (response.remaining() > getCleanByteArray().length) {
      response.put(getCleanByteArray());
    }
    while (response.remaining() > 0) {
      response.put((byte) 0);
    }
    response.clear();
  }

  @Immutable
  private static final byte[] cleanByteArray = createCleanByteArray();

  private static byte[] createCleanByteArray() {
    byte[] cleanByteArray = new byte[RESPONSE_HEADER_LENGTH];
    for (int i = 0; i < cleanByteArray.length; i++) {
      cleanByteArray[i] = 0;
    }
    return cleanByteArray;
  }

  private byte[] getCleanByteArray() {
    return cleanByteArray;
  }

  public void sendReply(ByteBuffer reply) throws IOException {
    // for binary set the response opCode
    if (protocol == Protocol.BINARY) {
      reply.rewind();
      reply.put(POSITION_OPCODE, buffer.get(POSITION_OPCODE));
      reply.putInt(POSITION_OPAQUE, buffer.getInt(POSITION_OPAQUE));
      if (ConnectionHandler.getLogger().finerEnabled()) {
        ConnectionHandler.getLogger()
            .finer("sending reply:" + reply + " " + Command.buffertoString(reply));
      }
    }
    SocketChannel channel = socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot write to channel");
    }
    channel.write(reply);
  }

  public void sendException(Exception e) {
    SocketChannel channel = socket.getChannel();
    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("cannot write to channel");
    }
    try {
      if (e instanceof ClientError) {
        channel.write(charsetASCII.encode(Reply.CLIENT_ERROR.toString()));
      } else {
        channel.write(charsetASCII.encode(Reply.ERROR.toString()));
      }
    } catch (IOException ignored) {
    }
  }
}
