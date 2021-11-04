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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNAUTHENTICATED_BULK;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNAUTHENTICATED_MULTIBULK;
import static org.apache.geode.redis.internal.RedisProperties.getIntegerSystemProperty;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.ARRAY_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.BULK_STRING_ID;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bCRLF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.RedisProperties;
import org.apache.geode.redis.internal.services.RedisSecurityService;
import org.apache.geode.redis.internal.statistics.RedisStats;

/**
 * This is the first part of the channel pipeline for Netty. Here incoming bytes are read and a
 * created {@link Command} is sent down the pipeline. It is unfortunate that this class is not
 * {@link io.netty.channel.ChannelHandler.Sharable} because no state is kept in this class. State is
 * kept by {@link ByteToMessageDecoder}, it may be worthwhile to look at a different decoder setup
 * as to avoid allocating a decoder for every new connection.
 * <p>
 * The code flow of the protocol parsing may not be exactly Java like, but this is done very
 * intentionally. It was found that in cases where large Redis requests are sent that end up being
 * fragmented, throwing exceptions when the command could not be fully parsed took up an enormous
 * amount of cpu time. The simplicity of the Redis protocol allows us to just back out and wait for
 * more data, while exceptions are left to malformed requests which should never happen if using a
 * proper Redis client.
 */
public class ByteToCommandDecoder extends ByteToMessageDecoder {

  public static final String UNAUTHENTICATED_MAX_ARRAY_SIZE_PARAM =
      RedisProperties.UNAUTHENTICATED_MAX_ARRAY_SIZE;
  public static final String UNAUTHENTICATED_MAX_BULK_STRING_LENGTH_PARAM =
      RedisProperties.UNAUTHENTICATED_MAX_BULK_STRING_LENGTH;

  private static final int MAX_BULK_STRING_LENGTH = 512 * 1024 * 1024; // 512 MB
  // These 2 defaults are taken from native Redis
  public static final int UNAUTHENTICATED_MAX_ARRAY_SIZE =
      getIntegerSystemProperty(UNAUTHENTICATED_MAX_ARRAY_SIZE_PARAM, 10, 1);
  public static final int UNAUTHENTICATED_MAX_BULK_STRING_LENGTH =
      getIntegerSystemProperty(UNAUTHENTICATED_MAX_BULK_STRING_LENGTH_PARAM, 16384, 1);

  private final RedisStats redisStats;
  private final RedisSecurityService securityService;
  private final ChannelId channelId;

  public ByteToCommandDecoder(RedisStats redisStats, RedisSecurityService securityService,
      ChannelId channelId) {
    this.redisStats = redisStats;
    this.securityService = securityService;
    this.channelId = channelId;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    Command c;
    long bytesRead = 0;
    do {
      int startReadIndex = in.readerIndex();
      c = parse(in);
      if (c == null) {
        in.readerIndex(startReadIndex);
        break;
      }
      bytesRead += in.readerIndex() - startReadIndex;
      out.add(c);
    } while (in.isReadable()); // Try to take advantage of pipelining if it is being used
    redisStats.incNetworkBytesRead(bytesRead);
  }

  private Command parse(ByteBuf buffer) throws RedisCommandParserException {
    if (buffer == null) {
      throw new NullPointerException();
    }
    if (!buffer.isReadable()) {
      return null;
    }

    byte firstB = buffer.readByte();
    if (firstB != ARRAY_ID) {
      throw new RedisCommandParserException(
          "Expected: " + (char) ARRAY_ID + " Actual: " + (char) firstB);
    }
    List<byte[]> commandElems = parseArray(buffer);

    if (commandElems == null) {
      return null;
    }

    return new Command(commandElems);
  }

  private List<byte[]> parseArray(ByteBuf buffer)
      throws RedisCommandParserException {
    byte currentChar;
    int arrayLength = parseCurrentNumber(buffer);
    if (arrayLength < 0 || !parseRN(buffer)) {
      return null;
    }

    if (arrayLength > UNAUTHENTICATED_MAX_ARRAY_SIZE
        && securityService.isEnabled() && !securityService.isAuthenticated(channelId)) {
      throw new RedisException(ERROR_UNAUTHENTICATED_MULTIBULK);
    }

    List<byte[]> commandElems = new ArrayList<>(arrayLength);

    for (int i = 0; i < arrayLength; i++) {
      if (!buffer.isReadable()) {
        return null;
      }
      currentChar = buffer.readByte();
      if (currentChar == BULK_STRING_ID) {
        byte[] newBulkString = parseBulkString(buffer);
        if (newBulkString == null) {
          return null;
        }
        commandElems.add(newBulkString);
      } else {
        throw new RedisCommandParserException(
            "expected: \'$\', got \'" + (char) currentChar + "\'");
      }
    }
    return commandElems;
  }

  /**
   * Helper method to parse a bulk string when one is seen
   *
   * @param buffer Buffer to read from
   * @return byte[] representation of the Bulk String read
   * @throws RedisCommandParserException Thrown when there is illegal syntax
   */
  private byte[] parseBulkString(ByteBuf buffer) throws RedisCommandParserException {
    int bulkStringLength = parseCurrentNumber(buffer);
    if (bulkStringLength < 0) {
      return null;
    }

    if (bulkStringLength > MAX_BULK_STRING_LENGTH) {
      throw new RedisCommandParserException(
          "invalid bulk length, cannot exceed max length of " + MAX_BULK_STRING_LENGTH);
    }

    if (bulkStringLength > UNAUTHENTICATED_MAX_BULK_STRING_LENGTH
        && securityService.isEnabled() && !securityService.isAuthenticated(channelId)) {
      throw new RedisException(ERROR_UNAUTHENTICATED_BULK);
    }

    if (!parseRN(buffer)) {
      return null;
    }

    if (!buffer.isReadable(bulkStringLength)) {
      return null;
    }
    byte[] bulkString = new byte[bulkStringLength];
    buffer.readBytes(bulkString);

    if (!parseRN(buffer)) {
      return null;
    }

    return bulkString;
  }

  /**
   * Helper method to parse the number at the beginning of the buffer
   *
   * @param buffer Buffer to read
   * @return The number found at the beginning of the buffer
   */
  private int parseCurrentNumber(ByteBuf buffer) {
    int number = 0;
    int readerIndex = buffer.readerIndex();
    byte b = 0;
    while (true) {
      if (!buffer.isReadable()) {
        return Integer.MIN_VALUE;
      }
      b = buffer.readByte();
      if (Character.isDigit(b)) {
        number = number * 10 + (int) (b - '0');
        readerIndex++;
      } else {
        buffer.readerIndex(readerIndex);
        break;
      }
    }
    return number;
  }

  /**
   * Helper method that is called when the next characters are supposed to be "\r\n"
   *
   * @param buffer Buffer to read from
   * @throws RedisCommandParserException Thrown when the next two characters are not "\r\n"
   */
  private boolean parseRN(ByteBuf buffer) throws RedisCommandParserException {
    if (!buffer.isReadable(2)) {
      return false;
    }
    byte[] bytes = {buffer.readByte(), buffer.readByte()};
    if (!Arrays.equals(bytes, bCRLF)) {
      throw new RedisCommandParserException(
          "expected \'\\r\\n\' as byte[] of " + Arrays.toString(bCRLF) + ", got byte[] of "
              + Arrays.toString(bytes));
    }
    return true;
  }
}
