package com.gemstone.gemfire.internal.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;


public class ByteToCommandDecoder extends ByteToMessageDecoder {


  private static final byte rID = 13; // '\r';
  private static final byte nID = 10; // '\n';
  private static final byte bulkStringID = 36; // '$';
  private static final byte arrayID = 42; // '*';
  private static final int MAX_BULK_STRING_LENGTH = 512 * 1024 * 1024; // 512 MB
  
  public ByteToCommandDecoder() {
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    Command c = null;
    do {
      in.markReaderIndex();
      c = parse(in);
      if (c == null) {
        in.resetReaderIndex();
        return;
      }
      out.add(c);
    } while (in.isReadable()); // Try to take advantage of pipelining if it is being used
  }

  /**
   * The only public method for CommandParser is parse. It will take a buffer
   * and break up the individual pieces into a list is char[] for the caller
   * based on the Redis protocol.
   * 
   * @param buffer The buffer to read the command from
   * @return A new {@link Command} object
   * @throws RedisCommandParserException Thrown when the command has illegal syntax
   * @throws BufferUnderflowException Thrown when the parser runs out of chars
   * to read when it still expects chars to remain in the command
   */
  public static Command parse(ByteBuf buffer) throws RedisCommandParserException {
    if (buffer == null)
      throw new NullPointerException();
    if (!buffer.isReadable())
      return null;

    byte firstB = buffer.readByte();
    if (firstB != arrayID)
      throw new RedisCommandParserException("Expected: " + (char) arrayID + " Actual: " + (char) firstB);
    ArrayList<byte[]> commandElems = new ArrayList<byte[]>();

    if (!parseArray(commandElems, buffer))
      return null;

    return new Command(commandElems);
  }

  /**
   * Helper method to parse the array which contains the Redis command
   * 
   * @param commandElems The list to add the elements of the command to
   * @param buffer The buffer to read from
   * @throws RedisCommandParserException Thrown when command contains illegal syntax
   */
  private static boolean parseArray(ArrayList<byte[]> commandElems, ByteBuf buffer) throws RedisCommandParserException { 
    byte currentChar;
    int arrayLength = parseCurrentNumber(buffer);
    if (arrayLength == Integer.MIN_VALUE || !parseRN(buffer))
      return false;
    if (arrayLength < 0 || arrayLength > 1000000000)
      throw new RedisCommandParserException("invalid multibulk length");

    for (int i = 0; i < arrayLength; i++) {
      if (!buffer.isReadable())
        return false;
      currentChar = buffer.readByte();
      if (currentChar == bulkStringID) {
        byte[] newBulkString = parseBulkString(buffer);
        if (newBulkString == null)
          return false;
        commandElems.add(newBulkString);
      } else
        throw new RedisCommandParserException("expected: \'$\', got \'" + (char) currentChar + "\'");
    }
    return true;
  }

  /**
   * Helper method to parse a bulk string when one is seen
   * 
   * @param buffer Buffer to read from
   * @return byte[] representation of the Bulk String read
   * @throws RedisCommandParserException Thrown when there is illegal syntax
   */
  private static byte[] parseBulkString(ByteBuf buffer) throws RedisCommandParserException {
    int bulkStringLength = parseCurrentNumber(buffer);
    if (bulkStringLength == Integer.MIN_VALUE)
      return null;
    if (bulkStringLength > MAX_BULK_STRING_LENGTH)
      throw new RedisCommandParserException("invalid bulk length, cannot exceed max length of " + MAX_BULK_STRING_LENGTH);
    if (!parseRN(buffer))
      return null;

    if (!buffer.isReadable(bulkStringLength))
      return null;
    byte[] bulkString = new byte[bulkStringLength];
    buffer.readBytes(bulkString);

    if (!parseRN(buffer))
      return null;

    return bulkString;
  }

  /**
   * Helper method to parse the number at the beginning of the buffer
   * 
   * @param buffer Buffer to read
   * @return The number found at the beginning of the buffer
   */
  private static int parseCurrentNumber(ByteBuf buffer) {
    int number = 0;
    int readerIndex = buffer.readerIndex();
    byte b = 0;
    while (true) {
      if (!buffer.isReadable())
        return Integer.MIN_VALUE;
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
   * Helper method that is called when the next characters are 
   * supposed to be "\r\n"
   * 
   * @param buffer Buffer to read from
   * @throws RedisCommandParserException Thrown when the next two characters
   * are not "\r\n"
   */
  private static boolean parseRN(ByteBuf buffer) throws RedisCommandParserException {
    if (!buffer.isReadable(2))
      return false;
    byte b = buffer.readByte();
    if (b != rID)
      throw new RedisCommandParserException("expected \'" + (char) rID + "\', got \'" + (char) b + "\'");
    b = buffer.readByte();
    if (b != nID)
      throw new RedisCommandParserException("expected: \'" + (char) nID + "\', got \'" + (char) b + "\'");
    return true;
  }

}
