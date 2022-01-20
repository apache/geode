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

package org.apache.geode.internal.tcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import javax.crypto.AEADBadTagException;
import javax.net.ssl.SSLException;

import org.apache.commons.io.HexDump;

public class BufferDebugging {

  public static final int INVERSE_PROBABILITY_OF_EXCEPTION = Integer.MAX_VALUE;

  private static class Offsets {
    private final int position;
    private final int limit;

    public Offsets(final ByteBuffer buff) {
      position = buff.position();
      limit = buff.limit();
    }

    private void reposition(final ByteBuffer buff) {
      buff.position(position);
      buff.limit(limit);
    }
  }

  private byte[] originalContent;

  private Offsets writeableOffsetsBeforeProcessing;
  private Offsets readableOffsetsAfterProcessing;

  private Offsets readableOffsetsBeforeProcessing;
  private Offsets writableOffsetsAfterProcessing;

  public static void throwTagMishmashSometimes() throws SSLException {
    if (INVERSE_PROBABILITY_OF_EXCEPTION != Integer.MAX_VALUE &&
        ThreadLocalRandom.current().nextInt(INVERSE_PROBABILITY_OF_EXCEPTION) == 0) {
      throw new SSLException("Tag mismatch! ", new AEADBadTagException("Tag mismatch!"));
    }
  }

  @FunctionalInterface
  public interface BufferProcessing {
    void process(ByteBuffer buff) throws IOException;
  }

  /*
   Buffer is writeable before processing. When an exception is thrown,
   the buffer is in a readable state.
   */
  public void doProcessingOnWriteableBuffer(
      final ByteBuffer buff,
      final BufferProcessing processing)
      throws IOException {
    beforeProcessingWriteableBuffer(buff);
    try {
      processing.process(buff);
    } catch (final IOException e) {
      onIOExceptionForReadableBuffer(buff);
      throw e;
    }
  }

  public void doProcessingOnReadableBuffer(
      final ByteBuffer buff,
      final BufferProcessing processing)
      throws IOException {
    beforeProcessingReadableBuffer(buff);
    try {
      processing.process(buff);
    } catch (final IOException e) {
      onIOExceptionForWritableBuffer(buff);
      throw e;
    }
  }

  /*
   buff is writeable on entry and exit
   */
  private void beforeProcessingWriteableBuffer(final ByteBuffer buff) {
    // lightweight capture of pre-processing offsets in case we see an exception later
    writeableOffsetsBeforeProcessing = new Offsets(buff);
  }

  private void beforeProcessingReadableBuffer(final ByteBuffer buff) {
    // lightweight capture of pre-processing offsets in case we see an exception later
    readableOffsetsBeforeProcessing = new Offsets(buff);
  }

  /*
   we encountered an exception so capture the buffer
   buff is readable on entry and exit
   */
  private void onIOExceptionForReadableBuffer(final ByteBuffer buff) {
    readableOffsetsAfterProcessing = new Offsets(buff);
    buff.position(0);
    buff.limit(writeableOffsetsBeforeProcessing.position);
    final int length = buff.limit() - buff.position();
    originalContent = new byte[length];
    buff.get(originalContent, 0, length);
    readableOffsetsAfterProcessing.reposition(buff);
  }

  private void onIOExceptionForWritableBuffer(final ByteBuffer buff) {
    writableOffsetsAfterProcessing = new Offsets(buff);
    readableOffsetsBeforeProcessing.reposition(buff);
    final int length = buff.limit() - buff.position();
    originalContent = new byte[length];
    buff.get(originalContent, 0, length);
    writableOffsetsAfterProcessing.reposition(buff);
  }

  public String dumpReadableBuffer() {
    String result = "";
    if (readableOffsetsAfterProcessing != null) {
      result += String.format("After processing, offsets (hex) are: position: %08x, limit: %08x\n",
          readableOffsetsAfterProcessing.position, readableOffsetsAfterProcessing.limit);
    }
    if (writeableOffsetsBeforeProcessing != null) {
      result += String.format("Before processing:\n%s\n", dump(originalContent,
          0, writeableOffsetsBeforeProcessing.position));
    }
    return result;
  }

  public String dumpWritableBuffer() {
    String result = "";
    if (writableOffsetsAfterProcessing != null) {
      result += String.format("After processing, offsets (hex) are: position: %08x, limit: %08x\n",
          writableOffsetsAfterProcessing.position, writableOffsetsAfterProcessing.limit);
    }
    if (readableOffsetsBeforeProcessing != null) {
      result += String.format("Before processing:\n%s\n", dump(originalContent,
          readableOffsetsBeforeProcessing.position, readableOffsetsBeforeProcessing.limit));
    }
    return result;
  }

  private String dump(final byte[] content, final int position, final int limit) {
    final byte[] slice = Arrays.copyOfRange(content, position, limit);
    final ByteArrayOutputStream formatted = new ByteArrayOutputStream();
    String result;
    try {
      HexDump.dump(slice, position, formatted,0);
      result = formatted.toString();
    } catch (IOException e) {
      result = String.format("(Failed to format content; position: %d, limit: %d)", position, limit);
    }
    return result;
  }
}
