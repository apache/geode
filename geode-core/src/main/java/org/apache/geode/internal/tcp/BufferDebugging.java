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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import javax.crypto.AEADBadTagException;
import javax.net.ssl.SSLException;

import org.apache.commons.io.HexDump;

public class BufferDebugging {

  private static final boolean SIMULATE_TAG_MISMATCH = false;
  private static final boolean ALLOW_NULL_CIPHER = false;

  private static final int NEVER_THROW = Integer.MAX_VALUE;

  /*
   * Set to Integer.MAX_VALUE to make throwTagMishmashSometimes() a no-op.
   * Set to value greater than zero to determine how often that method throws
   * its exception. 1_000 is a good, empirically-derived value for testing.
   */
  private static final int INVERSE_PROBABILITY_OF_EXCEPTION =
      SIMULATE_TAG_MISMATCH ? 1_000 : NEVER_THROW;

  /*
   * TODO: eliminate hard-coded absolute path here. As it stands it only works on
   * bburcham laptop.
   */
  private static final String SECURITY_PROPERTIES_OVERRIDE_JVM_ARG =
      "-Djava.security.properties=/Users/bburcham/Projects/geode/geode-core/src/distributedTest/resources/org/apache/geode/distributed/internal/java.security";

  public static void overrideJVMSecurityPropertiesFile(final ArrayList<String> cmds) {
    if (ALLOW_NULL_CIPHER) {
      cmds.add(SECURITY_PROPERTIES_OVERRIDE_JVM_ARG);
    }
  }

  private static class Offsets {

    /*
     * TODO: it might be nice to capture the mark here too but there is no straightforward
     * way to do that through the ByteBuffer public interface.
     */

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

  /*
   * TODO: as of the time of this writing, we are capturing the buffer content only
   * after processing. Buffer mutation during processing e.g. ByteBuffer.compact()
   * will affect what we log. If we eventually need to see the buffer as it existed
   * before processing we'd have to capture it ahead of time (unconditionally). We
   * might be able to do this with acceptable performance by capturing the direct
   * buffer to another direct buffer. Then we could copy the bytes out to heap only
   * if they were actually (eventually) needed for logging.
   */
  private byte[] bufferContent;

  private Offsets readableOffsetsBeforeProcessing;
  private Offsets writeableOffsetsBeforeProcessing;
  private Offsets readableOffsetsAfterProcessing;

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
   * Buffer is writeable before processing. When an exception is thrown,
   * the buffer is in a readable state.
   */
  public void doProcessingForReceiver(
      final ByteBuffer buff,
      final BufferProcessing processing)
      throws IOException {
    beforeProcessingForReceiver(buff);
    try {
      processing.process(buff);
    } catch (final IOException e) {
      onIOExceptionForReceiver(buff);
      throw e;
    }
  }

  /*
   * Buffer is readable before processing. When an exception is thrown,
   * the buffer is still in a readable state. Contrast with doProcessingForReceiver().
   */
  public void doProcessingForSender(
      final ByteBuffer buff,
      final BufferProcessing processing)
      throws IOException {
    beforeProcessingForSender(buff);
    try {
      processing.process(buff);
    } catch (final IOException e) {
      onIOExceptionForSender(buff);
      throw e;
    }
  }

  public String dumpBufferForReceiver() {
    String result = postProcessingMessage();
    if (writeableOffsetsBeforeProcessing != null) {
      result += preProcessingMessage(writeableOffsetsBeforeProcessing);
      result += String.format("Current buffer array from 0 to pre-processing position:\n%s\n",
          dump(bufferContent, 0, writeableOffsetsBeforeProcessing.position));
    }
    return result;
  }

  public String dumpBufferForSender() {
    String result = postProcessingMessage();
    if (readableOffsetsBeforeProcessing != null) {
      result += preProcessingMessage(readableOffsetsBeforeProcessing);
      result += String.format("Current buffer array from 0 to pre-processing limit:\n%s\n",
          dump(bufferContent, 0, readableOffsetsBeforeProcessing.limit));
    }
    return result;
  }

  private String preProcessingMessage(final Offsets offsets) {
    return String.format("Before processing, offsets (hex) were: position: %08x, limit: %08x\n",
        offsets.position, offsets.limit);
  }

  private String postProcessingMessage() {
    if (readableOffsetsAfterProcessing != null) {
      return String.format("After processing, offsets (hex) are: position: %08x, limit: %08x\n",
          readableOffsetsAfterProcessing.position, readableOffsetsAfterProcessing.limit);
    } else {
      return "";
    }
  }

  /*
   * buff is writeable on entry and exit
   */
  private void beforeProcessingForReceiver(final ByteBuffer buff) {
    // lightweight capture of pre-processing offsets in case we see an exception later
    writeableOffsetsBeforeProcessing = new Offsets(buff);
  }

  /*
   * buff is readable on entry and exit
   */
  private void beforeProcessingForSender(final ByteBuffer buff) {
    // lightweight capture of pre-processing offsets in case we see an exception later
    readableOffsetsBeforeProcessing = new Offsets(buff);
  }

  /*
   * we encountered an exception in receiver processing so capture the buffer
   * buff is readable on entry and exit
   */
  private void onIOExceptionForReceiver(final ByteBuffer buff) {
    readableOffsetsAfterProcessing = new Offsets(buff);
    buff.position(0);
    buff.limit(writeableOffsetsBeforeProcessing.position);
    final int length = buff.remaining();
    bufferContent = new byte[length];
    buff.get(bufferContent, 0, length);
    readableOffsetsAfterProcessing.reposition(buff);
  }

  /*
   * we encountered an exception in sender processing so capture the buffer
   * buff is readable on entry and exit
   *
   * this method differs from onIOExceptionForReceiver() since in this case
   * we've captured readableOffsetsBeforeProcessing since, unlike in the
   * receiver case, in this case, the buffer was readable before processing
   */
  private void onIOExceptionForSender(final ByteBuffer buff) {
    readableOffsetsAfterProcessing = new Offsets(buff);
    readableOffsetsBeforeProcessing.reposition(buff);
    final int length = buff.remaining();
    bufferContent = new byte[length];
    buff.get(bufferContent, 0, length);
    readableOffsetsAfterProcessing.reposition(buff);
  }

  private String dump(final byte[] content, final int position, final int limit) {
    final byte[] slice = Arrays.copyOfRange(content, position, limit);
    final ByteArrayOutputStream formatted = new ByteArrayOutputStream();
    String result;
    try {
      HexDump.dump(slice, position, formatted, 0);
      result = formatted.toString();
    } catch (IOException e) {
      result =
          String.format("(Failed to format content; position: %d, limit: %d)", position, limit);
    }
    return result;
  }
}
