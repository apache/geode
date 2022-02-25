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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.crypto.AEADBadTagException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocketFactory;

import org.apache.commons.io.HexDump;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class BufferDebugging {

  private static final Logger logger = LogService.getLogger();

  private static final boolean SIMULATE_TAG_MISMATCH_IN_GEODE_PRODUCT = false;
  private static final boolean USE_NULL_CIPHER_IN_DUNIT_TESTS = true;

  public static final int SENDER_BUFFER_LOGGING_CAPACITY = 16 * 1024; // bytes

  private static final int NEVER_THROW = Integer.MAX_VALUE;

  /*
   * Set to Integer.MAX_VALUE to make throwTagMishmashSometimes() a no-op.
   * Set to value greater than zero to determine how often that method throws
   * its exception. 1_000 is a good, empirically-derived value for testing.
   */
  private static final int INVERSE_PROBABILITY_OF_EXCEPTION =
      SIMULATE_TAG_MISMATCH_IN_GEODE_PRODUCT ? 1_000 : NEVER_THROW;

  /*
   * TODO: eliminate hard-coded absolute path here. As it stands it only works on
   * bburcham laptop.
   */
  private static final String SECURITY_PROPERTIES_OVERRIDE_JVM_ARG =
      "-Djava.security.properties=/Users/bburcham/Projects/geode/geode-core/src/distributedTest/resources/org/apache/geode/distributed/internal/java.security";

  public static void overrideJVMSecurityPropertiesFile(final ArrayList<String> cmds) {
    if (USE_NULL_CIPHER_IN_DUNIT_TESTS) {
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

  public String dumpBufferForReceiver() {
    String result = postProcessingMessage();
    if (writeableOffsetsBeforeProcessing != null) {
      result += preProcessingMessage(writeableOffsetsBeforeProcessing);
      result += String.format("Current buffer array from 0 to pre-processing position:\n%s\n",
          dump(bufferContent, 0, writeableOffsetsBeforeProcessing.position));
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

  private static String dump(final byte[] content, final int position, final int limit) {
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

  public static void setCipher(final Properties securityProperties) {
    /*
     * Null ciphers:
     * SSL_RSA_WITH_NULL_MD5
     * SSL_RSA_WITH_NULL_SHA
     * TLS_ECDHE_ECDSA_WITH_NULL_SHA
     * TLS_ECDHE_RSA_WITH_NULL_SHA
     * TLS_ECDH_ECDSA_WITH_NULL_SHA
     * TLS_ECDH_RSA_WITH_NULL_SHA
     * TLS_RSA_WITH_NULL_SHA256
     */
    if (USE_NULL_CIPHER_IN_DUNIT_TESTS) {
      securityProperties.setProperty(SSL_CIPHERS, "SSL_RSA_WITH_NULL_MD5");
    }
  }

  /*
   * Run this in a JVM to see what ciphers are available.
   */
  public static void listCiphers()
      throws Exception {
    SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();

    String[] defaultCiphers = ssf.getDefaultCipherSuites();
    String[] availableCiphers = ssf.getSupportedCipherSuites();

    TreeMap ciphers = new TreeMap();

    for (int i = 0; i < availableCiphers.length; ++i)
      ciphers.put(availableCiphers[i], Boolean.FALSE);

    for (int i = 0; i < defaultCiphers.length; ++i)
      ciphers.put(defaultCiphers[i], Boolean.TRUE);

    System.out.println("Default\tCipher");
    for (Iterator i = ciphers.entrySet().iterator(); i.hasNext();) {
      Map.Entry cipher = (Map.Entry) i.next();

      if (Boolean.TRUE.equals(cipher.getValue()))
        System.out.print('*');
      else
        System.out.print(' ');

      System.out.print('\t');
      System.out.println(cipher.getKey());
    }
  }

  @FunctionalInterface
  public interface BufferWriting {
    int write(ByteBuffer buff) throws IOException;
  }

  @FunctionalInterface
  public interface SenderProcessing {
    void process() throws IOException;
  }

  public static class SenderDebugging {

    private final BufferDebuggingCircularBuffer circularBuffer = new BufferDebuggingCircularBuffer(
        SENDER_BUFFER_LOGGING_CAPACITY);

    public void doProcessingForSender(final Connection connection,
        final SenderProcessing processing) throws IOException {
      try {
        processing.process();
      } catch (final IOException e) {
        final int bytesAvailable = circularBuffer.bytesAvailableForReading();
        final byte[] bytes = new byte[bytesAvailable];
        circularBuffer.get(bytes, 0, bytesAvailable);
        logger.info("Sender (for connection {}) caught IO exception", connection, e);
        logger.info("Sender (for connection {}) recently sent\n{}",
            connection, dump(bytes, 0, bytes.length));
        throw e;
      }
    }

    public int doWriteForSender(
        final ByteBuffer readableEncodedBuffer,
        final BufferWriting writing)
        throws IOException {
      final int bytesWritten = writing.write(readableEncodedBuffer);
      afterWritingForSender(bytesWritten, readableEncodedBuffer);
      return bytesWritten;
    }

    /*
     * We just wrote to the network (channel). Bytes came from readableBuffer. So its position is
     * one past the last byte written to the network.
     */
    private void afterWritingForSender(final int bytesWritten, final ByteBuffer readableBuffer) {
      final ByteBuffer justWritten = readableBuffer.duplicate();
      final int nextReadPosition = justWritten.position();
      justWritten.position(nextReadPosition - bytesWritten).limit(nextReadPosition);
      circularBuffer.put(justWritten);
    }

  }
}
