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
import static org.apache.geode.internal.tcp.Connection.MSG_HEADER_BYTES;
import static org.apache.geode.internal.tcp.Connection.readHandshakeForReceiverFunction;
import static org.apache.geode.internal.tcp.Connection.readMessageHeaderFunction;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
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
import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.InternalDataSerializer;

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
    if (ALLOW_NULL_CIPHER) {
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

  private static final String s0 =
      "070000894cffff0007040a5405130000a0285700316c72742d7365727665722d302e6c72742d7365727665722e636c75737465722e7376632e636c75737465722e6c6f63616c090000bb15000000010a0057000c6c72742d7365727665722d30570001325700000000012cff009631d6d79b535266a64c1a3bac0904df0f00000100000000000005d3ff009600000001";
  private static final String s1 = "070000104cffff46000000000000ea6000000008ff0096";
  private static final String s2 =
      "070005126cffff015d188000005700102f5472616465735265706c69636174650100000000ff000cf66e0020788587a8dcb6ed2f0000017ed6d714070a1f040a54091e0000c256050d5700083632316262326436570006636c69656e740a020007b8af02000eeeeeffffffff000057000139fe043b5d0000043200e4a85b570001395700045342555800000003605700063834332e3339fe0400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000017ed6d714060000017ed6d714060019000f0004012662015c040a54091e0000c25657001a6c72742d636c69656e742d6664376263373437622d677071356c0900000000000000010d00570006636c69656e7457000836323162623264365700000000012cff0096000000000000000000000000000000000000000001";
  private final String s3 = "070000034cffff014800";

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    final Object deserializedGemFireObject = parseGemFireP2PProtocolMessage(s1);
    System.out.println("done");
  };

  /*
   * TODO:
   * implement readHandshakeForSender parsing
   */
  private static Object parseGemFireP2PProtocolMessage(final String gemFireP2PHexByteString) {
    final byte[] bytes = hexStringToByteArray(gemFireP2PHexByteString);

    final ByteBuffer unprocessed = ByteBuffer.wrap(bytes);
    final Connection.MessageHeaderParsing messageHeaderParsing =
        readMessageHeaderFunction(unprocessed, false);

    if (messageHeaderParsing.thrown == null) {
      System.out.println("Header parsed: " + messageHeaderParsing);
    } else {
      System.out.println("Exception while parsing message header: " + messageHeaderParsing.thrown);
      return null;
    }

    // make a new buffer to speculatively process any handshakes
    final ByteBuffer handshakeBuffer = createBufferAfterHeader(unprocessed);
    Connection.HandshakeForReceiverParsing handshakeForReceiverParsing = null;
    Object distributionMessage = null;

    try (ByteBufferInputStream bbis = new ByteBufferInputStream(handshakeBuffer);
        DataInputStream dis = new DataInputStream(bbis)) {
      handshakeForReceiverParsing =
          readHandshakeForReceiverFunction(dis);
    } catch (IOException e) {
      System.out.println("While trying to process bytes after header: " + e);
      return null;
    }

    final ByteBuffer distributionMessageBuffer;
    if (handshakeForReceiverParsing.thrown == null) {
      System.out.println("Handshake for receiver parsed: " + handshakeForReceiverParsing);
      // handshake was seen: keep processing bytes after handshake
      distributionMessageBuffer = handshakeBuffer;
    } else {
      // no handshake was seen: re-read bytes after header
      distributionMessageBuffer = createBufferAfterHeader(unprocessed);
    }

    if (distributionMessageBuffer.remaining() < 1) {
      return null;
    }

    try (ByteBufferInputStream bbis = new ByteBufferInputStream(distributionMessageBuffer);
        DataInputStream dis = new DataInputStream(bbis)) {
      distributionMessage = InternalDataSerializer.readDSFID(dis);
    } catch (IOException | ClassNotFoundException e) {
      System.out.println("While trying to process bytes after header: " + e);
      return null;
    }

    if (distributionMessage != null) {
      System.out.println("DistributionMessage parsed: " + distributionMessage);
    }
    return distributionMessage;
  }

  @NotNull
  private static ByteBuffer createBufferAfterHeader(final ByteBuffer unprocessed) {
    final ByteBuffer afterHeader = unprocessed.duplicate();
    afterHeader.position(MSG_HEADER_BYTES); // header parsing left position at 0
    return afterHeader;
  }

  /* s must be an even-length string. */
  private static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
          + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }
}
