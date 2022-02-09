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
import static org.apache.geode.internal.tcp.Connection.readMessageHeaderFunction;

import java.io.ByteArrayInputStream;
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

  private static final String sx =
      "070000894cffff0007040a5405130000a0285700316c72742d7365727665722d302e6c72742d7365727665722e636c75737465722e7376632e636c75737465722e6c6f63616c090000bb15000000010a0057000c6c72742d7365727665722d30570001325700000000012cff009631d6d79b535266a64c1a3bac0904df0f00000100000000000005ceff009600000002\n"
          + "070000314cffff01390500146c9a29010c2902085300010010ff0007fc310007fc31cea4dbb6ed2f6e7c9507c0f941b38235134919fea7e9\n"
          + "0700003a4cffff0200960100146e0e0100010010ff000b3ec1000b3ec18da9dbb6ed2f040a540a130000a028050a5700013457000c6c72742d7365727665722d32\n"
          + "0700002c4cffff02009601001491170300010010ff00080a4400080a44b9abdcb6ed2f6e7c9507c0f941b38235134919fea7e9\n"
          + "0700003e4cffff013905001492be29010129018800010010ff000b5612000b5612a6b0dcb6ed2f040a540a130000a028050a5700013457000c6c72742d7365727665722d32\n"
          + "0700003a4cffff02009601001492c10100010010ff000b5614000b5614a9b0dcb6ed2f040a540a130000a028050a5700013457000c6c72742d7365727665722d32\n"
          + "0700003a4cffff020096010014962b0100010010ff000b5555000b5555b3bcdcb6ed2f040a540a130000a028050a5700013457000c6c72742d7365727665722d32\n"
          + "070000314cffff01390500149d792901012902085300010010ff0008142500081425b8d3dcb6ed2f6e7c9507c0f941b38235134919fea7e9\n"
          + "0700003e4cffff0139050014a25a29010c29018800010010ff000b5f1a000b5f1af9e1dcb6ed2f040a540a130000a028050a5700013457000c6c72742d7365727665722d32\n"
          + "070000314cffff0139050014a3522901012902085300010010ff0008162a0008162aafe5dcb6ed2f6e7c9507c0f941b38235134919fea7e9\n"
          + "070000314cffff0139050014a57929010c2902085300010010ff0008128c0008128ce4ecdcb6ed2f6e7c9507c0f941b38235134919fea7e9\n";

  private static final String s1 =
      "070000894cffff0007040a5405130000a0285700316c72742d7365727665722d302e6c72742d7365727665722e636c75737465722e7376632e636c75737465722e6c6f63616c090000bb15000000010a0057000c6c72742d7365727665722d30570001325700000000012cff009631d6d79b535266a64c1a3bac0904df0f00000100000000000005ceff009600000002";

  private static final String s =
      "070000894cffff0007040a5405130000a0285700316c72742d7365727665722d302e6c72742d7365727665722e636c75737465722e7376632e636c75737465722e6c6f63616c090000bb15000000010a0057000c6c72742d7365727665722d30570001325700000000012cff009631d6d79b535266a64c1a3bac0904df0f00000100000000000005ceff009600000002";

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    final Object deserializedGemFireObject = deserialize(s);
    System.out.println("I see: " + deserializedGemFireObject);
  };

  private static Object deserialize(final String gemFireP2PHexByteString)
      throws IOException, ClassNotFoundException {
    final byte[] bytes = hexStringToByteArray(gemFireP2PHexByteString);

    final ByteBuffer wrap = ByteBuffer.wrap(bytes);
    final Connection.MessageHeaderParsing messageHeaderParsing =
        readMessageHeaderFunction(wrap, false);

    final DataInputStream dataInputStream = new DataInputStream(
        new ByteArrayInputStream(bytes, MSG_HEADER_BYTES, bytes.length - MSG_HEADER_BYTES));
    return InternalDataSerializer.readDSFID(dataInputStream);
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
