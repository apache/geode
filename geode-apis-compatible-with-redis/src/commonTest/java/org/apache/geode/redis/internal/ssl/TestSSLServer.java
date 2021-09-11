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

package org.apache.geode.redis.internal.ssl;

/*
 * Command-line tool to test a SSL/TLS server for some vulnerabilities.
 * =====================================================================
 *
 * This application connects to the provided SSL/TLS server (by name and
 * port) and extracts the following information:
 * - supported versions (SSL 2.0, SSL 3.0, TLS 1.0 to 1.2)
 * - support of Deflate compression
 * - list of supported cipher suites (for each protocol version)
 * - BEAST/CRIME vulnerabilities.
 *
 * BEAST and CRIME are client-side attack, but the server can protect the
 * client by refusing to use the feature combinations which can be
 * attacked. For CRIME, the weakness is Deflate compression. For BEAST,
 * the attack conditions are more complex: it works with CBC ciphers with
 * SSL 3.0 and TLS 1.0. Hence, a server fails to protect the client against
 * BEAST if it does not enforce usage of RC4 over CBC ciphers under these
 * protocol versions, if given the choice.
 *
 * (The BEAST test considers only the cipher suites with strong
 * encryption; if the server supports none, then there are bigger
 * problems. We also assume that all clients support RC4-128; thus, the
 * server protects the client if it selects RC4-128 even if some strong
 * CBC-based ciphers are announced as supported by the client with a
 * higher preference level.)
 *
 * ----------------------------------------------------------------------
 * Copyright (c) 2012  Thomas Pornin <pornin@bolet.org>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ----------------------------------------------------------------------
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TestSSLServer {

  static void usage() {
    System.err.println("usage: TestSSLServer servername [ port ]");
    System.exit(1);
  }

  public static void main(String[] args)
      throws IOException {
    if (args.length == 0 || args.length > 2) {
      usage();
    }
    String name = args[0];
    int port = 443;
    if (args.length == 2) {
      try {
        port = Integer.parseInt(args[1]);
      } catch (NumberFormatException nfe) {
        usage();
      }
      if (port <= 0 || port > 65535) {
        usage();
      }
    }
    InetSocketAddress isa = new InetSocketAddress(name, port);

    Set<Integer> sv = new TreeSet<Integer>();
    boolean compress = false;
    for (int v = 0x0300; v <= 0x0303; v++) {
      ServerHello sh = connect(isa, v, CIPHER_SUITES.keySet());
      if (sh == null) {
        continue;
      }
      sv.add(sh.protoVersion);
      if (sh.compression == 1) {
        compress = true;
      }
    }

    ServerHelloSSLv2 sh2 = connectV2(isa);

    if (sh2 != null) {
      sv.add(0x0200);
    }

    if (sv.size() == 0) {
      System.out.println("No SSL/TLS server at " + isa);
      System.exit(1);
    }
    System.out.print("Supported versions:");
    for (int v : sv) {
      System.out.print(" ");
      System.out.print(versionString(v));
    }
    System.out.println();
    System.out.println("Deflate compression: "
        + (compress ? "YES" : "no"));

    System.out.println("Supported cipher suites"
        + " (ORDER IS NOT SIGNIFICANT):");
    Set<Integer> lastSuppCS = null;
    Map<Integer, Set<Integer>> suppCS =
        new TreeMap<Integer, Set<Integer>>();
    Set<String> certID = new TreeSet<String>();

    if (sh2 != null) {
      System.out.println("  " + versionString(0x0200));
      Set<Integer> vc2 = new TreeSet<Integer>();
      for (int c : sh2.cipherSuites) {
        vc2.add(c);
      }
      for (int c : vc2) {
        System.out.println("     "
            + cipherSuiteStringV2(c));
      }
      suppCS.put(0x0200, vc2);
      if (sh2.serverCertName != null) {
        certID.add(sh2.serverCertHash
            + ": " + sh2.serverCertName);
      }
    }

    for (int v : sv) {
      if (v == 0x0200) {
        continue;
      }
      Set<Integer> vsc = supportedSuites(isa, v, certID);
      suppCS.put(v, vsc);
      if (lastSuppCS == null || !lastSuppCS.equals(vsc)) {
        System.out.println("  " + versionString(v));
        for (int c : vsc) {
          System.out.println("     "
              + cipherSuiteString(c));
        }
        lastSuppCS = vsc;
      } else {
        System.out.println("  (" + versionString(v)
            + ": idem)");
      }
    }
    System.out.println("----------------------");
    if (certID.size() == 0) {
      System.out.println("No server certificate !");
    } else {
      System.out.println("Server certificate(s):");
      for (String cc : certID) {
        System.out.println("  " + cc);
      }
    }
    System.out.println("----------------------");
    int agMaxStrength = STRONG;
    int agMinStrength = STRONG;
    boolean vulnBEAST = false;
    for (int v : sv) {
      Set<Integer> vsc = suppCS.get(v);
      agMaxStrength = Math.min(
          maxStrength(vsc), agMaxStrength);
      agMinStrength = Math.min(
          minStrength(vsc), agMinStrength);
      if (!vulnBEAST) {
        vulnBEAST = testBEAST(isa, v, vsc);
      }
    }
    System.out.println("Minimal encryption strength:     "
        + strengthString(agMinStrength));
    System.out.println("Achievable encryption strength:  "
        + strengthString(agMaxStrength));
    System.out.println("BEAST status: "
        + (vulnBEAST ? "vulnerable" : "protected"));
    System.out.println("CRIME status: "
        + (compress ? "vulnerable" : "protected"));

  }

  /*
   * Get cipher suites supported by the server. This is done by
   * repeatedly contacting the server, each time removing from our
   * list of supported suites the suite which the server just
   * selected. We keep on until the server can no longer respond
   * to us with a ServerHello.
   */
  static Set<Integer> supportedSuites(InetSocketAddress isa, int version,
                                      Set<String> serverCertID) {
    Set<Integer> cs = new TreeSet<Integer>(CIPHER_SUITES.keySet());
    Set<Integer> rs = new TreeSet<Integer>();
    for (; ; ) {
      ServerHello sh = connect(isa, version, cs);
      if (sh == null) {
        break;
      }
      if (!cs.contains(sh.cipherSuite)) {
        System.err.printf("[ERR: server wants to use"
            + " cipher suite 0x%04X which client"
            + " did not announce]", sh.cipherSuite);
        System.err.println();
        break;
      }
      cs.remove(sh.cipherSuite);
      rs.add(sh.cipherSuite);
      if (sh.serverCertName != null) {
        serverCertID.add(sh.serverCertHash
            + ": " + sh.serverCertName);
      }
    }
    return rs;
  }

  static int minStrength(Set<Integer> supp) {
    int m = STRONG;
    for (int suite : supp) {
      CipherSuite cs = CIPHER_SUITES.get(suite);
      if (cs == null) {
        continue;
      }
      if (cs.strength < m) {
        m = cs.strength;
      }
    }
    return m;
  }

  static int maxStrength(Set<Integer> supp) {
    int m = CLEAR;
    for (int suite : supp) {
      CipherSuite cs = CIPHER_SUITES.get(suite);
      if (cs == null) {
        continue;
      }
      if (cs.strength > m) {
        m = cs.strength;
      }
    }
    return m;
  }

  static boolean testBEAST(InetSocketAddress isa,
                           int version, Set<Integer> supp) {
    /*
     * TLS 1.1+ is not vulnerable to BEAST.
     * We do not test SSLv2 either.
     */
    if (version < 0x0300 || version > 0x0301) {
      return false;
    }

    /*
     * BEAST attack works if the server allows the client to
     * use a CBC cipher. Existing clients also supports RC4,
     * so we consider that a server protects the clients if
     * it chooses RC4 over CBC streams when given the choice.
     * We only consider strong cipher suites here.
     */
    List<Integer> strongCBC = new ArrayList<Integer>();
    List<Integer> strongStream = new ArrayList<Integer>();
    for (int suite : supp) {
      CipherSuite cs = CIPHER_SUITES.get(suite);
      if (cs == null) {
        continue;
      }
      if (cs.strength < STRONG) {
        continue;
      }
      if (cs.isCBC) {
        strongCBC.add(suite);
      } else {
        strongStream.add(suite);
      }
    }
    if (strongCBC.size() == 0) {
      return false;
    }
    if (strongStream.size() == 0) {
      return true;
    }
    List<Integer> ns = new ArrayList<Integer>(strongCBC);
    ns.addAll(strongStream);
    ServerHello sh = connect(isa, version, ns);
    return !strongStream.contains(sh.cipherSuite);
  }

  static String versionString(int version) {
    if (version == 0x0200) {
      return "SSLv2";
    } else if (version == 0x0300) {
      return "SSLv3";
    } else if ((version >>> 8) == 0x03) {
      return "TLSv1." + ((version & 0xFF) - 1);
    } else {
      return String.format("UNKNOWN_VERSION:0x%04X", version);
    }
  }

  /*
   * Connect to the server, send a ClientHello, and decode the
   * response (ServerHello). On error, null is returned.
   */
  static ServerHello connect(InetSocketAddress isa,
                             int version, Collection<Integer> cipherSuites) {
    Socket s = null;
    try {
      s = new Socket();
      try {
        s.connect(isa);
      } catch (IOException ioe) {
        System.err.println("could not connect to "
            + isa + ": " + ioe.toString());
        return null;
      }
      byte[] ch = makeClientHello(version, cipherSuites);
      OutputRecord orec = new OutputRecord(
          s.getOutputStream());
      orec.setType(HANDSHAKE);
      orec.setVersion(version);
      orec.write(ch);
      orec.flush();
      return new ServerHello(s.getInputStream());
    } catch (IOException ioe) {
      // ignored
    } finally {
      try {
        s.close();
      } catch (IOException ioe) {
        // ignored
      }
    }
    return null;
  }

  /*
   * Connect to the server, send a SSLv2 CLIENT HELLO, and decode
   * the response (SERVER HELLO). On error, null is returned.
   */
  static ServerHelloSSLv2 connectV2(InetSocketAddress isa) {
    Socket s = null;
    try {
      s = new Socket();
      try {
        s.connect(isa);
      } catch (IOException ioe) {
        System.err.println("could not connect to "
            + isa + ": " + ioe.toString());
        return null;
      }
      s.getOutputStream().write(SSL2_CLIENT_HELLO);
      return new ServerHelloSSLv2(s.getInputStream());
    } catch (IOException ioe) {
      // ignored
    } finally {
      try {
        s.close();
      } catch (IOException ioe) {
        // ignored
      }
    }
    return null;
  }

  static final void enc16be(int val, byte[] buf, int off) {
    buf[off] = (byte) (val >>> 8);
    buf[off + 1] = (byte) val;
  }

  static final void enc24be(int val, byte[] buf, int off) {
    buf[off] = (byte) (val >>> 16);
    buf[off + 1] = (byte) (val >>> 8);
    buf[off + 2] = (byte) val;
  }

  static final void enc32be(int val, byte[] buf, int off) {
    buf[off] = (byte) (val >>> 24);
    buf[off + 1] = (byte) (val >>> 16);
    buf[off + 2] = (byte) (val >>> 8);
    buf[off + 3] = (byte) val;
  }

  static final int dec16be(byte[] buf, int off) {
    return ((buf[off] & 0xFF) << 8)
        | (buf[off + 1] & 0xFF);
  }

  static final int dec24be(byte[] buf, int off) {
    return ((buf[off] & 0xFF) << 16)
        | ((buf[off + 1] & 0xFF) << 8)
        | (buf[off + 2] & 0xFF);
  }

  static final int dec32be(byte[] buf, int off) {
    return ((buf[off] & 0xFF) << 24)
        | ((buf[off + 1] & 0xFF) << 16)
        | ((buf[off + 2] & 0xFF) << 8)
        | (buf[off + 3] & 0xFF);
  }

  static final int CHANGE_CIPHER_SPEC = 20;
  static final int ALERT = 21;
  static final int HANDSHAKE = 22;
  static final int APPLICATION = 23;

  static final int MAX_RECORD_LEN = 16384;

  /*
   * A custom stream which encodes data bytes into SSL/TLS records
   * (no encryption).
   */
  static class OutputRecord extends OutputStream {

    private OutputStream out;
    private byte[] buffer = new byte[MAX_RECORD_LEN + 5];
    private int ptr;
    private int version;
    private int type;

    OutputRecord(OutputStream out) {
      this.out = out;
      ptr = 5;
    }

    void setType(int type) {
      this.type = type;
    }

    void setVersion(int version) {
      this.version = version;
    }

    public void flush()
        throws IOException {
      buffer[0] = (byte) type;
      enc16be(version, buffer, 1);
      enc16be(ptr - 5, buffer, 3);
      out.write(buffer, 0, ptr);
      out.flush();
      ptr = 5;
    }

    public void write(int b)
        throws IOException {
      buffer[ptr++] = (byte) b;
      if (ptr == buffer.length) {
        flush();
      }
    }

    public void write(byte[] buf, int off, int len)
        throws IOException {
      while (len > 0) {
        int clen = Math.min(buffer.length - ptr, len);
        System.arraycopy(buf, off, buffer, ptr, clen);
        ptr += clen;
        off += clen;
        len -= clen;
        if (ptr == buffer.length) {
          flush();
        }
      }
    }
  }

  static void readFully(InputStream in, byte[] buf)
      throws IOException {
    readFully(in, buf, 0, buf.length);
  }

  static void readFully(InputStream in, byte[] buf, int off, int len)
      throws IOException {
    while (len > 0) {
      int rlen = in.read(buf, off, len);
      if (rlen < 0) {
        throw new EOFException();
      }
      off += rlen;
      len -= rlen;
    }
  }

  /*
   * A custom stream which expects SSL/TLS records (no encryption)
   * and rebuilds the encoded data stream. Incoming records MUST
   * have the expected type (e.g. "handshake"); alert messages
   * are skipped.
   */
  static class InputRecord extends InputStream {

    private InputStream in;
    private byte[] buffer = new byte[MAX_RECORD_LEN + 5];
    private int ptr, end;
    private int version;
    private int type;
    private int expectedType;

    InputRecord(InputStream in) {
      this.in = in;
      ptr = 0;
      end = 0;
    }

    void setExpectedType(int expectedType) {
      this.expectedType = expectedType;
    }

    int getVersion() {
      return version;
    }

    private void refill()
        throws IOException {
      for (; ; ) {
        readFully(in, buffer, 0, 5);
        type = buffer[0] & 0xFF;
        version = dec16be(buffer, 1);
        end = dec16be(buffer, 3);
        readFully(in, buffer, 0, end);
        ptr = 0;
        if (type != expectedType) {
          if (type == ALERT) {
            /*
             * We just ignore alert
             * messages.
             */
            continue;
          }
          throw new IOException(
              "unexpected record type: "
                  + type);
        }
        return;
      }
    }

    public int read()
        throws IOException {
      while (ptr == end) {
        refill();
      }
      return buffer[ptr++] & 0xFF;
    }

    public int read(byte[] buf, int off, int len)
        throws IOException {
      while (ptr == end) {
        refill();
      }
      int clen = Math.min(end - ptr, len);
      System.arraycopy(buffer, ptr, buf, off, clen);
      ptr += clen;
      return clen;
    }
  }

  private static final SecureRandom RNG = new SecureRandom();

  /*
   * Build a ClientHello message, with the specified maximum
   * supported version, and list of cipher suites.
   */
  static byte[] makeClientHello(int version,
                                Collection<Integer> cipherSuites) {
    try {
      return makeClientHello0(version, cipherSuites);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  static byte[] makeClientHello0(int version,
                                 Collection<Integer> cipherSuites)
      throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();

    /*
     * Message header:
     *   message type: one byte (1 = "ClientHello")
     *   message length: three bytes (this will be adjusted
     *   at the end of this method).
     */
    b.write(1);
    b.write(0);
    b.write(0);
    b.write(0);

    /*
     * The maximum version that we intend to support.
     */
    b.write(version >>> 8);
    b.write(version);

    /*
     * The client random has length 32 bytes, but begins with
     * the client's notion of the current time, over 32 bits
     * (seconds since 1970/01/01 00:00:00 UTC, not counting
     * leap seconds).
     */
    byte[] rand = new byte[32];
    RNG.nextBytes(rand);
    enc32be((int) (System.currentTimeMillis() / 1000), rand, 0);
    b.write(rand);

    /*
     * We send an empty session ID.
     */
    b.write(0);

    /*
     * The list of cipher suites (list of 16-bit values; the
     * list length in bytes is written first).
     */
    int num = cipherSuites.size();
    byte[] cs = new byte[2 + num * 2];
    enc16be(num * 2, cs, 0);
    int j = 2;
    for (int s : cipherSuites) {
      enc16be(s, cs, j);
      j += 2;
    }
    b.write(cs);

    /*
     * Compression methods: we claim to support Deflate (1)
     * and the standard no-compression (0), with Deflate
     * being preferred.
     */
    b.write(2);
    b.write(1);
    b.write(0);

    /*
     * If we had extensions to add, they would go here.
     */

    /*
     * We now get the message as a blob. The message length
     * must be adjusted in the header.
     */
    byte[] msg = b.toByteArray();
    enc24be(msg.length - 4, msg, 1);
    return msg;
  }

  /*
   * Compute the SHA-1 hash of some bytes, returning the hash
   * value in hexadecimal.
   */
  static String doSHA1(byte[] buf) {
    return doSHA1(buf, 0, buf.length);
  }

  static String doSHA1(byte[] buf, int off, int len) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA1");
      md.update(buf, off, len);
      byte[] hv = md.digest();
      Formatter f = new Formatter();
      for (byte b : hv) {
        f.format("%02x", b & 0xFF);
      }
      return f.toString();
    } catch (NoSuchAlgorithmException nsae) {
      throw new Error(nsae);
    }
  }

  /*
   * This class decodes a ServerHello message from the server. The
   * fields we are interested in are stored in the
   * package-accessible fields.
   */
  static class ServerHello {

    int recordVersion;
    int protoVersion;
    long serverTime;
    int cipherSuite;
    int compression;
    String serverCertName;
    String serverCertHash;

    ServerHello(InputStream in)
        throws IOException {
      InputRecord rec = new InputRecord(in);
      rec.setExpectedType(HANDSHAKE);

      /*
       * First, get the handshake message header (4 bytes).
       * First byte should be 2 ("ServerHello"), then
       * comes the message size (over 3 bytes).
       */
      byte[] buf = new byte[4];
      readFully(rec, buf);
      recordVersion = rec.getVersion();
      if (buf[0] != 2) {
        throw new IOException("unexpected handshake"
            + " message type: " + (buf[0] & 0xFF));
      }
      buf = new byte[dec24be(buf, 1)];

      /*
       * Read the complete message in RAM.
       */
      readFully(rec, buf);
      int ptr = 0;

      /*
       * The protocol version which we will use.
       */
      if (ptr + 2 > buf.length) {
        throw new IOException("invalid ServerHello");
      }
      protoVersion = dec16be(buf, 0);
      ptr += 2;

      /*
       * The server random begins with the server's notion
       * of the current time.
       */
      if (ptr + 32 > buf.length) {
        throw new IOException("invalid ServerHello");
      }
      serverTime = 1000L * (dec32be(buf, ptr) & 0xFFFFFFFFL);
      ptr += 32;

      /*
       * We skip the session ID.
       */
      if (ptr + 1 > buf.length) {
        throw new IOException("invalid ServerHello");
      }
      ptr += 1 + (buf[ptr] & 0xFF);

      /*
       * The cipher suite and compression follow.
       */
      if (ptr + 3 > buf.length) {
        throw new IOException("invalid ServerHello");
      }
      cipherSuite = dec16be(buf, ptr);
      compression = buf[ptr + 2] & 0xFF;

      /*
       * The ServerHello could include some extensions
       * here, which we ignore.
       */

      /*
       * We now read a few extra messages, until we
       * reach the server's Certificate message, or
       * ServerHelloDone.
       */
      for (; ; ) {
        buf = new byte[4];
        readFully(rec, buf);
        int mt = buf[0] & 0xFF;
        buf = new byte[dec24be(buf, 1)];
        readFully(rec, buf);
        switch (mt) {
          case 11:
            processCertificate(buf);
            return;
          case 14:
            // ServerHelloDone
            return;
        }
      }
    }

    private void processCertificate(byte[] buf) {
      if (buf.length <= 6) {
        return;
      }
      int len1 = dec24be(buf, 0);
      if (len1 != buf.length - 3) {
        return;
      }
      int len2 = dec24be(buf, 3);
      if (len2 > buf.length - 6) {
        return;
      }
      byte[] ec = new byte[len2];
      System.arraycopy(buf, 6, ec, 0, len2);
      try {
        CertificateFactory cf =
            CertificateFactory.getInstance("X.509");
        X509Certificate xc =
            (X509Certificate) cf.generateCertificate(
                new ByteArrayInputStream(ec));
        serverCertName =
            xc.getSubjectX500Principal().toString();
        serverCertHash = doSHA1(ec);
      } catch (CertificateException e) {
        // ignored
        return;
      }
    }
  }

  /*
   * A constant SSLv2 CLIENT-HELLO message. Only one connection
   * is needed for SSLv2, since the server response will contain
   * _all_ the cipher suites that the server is willing to
   * support.
   *
   * Note: when (mis)interpreted as a SSLv3+ record, this message
   * apparently encodes some data of (invalid) 0x80 type, using
   * protocol version TLS 44.1, and record length of 2 bytes.
   * Thus, the receiving part will quickly conclude that it will
   * not support that, instead of stalling for more data from the
   * client.
   */
  private static final byte[] SSL2_CLIENT_HELLO = {
      (byte) 0x80, (byte) 0x2E,  // header (record length)
      (byte) 0x01,              // message type (CLIENT HELLO)
      (byte) 0x00, (byte) 0x02,  // version (0x0002)
      (byte) 0x00, (byte) 0x15,  // cipher specs list length
      (byte) 0x00, (byte) 0x00,  // session ID length
      (byte) 0x00, (byte) 0x10,  // challenge length
      0x01, 0x00, (byte) 0x80,  // SSL_CK_RC4_128_WITH_MD5
      0x02, 0x00, (byte) 0x80,  // SSL_CK_RC4_128_EXPORT40_WITH_MD5
      0x03, 0x00, (byte) 0x80,  // SSL_CK_RC2_128_CBC_WITH_MD5
      0x04, 0x00, (byte) 0x80,  // SSL_CK_RC2_128_CBC_EXPORT40_WITH_MD5
      0x05, 0x00, (byte) 0x80,  // SSL_CK_IDEA_128_CBC_WITH_MD5
      0x06, 0x00, (byte) 0x40,  // SSL_CK_DES_64_CBC_WITH_MD5
      0x07, 0x00, (byte) 0xC0,  // SSL_CK_DES_192_EDE3_CBC_WITH_MD5
      0x54, 0x54, 0x54, 0x54,  // challenge data (16 bytes)
      0x54, 0x54, 0x54, 0x54,
      0x54, 0x54, 0x54, 0x54,
      0x54, 0x54, 0x54, 0x54
  };

  /*
   * This class represents the response of a server which knows
   $ SSLv2. It includes the list of cipher suites, and the
   * identification of the server certificate.
   */
  static class ServerHelloSSLv2 {

    int[] cipherSuites;
    String serverCertName;
    String serverCertHash;

    ServerHelloSSLv2(InputStream in)
        throws IOException {
      // Record length
      byte[] buf = new byte[2];
      readFully(in, buf);
      int len = dec16be(buf, 0);
      if ((len & 0x8000) == 0) {
        throw new IOException("not a SSLv2 record");
      }
      len &= 0x7FFF;
      if (len < 11) {
        throw new IOException(
            "not a SSLv2 server hello");
      }
      buf = new byte[11];
      readFully(in, buf);
      if (buf[0] != 0x04) {
        throw new IOException(
            "not a SSLv2 server hello");
      }
      int certLen = dec16be(buf, 5);
      int csLen = dec16be(buf, 7);
      int connIdLen = dec16be(buf, 9);
      if (len != 11 + certLen + csLen + connIdLen) {
        throw new IOException(
            "not a SSLv2 server hello");
      }
      if (csLen == 0 || csLen % 3 != 0) {
        throw new IOException(
            "not a SSLv2 server hello");
      }
      byte[] cert = new byte[certLen];
      readFully(in, cert);
      byte[] cs = new byte[csLen];
      readFully(in, cs);
      byte[] connId = new byte[connIdLen];
      readFully(in, connId);
      cipherSuites = new int[csLen / 3];
      for (int i = 0, j = 0; i < csLen; i += 3, j++) {
        cipherSuites[j] = dec24be(cs, i);
      }
      try {
        CertificateFactory cf =
            CertificateFactory.getInstance("X.509");
        X509Certificate xc =
            (X509Certificate) cf.generateCertificate(
                new ByteArrayInputStream(cert));
        serverCertName =
            xc.getSubjectX500Principal().toString();
        serverCertHash = doSHA1(cert);
      } catch (CertificateException e) {
        // ignored
      }
    }
  }

  static Map<Integer, CipherSuite> CIPHER_SUITES =
      new TreeMap<Integer, CipherSuite>();

  static class CipherSuite {

    int suite;
    String name;
    boolean isCBC;
    int strength;
  }

  static final int CLEAR = 0; // no encryption
  static final int WEAK = 1; // weak encryption: 40-bit key
  static final int MEDIUM = 2; // medium encryption: 56-bit key
  static final int STRONG = 3; // strong encryption

  static final String strengthString(int strength) {
    switch (strength) {
      case CLEAR:
        return "no encryption";
      case WEAK:
        return "weak encryption (40-bit)";
      case MEDIUM:
        return "medium encryption (56-bit)";
      case STRONG:
        return "strong encryption (96-bit or more)";
      default:
        throw new Error("strange strength: " + strength);
    }
  }

  static final String cipherSuiteString(int suite) {
    CipherSuite cs = CIPHER_SUITES.get(suite);
    if (cs == null) {
      return String.format("UNKNOWN_SUITE:0x%04X", cs);
    } else {
      return cs.name;
    }
  }

  static final String cipherSuiteStringV2(int suite) {
    CipherSuite cs = CIPHER_SUITES.get(suite);
    if (cs == null) {
      return String.format("UNKNOWN_SUITE:%02X,%02X,%02X",
          suite >> 16, (suite >> 8) & 0xFF, suite & 0XFF);
    } else {
      return cs.name;
    }
  }

  private static final void makeCS(int suite, String name,
                                   boolean isCBC, int strength) {
    CipherSuite cs = new CipherSuite();
    cs.suite = suite;
    cs.name = name;
    cs.isCBC = isCBC;
    cs.strength = strength;
    CIPHER_SUITES.put(suite, cs);

    /*
     * Consistency test: the strength and CBC status can normally
     * be inferred from the name itself.
     */
    boolean inferredCBC = name.contains("_CBC_");
    int inferredStrength;
    if (name.contains("_NULL_")) {
      inferredStrength = CLEAR;
    } else if (name.contains("DES40") || name.contains("_40_")
        || name.contains("EXPORT40")) {
      inferredStrength = WEAK;
    } else if ((name.contains("_DES_") || name.contains("DES_64"))
        && !name.contains("DES_192")) {
      inferredStrength = MEDIUM;
    } else {
      inferredStrength = STRONG;
    }
    if (inferredStrength != strength || inferredCBC != isCBC) {
      throw new RuntimeException(
          "wrong classification: " + name);
    }
  }

  private static final void N(int suite, String name) {
    makeCS(suite, name, false, CLEAR);
  }

  private static final void S4(int suite, String name) {
    makeCS(suite, name, false, WEAK);
  }

  private static final void S8(int suite, String name) {
    makeCS(suite, name, false, STRONG);
  }

  private static final void B4(int suite, String name) {
    makeCS(suite, name, true, WEAK);
  }

  private static final void B5(int suite, String name) {
    makeCS(suite, name, true, MEDIUM);
  }

  private static final void B8(int suite, String name) {
    makeCS(suite, name, true, STRONG);
  }

  static {
    /*
     * SSLv2 cipher suites.
     */
    S8(0x010080, "RC4_128_WITH_MD5");
    S4(0x020080, "RC4_128_EXPORT40_WITH_MD5");
    B8(0x030080, "RC2_128_CBC_WITH_MD5");
    B4(0x040080, "RC2_128_CBC_EXPORT40_WITH_MD5");
    B8(0x050080, "IDEA_128_CBC_WITH_MD5");
    B5(0x060040, "DES_64_CBC_WITH_MD5");
    B8(0x0700C0, "DES_192_EDE3_CBC_WITH_MD5");

    /*
     * Original suites (SSLv3, TLS 1.0).
     */
    N(0x0000, "NULL_WITH_NULL_NULL");
    N(0x0001, "RSA_WITH_NULL_MD5");
    N(0x0002, "RSA_WITH_NULL_SHA");
    S4(0x0003, "RSA_EXPORT_WITH_RC4_40_MD5");
    S8(0x0004, "RSA_WITH_RC4_128_MD5");
    S8(0x0005, "RSA_WITH_RC4_128_SHA");
    B4(0x0006, "RSA_EXPORT_WITH_RC2_CBC_40_MD5");
    B8(0x0007, "RSA_WITH_IDEA_CBC_SHA");
    B4(0x0008, "RSA_EXPORT_WITH_DES40_CBC_SHA");
    B5(0x0009, "RSA_WITH_DES_CBC_SHA");
    B8(0x000A, "RSA_WITH_3DES_EDE_CBC_SHA");
    B4(0x000B, "DH_DSS_EXPORT_WITH_DES40_CBC_SHA");
    B5(0x000C, "DH_DSS_WITH_DES_CBC_SHA");
    B8(0x000D, "DH_DSS_WITH_3DES_EDE_CBC_SHA");
    B4(0x000E, "DH_RSA_EXPORT_WITH_DES40_CBC_SHA");
    B5(0x000F, "DH_RSA_WITH_DES_CBC_SHA");
    B8(0x0010, "DH_RSA_WITH_3DES_EDE_CBC_SHA");
    B4(0x0011, "DHE_DSS_EXPORT_WITH_DES40_CBC_SHA");
    B5(0x0012, "DHE_DSS_WITH_DES_CBC_SHA");
    B8(0x0013, "DHE_DSS_WITH_3DES_EDE_CBC_SHA");
    B4(0x0014, "DHE_RSA_EXPORT_WITH_DES40_CBC_SHA");
    B5(0x0015, "DHE_RSA_WITH_DES_CBC_SHA");
    B8(0x0016, "DHE_RSA_WITH_3DES_EDE_CBC_SHA");
    S4(0x0017, "DH_anon_EXPORT_WITH_RC4_40_MD5");
    S8(0x0018, "DH_anon_WITH_RC4_128_MD5");
    B4(0x0019, "DH_anon_EXPORT_WITH_DES40_CBC_SHA");
    B5(0x001A, "DH_anon_WITH_DES_CBC_SHA");
    B8(0x001B, "DH_anon_WITH_3DES_EDE_CBC_SHA");

    /*
     * FORTEZZA suites (SSLv3 only; see RFC 6101).
     */
    N(0x001C, "FORTEZZA_KEA_WITH_NULL_SHA");
    B8(0x001D, "FORTEZZA_KEA_WITH_FORTEZZA_CBC_SHA");

		/* This one is deactivated since it conflicts with
		   one of the Kerberos cipher suites.
		S8(0x001E, "FORTEZZA_KEA_WITH_RC4_128_SHA"      );
		*/

    /*
     * Kerberos cipher suites (RFC 2712).
     */
    B5(0x001E, "KRB5_WITH_DES_CBC_SHA");
    B8(0x001F, "KRB5_WITH_3DES_EDE_CBC_SHA");
    S8(0x0020, "KRB5_WITH_RC4_128_SHA");
    B8(0x0021, "KRB5_WITH_IDEA_CBC_SHA");
    B5(0x0022, "KRB5_WITH_DES_CBC_MD5");
    B8(0x0023, "KRB5_WITH_3DES_EDE_CBC_MD5");
    S8(0x0024, "KRB5_WITH_RC4_128_MD5");
    B8(0x0025, "KRB5_WITH_IDEA_CBC_MD5");
    B4(0x0026, "KRB5_EXPORT_WITH_DES_CBC_40_SHA");
    B4(0x0027, "KRB5_EXPORT_WITH_RC2_CBC_40_SHA");
    S4(0x0028, "KRB5_EXPORT_WITH_RC4_40_SHA");
    B4(0x0029, "KRB5_EXPORT_WITH_DES_CBC_40_MD5");
    B4(0x002A, "KRB5_EXPORT_WITH_RC2_CBC_40_MD5");
    S4(0x002B, "KRB5_EXPORT_WITH_RC4_40_MD5");

    /*
     * Pre-shared key, no encryption cipher suites (RFC 4785).
     */
    N(0x002C, "PSK_WITH_NULL_SHA");
    N(0x002D, "DHE_PSK_WITH_NULL_SHA");
    N(0x002E, "RSA_PSK_WITH_NULL_SHA");

    /*
     * AES-based suites (TLS 1.1).
     */
    B8(0x002F, "RSA_WITH_AES_128_CBC_SHA");
    B8(0x0030, "DH_DSS_WITH_AES_128_CBC_SHA");
    B8(0x0031, "DH_RSA_WITH_AES_128_CBC_SHA");
    B8(0x0032, "DHE_DSS_WITH_AES_128_CBC_SHA");
    B8(0x0033, "DHE_RSA_WITH_AES_128_CBC_SHA");
    B8(0x0034, "DH_anon_WITH_AES_128_CBC_SHA");
    B8(0x0035, "RSA_WITH_AES_256_CBC_SHA");
    B8(0x0036, "DH_DSS_WITH_AES_256_CBC_SHA");
    B8(0x0037, "DH_RSA_WITH_AES_256_CBC_SHA");
    B8(0x0038, "DHE_DSS_WITH_AES_256_CBC_SHA");
    B8(0x0039, "DHE_RSA_WITH_AES_256_CBC_SHA");
    B8(0x003A, "DH_anon_WITH_AES_256_CBC_SHA");

    /*
     * Suites with SHA-256 (TLS 1.2).
     */
    N(0x003B, "RSA_WITH_NULL_SHA256");
    B8(0x003C, "RSA_WITH_AES_128_CBC_SHA256");
    B8(0x003D, "RSA_WITH_AES_256_CBC_SHA256");
    B8(0x003E, "DH_DSS_WITH_AES_128_CBC_SHA256");
    B8(0x003F, "DH_RSA_WITH_AES_128_CBC_SHA256");
    B8(0x0040, "DHE_DSS_WITH_AES_128_CBC_SHA256");
    B8(0x0067, "DHE_RSA_WITH_AES_128_CBC_SHA256");
    B8(0x0068, "DH_DSS_WITH_AES_256_CBC_SHA256");
    B8(0x0069, "DH_RSA_WITH_AES_256_CBC_SHA256");
    B8(0x006A, "DHE_DSS_WITH_AES_256_CBC_SHA256");
    B8(0x006B, "DHE_RSA_WITH_AES_256_CBC_SHA256");
    B8(0x006C, "DH_anon_WITH_AES_128_CBC_SHA256");
    B8(0x006D, "DH_anon_WITH_AES_256_CBC_SHA256");

    /*
     * Camellia cipher suites (RFC 5932).
     */
    B8(0x0041, "RSA_WITH_CAMELLIA_128_CBC_SHA");
    B8(0x0042, "DH_DSS_WITH_CAMELLIA_128_CBC_SHA");
    B8(0x0043, "DH_RSA_WITH_CAMELLIA_128_CBC_SHA");
    B8(0x0044, "DHE_DSS_WITH_CAMELLIA_128_CBC_SHA");
    B8(0x0045, "DHE_RSA_WITH_CAMELLIA_128_CBC_SHA");
    B8(0x0046, "DH_anon_WITH_CAMELLIA_128_CBC_SHA");
    B8(0x0084, "RSA_WITH_CAMELLIA_256_CBC_SHA");
    B8(0x0085, "DH_DSS_WITH_CAMELLIA_256_CBC_SHA");
    B8(0x0086, "DH_RSA_WITH_CAMELLIA_256_CBC_SHA");
    B8(0x0087, "DHE_DSS_WITH_CAMELLIA_256_CBC_SHA");
    B8(0x0088, "DHE_RSA_WITH_CAMELLIA_256_CBC_SHA");
    B8(0x0089, "DH_anon_WITH_CAMELLIA_256_CBC_SHA");

    /*
     * Unsorted (yet), from the IANA TLS registry:
     * http://www.iana.org/assignments/tls-parameters/
     */
    S8(0x008A, "TLS_PSK_WITH_RC4_128_SHA");
    B8(0x008B, "TLS_PSK_WITH_3DES_EDE_CBC_SHA");
    B8(0x008C, "TLS_PSK_WITH_AES_128_CBC_SHA");
    B8(0x008D, "TLS_PSK_WITH_AES_256_CBC_SHA");
    S8(0x008E, "TLS_DHE_PSK_WITH_RC4_128_SHA");
    B8(0x008F, "TLS_DHE_PSK_WITH_3DES_EDE_CBC_SHA");
    B8(0x0090, "TLS_DHE_PSK_WITH_AES_128_CBC_SHA");
    B8(0x0091, "TLS_DHE_PSK_WITH_AES_256_CBC_SHA");
    S8(0x0092, "TLS_RSA_PSK_WITH_RC4_128_SHA");
    B8(0x0093, "TLS_RSA_PSK_WITH_3DES_EDE_CBC_SHA");
    B8(0x0094, "TLS_RSA_PSK_WITH_AES_128_CBC_SHA");
    B8(0x0095, "TLS_RSA_PSK_WITH_AES_256_CBC_SHA");
    B8(0x0096, "TLS_RSA_WITH_SEED_CBC_SHA");
    B8(0x0097, "TLS_DH_DSS_WITH_SEED_CBC_SHA");
    B8(0x0098, "TLS_DH_RSA_WITH_SEED_CBC_SHA");
    B8(0x0099, "TLS_DHE_DSS_WITH_SEED_CBC_SHA");
    B8(0x009A, "TLS_DHE_RSA_WITH_SEED_CBC_SHA");
    B8(0x009B, "TLS_DH_anon_WITH_SEED_CBC_SHA");
    S8(0x009C, "TLS_RSA_WITH_AES_128_GCM_SHA256");
    S8(0x009D, "TLS_RSA_WITH_AES_256_GCM_SHA384");
    S8(0x009E, "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256");
    S8(0x009F, "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
    S8(0x00A0, "TLS_DH_RSA_WITH_AES_128_GCM_SHA256");
    S8(0x00A1, "TLS_DH_RSA_WITH_AES_256_GCM_SHA384");
    S8(0x00A2, "TLS_DHE_DSS_WITH_AES_128_GCM_SHA256");
    S8(0x00A3, "TLS_DHE_DSS_WITH_AES_256_GCM_SHA384");
    S8(0x00A4, "TLS_DH_DSS_WITH_AES_128_GCM_SHA256");
    S8(0x00A5, "TLS_DH_DSS_WITH_AES_256_GCM_SHA384");
    S8(0x00A6, "TLS_DH_anon_WITH_AES_128_GCM_SHA256");
    S8(0x00A7, "TLS_DH_anon_WITH_AES_256_GCM_SHA384");
    S8(0x00A8, "TLS_PSK_WITH_AES_128_GCM_SHA256");
    S8(0x00A9, "TLS_PSK_WITH_AES_256_GCM_SHA384");
    S8(0x00AA, "TLS_DHE_PSK_WITH_AES_128_GCM_SHA256");
    S8(0x00AB, "TLS_DHE_PSK_WITH_AES_256_GCM_SHA384");
    S8(0x00AC, "TLS_RSA_PSK_WITH_AES_128_GCM_SHA256");
    S8(0x00AD, "TLS_RSA_PSK_WITH_AES_256_GCM_SHA384");
    B8(0x00AE, "TLS_PSK_WITH_AES_128_CBC_SHA256");
    B8(0x00AF, "TLS_PSK_WITH_AES_256_CBC_SHA384");
    N(0x00B0, "TLS_PSK_WITH_NULL_SHA256");
    N(0x00B1, "TLS_PSK_WITH_NULL_SHA384");
    B8(0x00B2, "TLS_DHE_PSK_WITH_AES_128_CBC_SHA256");
    B8(0x00B3, "TLS_DHE_PSK_WITH_AES_256_CBC_SHA384");
    N(0x00B4, "TLS_DHE_PSK_WITH_NULL_SHA256");
    N(0x00B5, "TLS_DHE_PSK_WITH_NULL_SHA384");
    B8(0x00B6, "TLS_RSA_PSK_WITH_AES_128_CBC_SHA256");
    B8(0x00B7, "TLS_RSA_PSK_WITH_AES_256_CBC_SHA384");
    N(0x00B8, "TLS_RSA_PSK_WITH_NULL_SHA256");
    N(0x00B9, "TLS_RSA_PSK_WITH_NULL_SHA384");
    B8(0x00BA, "TLS_RSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0x00BB, "TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0x00BC, "TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0x00BD, "TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0x00BE, "TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0x00BF, "TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0x00C0, "TLS_RSA_WITH_CAMELLIA_256_CBC_SHA256");
    B8(0x00C1, "TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA256");
    B8(0x00C2, "TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA256");
    B8(0x00C3, "TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA256");
    B8(0x00C4, "TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256");
    B8(0x00C5, "TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA256");
		/* This one is a fake cipher suite which marks a
		   renegotiation.
		N(0x00FF, "TLS_EMPTY_RENEGOTIATION_INFO_SCSV"                );
		*/
    N(0xC001, "TLS_ECDH_ECDSA_WITH_NULL_SHA");
    S8(0xC002, "TLS_ECDH_ECDSA_WITH_RC4_128_SHA");
    B8(0xC003, "TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA");
    B8(0xC004, "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA");
    B8(0xC005, "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA");
    N(0xC006, "TLS_ECDHE_ECDSA_WITH_NULL_SHA");
    S8(0xC007, "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA");
    B8(0xC008, "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA");
    B8(0xC009, "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA");
    B8(0xC00A, "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA");
    N(0xC00B, "TLS_ECDH_RSA_WITH_NULL_SHA");
    S8(0xC00C, "TLS_ECDH_RSA_WITH_RC4_128_SHA");
    B8(0xC00D, "TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA");
    B8(0xC00E, "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA");
    B8(0xC00F, "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA");
    N(0xC010, "TLS_ECDHE_RSA_WITH_NULL_SHA");
    S8(0xC011, "TLS_ECDHE_RSA_WITH_RC4_128_SHA");
    B8(0xC012, "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA");
    B8(0xC013, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
    B8(0xC014, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    N(0xC015, "TLS_ECDH_anon_WITH_NULL_SHA");
    S8(0xC016, "TLS_ECDH_anon_WITH_RC4_128_SHA");
    B8(0xC017, "TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA");
    B8(0xC018, "TLS_ECDH_anon_WITH_AES_128_CBC_SHA");
    B8(0xC019, "TLS_ECDH_anon_WITH_AES_256_CBC_SHA");
    B8(0xC01A, "TLS_SRP_SHA_WITH_3DES_EDE_CBC_SHA");
    B8(0xC01B, "TLS_SRP_SHA_RSA_WITH_3DES_EDE_CBC_SHA");
    B8(0xC01C, "TLS_SRP_SHA_DSS_WITH_3DES_EDE_CBC_SHA");
    B8(0xC01D, "TLS_SRP_SHA_WITH_AES_128_CBC_SHA");
    B8(0xC01E, "TLS_SRP_SHA_RSA_WITH_AES_128_CBC_SHA");
    B8(0xC01F, "TLS_SRP_SHA_DSS_WITH_AES_128_CBC_SHA");
    B8(0xC020, "TLS_SRP_SHA_WITH_AES_256_CBC_SHA");
    B8(0xC021, "TLS_SRP_SHA_RSA_WITH_AES_256_CBC_SHA");
    B8(0xC022, "TLS_SRP_SHA_DSS_WITH_AES_256_CBC_SHA");
    B8(0xC023, "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");
    B8(0xC024, "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");
    B8(0xC025, "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256");
    B8(0xC026, "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384");
    B8(0xC027, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
    B8(0xC028, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");
    B8(0xC029, "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256");
    B8(0xC02A, "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384");
    S8(0xC02B, "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
    S8(0xC02C, "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
    S8(0xC02D, "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256");
    S8(0xC02E, "TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384");
    S8(0xC02F, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    S8(0xC030, "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
    S8(0xC031, "TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256");
    S8(0xC032, "TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384");
    S8(0xC033, "TLS_ECDHE_PSK_WITH_RC4_128_SHA");
    B8(0xC034, "TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA");
    B8(0xC035, "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA");
    B8(0xC036, "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA");
    B8(0xC037, "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256");
    B8(0xC038, "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384");
    N(0xC039, "TLS_ECDHE_PSK_WITH_NULL_SHA");
    N(0xC03A, "TLS_ECDHE_PSK_WITH_NULL_SHA256");
    N(0xC03B, "TLS_ECDHE_PSK_WITH_NULL_SHA384");
    B8(0xC03C, "TLS_RSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC03D, "TLS_RSA_WITH_ARIA_256_CBC_SHA384");
    B8(0xC03E, "TLS_DH_DSS_WITH_ARIA_128_CBC_SHA256");
    B8(0xC03F, "TLS_DH_DSS_WITH_ARIA_256_CBC_SHA384");
    B8(0xC040, "TLS_DH_RSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC041, "TLS_DH_RSA_WITH_ARIA_256_CBC_SHA384");
    B8(0xC042, "TLS_DHE_DSS_WITH_ARIA_128_CBC_SHA256");
    B8(0xC043, "TLS_DHE_DSS_WITH_ARIA_256_CBC_SHA384");
    B8(0xC044, "TLS_DHE_RSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC045, "TLS_DHE_RSA_WITH_ARIA_256_CBC_SHA384");
    B8(0xC046, "TLS_DH_anon_WITH_ARIA_128_CBC_SHA256");
    B8(0xC047, "TLS_DH_anon_WITH_ARIA_256_CBC_SHA384");
    B8(0xC048, "TLS_ECDHE_ECDSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC049, "TLS_ECDHE_ECDSA_WITH_ARIA_256_CBC_SHA384");
    B8(0xC04A, "TLS_ECDH_ECDSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC04B, "TLS_ECDH_ECDSA_WITH_ARIA_256_CBC_SHA384");
    B8(0xC04C, "TLS_ECDHE_RSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC04D, "TLS_ECDHE_RSA_WITH_ARIA_256_CBC_SHA384");
    B8(0xC04E, "TLS_ECDH_RSA_WITH_ARIA_128_CBC_SHA256");
    B8(0xC04F, "TLS_ECDH_RSA_WITH_ARIA_256_CBC_SHA384");
    S8(0xC050, "TLS_RSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC051, "TLS_RSA_WITH_ARIA_256_GCM_SHA384");
    S8(0xC052, "TLS_DHE_RSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC053, "TLS_DHE_RSA_WITH_ARIA_256_GCM_SHA384");
    S8(0xC054, "TLS_DH_RSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC055, "TLS_DH_RSA_WITH_ARIA_256_GCM_SHA384");
    S8(0xC056, "TLS_DHE_DSS_WITH_ARIA_128_GCM_SHA256");
    S8(0xC057, "TLS_DHE_DSS_WITH_ARIA_256_GCM_SHA384");
    S8(0xC058, "TLS_DH_DSS_WITH_ARIA_128_GCM_SHA256");
    S8(0xC059, "TLS_DH_DSS_WITH_ARIA_256_GCM_SHA384");
    S8(0xC05A, "TLS_DH_anon_WITH_ARIA_128_GCM_SHA256");
    S8(0xC05B, "TLS_DH_anon_WITH_ARIA_256_GCM_SHA384");
    S8(0xC05C, "TLS_ECDHE_ECDSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC05D, "TLS_ECDHE_ECDSA_WITH_ARIA_256_GCM_SHA384");
    S8(0xC05E, "TLS_ECDH_ECDSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC05F, "TLS_ECDH_ECDSA_WITH_ARIA_256_GCM_SHA384");
    S8(0xC060, "TLS_ECDHE_RSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC061, "TLS_ECDHE_RSA_WITH_ARIA_256_GCM_SHA384");
    S8(0xC062, "TLS_ECDH_RSA_WITH_ARIA_128_GCM_SHA256");
    S8(0xC063, "TLS_ECDH_RSA_WITH_ARIA_256_GCM_SHA384");
    B8(0xC064, "TLS_PSK_WITH_ARIA_128_CBC_SHA256");
    B8(0xC065, "TLS_PSK_WITH_ARIA_256_CBC_SHA384");
    B8(0xC066, "TLS_DHE_PSK_WITH_ARIA_128_CBC_SHA256");
    B8(0xC067, "TLS_DHE_PSK_WITH_ARIA_256_CBC_SHA384");
    B8(0xC068, "TLS_RSA_PSK_WITH_ARIA_128_CBC_SHA256");
    B8(0xC069, "TLS_RSA_PSK_WITH_ARIA_256_CBC_SHA384");
    S8(0xC06A, "TLS_PSK_WITH_ARIA_128_GCM_SHA256");
    S8(0xC06B, "TLS_PSK_WITH_ARIA_256_GCM_SHA384");
    S8(0xC06C, "TLS_DHE_PSK_WITH_ARIA_128_GCM_SHA256");
    S8(0xC06D, "TLS_DHE_PSK_WITH_ARIA_256_GCM_SHA384");
    S8(0xC06E, "TLS_RSA_PSK_WITH_ARIA_128_GCM_SHA256");
    S8(0xC06F, "TLS_RSA_PSK_WITH_ARIA_256_GCM_SHA384");
    B8(0xC070, "TLS_ECDHE_PSK_WITH_ARIA_128_CBC_SHA256");
    B8(0xC071, "TLS_ECDHE_PSK_WITH_ARIA_256_CBC_SHA384");
    B8(0xC072, "TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC073, "TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_CBC_SHA384");
    B8(0xC074, "TLS_ECDH_ECDSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC075, "TLS_ECDH_ECDSA_WITH_CAMELLIA_256_CBC_SHA384");
    B8(0xC076, "TLS_ECDHE_RSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC077, "TLS_ECDHE_RSA_WITH_CAMELLIA_256_CBC_SHA384");
    B8(0xC078, "TLS_ECDH_RSA_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC079, "TLS_ECDH_RSA_WITH_CAMELLIA_256_CBC_SHA384");
    S8(0xC07A, "TLS_RSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC07B, "TLS_RSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC07C, "TLS_DHE_RSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC07D, "TLS_DHE_RSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC07E, "TLS_DH_RSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC07F, "TLS_DH_RSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC080, "TLS_DHE_DSS_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC081, "TLS_DHE_DSS_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC082, "TLS_DH_DSS_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC083, "TLS_DH_DSS_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC084, "TLS_DH_anon_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC085, "TLS_DH_anon_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC086, "TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC087, "TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC088, "TLS_ECDH_ECDSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC089, "TLS_ECDH_ECDSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC08A, "TLS_ECDHE_RSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC08B, "TLS_ECDHE_RSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC08C, "TLS_ECDH_RSA_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC08D, "TLS_ECDH_RSA_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC08E, "TLS_PSK_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC08F, "TLS_PSK_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC090, "TLS_DHE_PSK_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC091, "TLS_DHE_PSK_WITH_CAMELLIA_256_GCM_SHA384");
    S8(0xC092, "TLS_RSA_PSK_WITH_CAMELLIA_128_GCM_SHA256");
    S8(0xC093, "TLS_RSA_PSK_WITH_CAMELLIA_256_GCM_SHA384");
    B8(0xC094, "TLS_PSK_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC095, "TLS_PSK_WITH_CAMELLIA_256_CBC_SHA384");
    B8(0xC096, "TLS_DHE_PSK_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC097, "TLS_DHE_PSK_WITH_CAMELLIA_256_CBC_SHA384");
    B8(0xC098, "TLS_RSA_PSK_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC099, "TLS_RSA_PSK_WITH_CAMELLIA_256_CBC_SHA384");
    B8(0xC09A, "TLS_ECDHE_PSK_WITH_CAMELLIA_128_CBC_SHA256");
    B8(0xC09B, "TLS_ECDHE_PSK_WITH_CAMELLIA_256_CBC_SHA384");
    S8(0xC09C, "TLS_RSA_WITH_AES_128_CCM");
    S8(0xC09D, "TLS_RSA_WITH_AES_256_CCM");
    S8(0xC09E, "TLS_DHE_RSA_WITH_AES_128_CCM");
    S8(0xC09F, "TLS_DHE_RSA_WITH_AES_256_CCM");
    S8(0xC0A0, "TLS_RSA_WITH_AES_128_CCM_8");
    S8(0xC0A1, "TLS_RSA_WITH_AES_256_CCM_8");
    S8(0xC0A2, "TLS_DHE_RSA_WITH_AES_128_CCM_8");
    S8(0xC0A3, "TLS_DHE_RSA_WITH_AES_256_CCM_8");
    S8(0xC0A4, "TLS_PSK_WITH_AES_128_CCM");
    S8(0xC0A5, "TLS_PSK_WITH_AES_256_CCM");
    S8(0xC0A6, "TLS_DHE_PSK_WITH_AES_128_CCM");
    S8(0xC0A7, "TLS_DHE_PSK_WITH_AES_256_CCM");
    S8(0xC0A8, "TLS_PSK_WITH_AES_128_CCM_8");
    S8(0xC0A9, "TLS_PSK_WITH_AES_256_CCM_8");
    S8(0xC0AA, "TLS_PSK_DHE_WITH_AES_128_CCM_8");
    S8(0xC0AB, "TLS_PSK_DHE_WITH_AES_256_CCM_8");
  }
}
