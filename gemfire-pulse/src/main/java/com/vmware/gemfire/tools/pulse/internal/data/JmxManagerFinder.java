/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.vmware.gemfire.tools.pulse.internal.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import com.vmware.gemfire.tools.pulse.internal.util.ConnectionUtil;

/**
 * This class can be used to connect to a locator and ask it to find a jmx
 * manager. If the locator can find a jmx manager that is already running it
 * returns it. Otherwise the locator will attempt to start a jmx manager and
 * then return it.
 * 
 * This code does not depend on gemfire.jar but in order to do this some of
 * GemFire's internal serialization codes and byte sequences have been hard
 * coded into this code.
 * 
 * @author darrel
 * @since version 7.0.Beta 2012-09-23
 * 
 */
public class JmxManagerFinder {

  private final static PulseLogWriter LOGGER = PulseLogWriter.getLogger();

  /*
   * public static void main(String[] args) throws IOException { if (args.length
   * != 2) { System.err.println(
   * "Usage: JmxManagerFinder locatorHost locatorPort. Expected two arguments but found "
   * + args.length); return; } String locatorHost = args[0]; int locatorPort =
   * Integer.parseInt(args[1]);
   * 
   * InetAddress addr = InetAddress.getByName(locatorHost); JmxManagerInfo
   * locRes = askLocatorForJmxManager(addr, locatorPort, 15000);
   * 
   * if (locRes.port == 0) {
   * System.out.println("Locator could not find a jmx manager"); } else {
   * System.out.println("Locator on host " + locRes.host + " port " +
   * locRes.port + (locRes.ssl ? " ssl" : "")); } }
   */
  private static final short JMX_MANAGER_LOCATOR_REQUEST = 2150;
  private static final short JMX_MANAGER_LOCATOR_RESPONSE = 2151;
  private static final byte DS_FIXED_ID_SHORT = 2;
  private static final int GOSSIPVERSION = 1001;
  private static final byte STRING_BYTES = 87;
  private static final byte NULL_STRING = 69;

  /**
   * Describes the location of a jmx manager. If a jmx manager does not exist
   * then port will be 0.
   * 
   * @author darrel
   * 
   */
  public static class JmxManagerInfo {
    JmxManagerInfo(String host, int port, boolean ssl) {
      this.host = host;
      this.port = port;
      this.ssl = ssl;
    }

    /**
     * The host/address the jmx manager is listening on.
     */
    public final String host;
    /**
     * The port the jmx manager is listening on.
     */
    public final int port;
    /**
     * True if the jmx manager is using SSL.
     */
    public final boolean ssl;
  }

  /**
   * Ask a locator to find a jmx manager. The locator will start one if one is
   * not already running.
   * 
   * @param addr
   *          the host address the locator is listening on
   * @param port
   *          the port the locator is listening on
   * @param timeout
   *          the number of milliseconds to wait for a response; 15000 is a
   *          reasonable default
   * @return describes the location of the jmx manager. The port will be zero if
   *         no jmx manager was found.
   * @throws IOException
   *           if a problem occurs trying to connect to the locator or
   *           communicate with it.
   */
  public static JmxManagerInfo askLocatorForJmxManager(InetAddress addr,
      int port, int timeout, boolean usessl) throws IOException {
    SocketAddress sockaddr = new InetSocketAddress(addr, port);
    Socket sock = ConnectionUtil.getSocketFactory(usessl).createSocket();
    try {
      sock.connect(sockaddr, timeout);
      sock.setSoTimeout(timeout);
      DataOutputStream out = new DataOutputStream(sock.getOutputStream());

      out.writeInt(GOSSIPVERSION);
      out.writeByte(DS_FIXED_ID_SHORT);
      out.writeShort(JMX_MANAGER_LOCATOR_REQUEST);
      out.flush();

      DataInputStream in = new DataInputStream(sock.getInputStream());
      byte header = in.readByte();
      if (header != DS_FIXED_ID_SHORT) {
        throw new IllegalStateException("Expected " + DS_FIXED_ID_SHORT
            + " but found " + header);
      }
      int msgType = in.readShort();
      if (msgType != JMX_MANAGER_LOCATOR_RESPONSE) {
        throw new IllegalStateException("Expected "
            + JMX_MANAGER_LOCATOR_RESPONSE + " but found " + msgType);
      }
      byte hostHeader = in.readByte();
      String host;
      if (hostHeader == NULL_STRING) {
        host = "";
      } else if (hostHeader == STRING_BYTES) {
        int len = in.readUnsignedShort();
        byte[] buf = new byte[len];
        in.readFully(buf, 0, len);
        @SuppressWarnings("deprecation")
        String str = new String(buf, 0);
        host = str;
      } else {
        throw new IllegalStateException("Expected " + STRING_BYTES + " or "
            + NULL_STRING + " but found " + hostHeader);
      }
      int jmport = in.readInt();
      boolean ssl = in.readBoolean();
      if (host.equals("")) {
        jmport = 0;
      }
      return new JmxManagerInfo(host, jmport, ssl);
    } finally {
      try {
        sock.close();
      } catch (Exception e) {
      }
    }
  }
}
