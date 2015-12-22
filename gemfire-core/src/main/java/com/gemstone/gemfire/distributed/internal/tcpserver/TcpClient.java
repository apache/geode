/*
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
 */
package com.gemstone.gemfire.distributed.internal.tcpserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.UnsupportedVersionException;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.VersionedDataOutputStream;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Client for the TcpServer. These methods were refactored out of GossipClient,
 * because they are required for the server regardess of whether we are using the 
 * GossipServer or the ServerLocator.
 * 
 * TODO - refactor this to support keep-alive connections to the server. requestToServer
 * probably shouldn't a static method.
 * @author dsmith
 * @since 5.7
 *
 */
public class TcpClient {
  private static final Logger logger = LogService.getLogger();
  
  private static final int REQUEST_TIMEOUT = 60 * 2 * 1000;

  private static Map<InetSocketAddress, Short> serverVersions = new HashMap<InetSocketAddress, Short>();
  

  /**
   * Stops the TcpServer running on a given host and port
   */
  public static void stop(InetAddress addr, int port) throws java.net.ConnectException {
    try {
      ShutdownRequest request =new ShutdownRequest();
      TcpClient.requestToServer(addr, port, request, REQUEST_TIMEOUT);
    } 
    catch (java.net.ConnectException ce) {
      // must not be running, rethrow so the caller can handle. 
      // In most cases this Exception should be ignored.
      throw ce;
    }
    catch(Exception ex) {
      logger.error("TcpClient.stop(): exception connecting to locator " + addr + ":" + port + ": " + ex);
    }
  }

  /** Contacts the gossip server running on the given host,
  * and port and gets information about it.  Two <code>String</code>s
  * are returned: the first string is the working directory of the
  * locator and the second string is the product directory of the
  * locator.
  */
  public static String[] getInfo(InetAddress addr, int port) {
    try {
      InfoRequest request = new InfoRequest();
      InfoResponse response = (InfoResponse) TcpClient.requestToServer(addr, port, request, REQUEST_TIMEOUT);
      return response.getInfo();
    } catch (java.net.ConnectException ignore) {
      return null;
    } catch(Exception ex) {
      logger.error("TcpClient.getInfo(): exception connecting to locator " + addr + ":" + port + ": " + ex);
      return null;
    }
  
  }

  public static Object requestToServer(InetAddress addr, int port, Object request, int timeout) throws IOException, ClassNotFoundException {
    return requestToServer(addr, port, request, timeout, true);
  }
  
  public static Object requestToServer(InetAddress addr, int port, Object request, int timeout, boolean replyExpected) throws IOException, ClassNotFoundException {
    InetSocketAddress ipAddr;
    if (addr == null) {
      ipAddr = new InetSocketAddress(port);
    } else {
      ipAddr = new InetSocketAddress(addr, port); // fix for bug 30810
    }
    
    logger.debug("TcpClient sending {} to {}", request, ipAddr);

    long giveupTime = System.currentTimeMillis() + timeout;
    
    // Get the GemFire version of the TcpServer first, before sending any other request.
    short serverVersion = getServerVersion(ipAddr, timeout).shortValue();

    if (serverVersion > Version.CURRENT_ORDINAL) {
      serverVersion = Version.CURRENT_ORDINAL;
    }

    // establish the old GossipVersion for the server
    int gossipVersion = TcpServer.getCurrentGossipVersion();

    if (Version.GFE_71.compareTo(serverVersion) > 0) {
      gossipVersion = TcpServer.getOldGossipVersion();
    }

    long newTimeout = giveupTime - System.currentTimeMillis();
    if (newTimeout <= 0) {
      return null;
    }
    
    Socket sock=SocketCreator.getDefaultInstance().connect(ipAddr.getAddress(), ipAddr.getPort(), (int)newTimeout, null, false);
    sock.setSoTimeout((int)newTimeout);
    DataOutputStream out = null;
    try {
      out=new DataOutputStream(sock.getOutputStream());
      
      if (serverVersion < Version.CURRENT_ORDINAL) {
        out = new VersionedDataOutputStream(out, Version.fromOrdinalNoThrow(serverVersion, false));
      }
      
      out.writeInt(gossipVersion);
      if (gossipVersion > TcpServer.getOldGossipVersion()) {
        out.writeShort(serverVersion);
      }
      DataSerializer.writeObject(request, out);
      out.flush();

      if (replyExpected) {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        in = new VersionedDataInputStream(in, Version.fromOrdinal(serverVersion, false)); 
        try {
          Object response = DataSerializer.readObject(in);
          logger.debug("received response: {}", response);
          return response;
        } catch (EOFException ex) {
          throw new EOFException("Locator at " + ipAddr + " did not respond. This is normal if the locator was shutdown. If it wasn't check its log for exceptions.");
        }
      }
      else {
        return null;
      }
    } catch (UnsupportedVersionException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Remote TcpServer version: " + serverVersion
                + " is higher than local version: " + Version.CURRENT_ORDINAL
                + ". This is never expected as remoteVersion");
      }
      return null;
    } finally {
      if (out != null) {
        out.close();
      }
      try {
        sock.close();
      } catch(Exception e) {
        logger.error("Error closing socket ", e);
      }
    }
  }

  public static Short getServerVersion(InetSocketAddress ipAddr, int timeout) throws IOException, ClassNotFoundException {

    int gossipVersion = TcpServer.getCurrentGossipVersion();
    Short serverVersion = null;

    // Get GemFire version of TcpServer first, before sending any other request.
    VersionResponse verRes = null;
    synchronized(serverVersions) {
      serverVersion = serverVersions.get(ipAddr);
    }
    if (serverVersion != null) {
      return serverVersion;
    }
    
    gossipVersion = TcpServer.getOldGossipVersion();
    
    Socket sock=SocketCreator.getDefaultInstance().connect(ipAddr.getAddress(), ipAddr.getPort(), timeout, null, false);
    sock.setSoTimeout(timeout);
    
    try {
      DataOutputStream out=new DataOutputStream(sock.getOutputStream());
      out = new VersionedDataOutputStream(out, Version.GFE_57);
      
      out.writeInt(gossipVersion);

      VersionRequest verRequest = new VersionRequest();
      DataSerializer.writeObject(verRequest, out);
      out.flush();

      DataInputStream in = new DataInputStream(sock.getInputStream());
      in = new VersionedDataInputStream(in, Version.GFE_57); 
      try {
        VersionResponse response = DataSerializer.readObject(in);
        if (response != null) {
          serverVersion = Short.valueOf(response.getVersionOrdinal());
          synchronized(serverVersions) {
            serverVersions.put(ipAddr, serverVersion);
          }
          return serverVersion;
        }
      } catch (EOFException ex) {
        // old locators will not recognize the version request and will close the connection
      }
    } finally {
      try {
        sock.close();
      } catch(Exception e) {
        logger.error("Error closing socket ", e);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Locator " + ipAddr + " did not respond to a request for its version.  I will assume it is using v5.7 for safety.");
    }
    synchronized(serverVersions) {
      serverVersions.put(ipAddr, Version.GFE_57.ordinal());
    }
    return Short.valueOf(Version.GFE_57.ordinal());
  }

  private TcpClient() {
    //static class
  }

}
