package com.gemstone.gemfire.distributed.internal.tcpserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.UnsupportedVersionException;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.VersionedDataOutputStream;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.GemFireTracer;

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
  private static final GemFireTracer LOG=GemFireTracer.getLog(TcpClient.class);
  private static final int REQUEST_TIMEOUT = 60 * 2 * 1000;

  private static Map<IpAddress, Short> serverVersions = new HashMap<IpAddress, Short>();
  

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
      LOG.error("TcpClient.stop(): exception connecting to locator " + addr + ":" + port + ": " + ex);
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
      LOG.error("TcpClient.getInfo(): exception connecting to locator " + addr + ":" + port + ": " + ex);
      return null;
    }
  
  }

  public static Object requestToServer(InetAddress addr, int port, Object request, int timeout) throws IOException, ClassNotFoundException {
    return requestToServer(addr, port, request, timeout, true);
  }
  
  public static Object requestToServer(InetAddress addr, int port, Object request, int timeout, boolean replyExpected) throws IOException, ClassNotFoundException {
    IpAddress ipAddr;
    if (addr == null) {
      ipAddr = new IpAddress(port);
    } else {
      ipAddr = new IpAddress(addr, port); // fix for bug 30810
    }

    // Get the GemFire version of the TcpServer first, before sending any other request.
    short serverVersion = getServerVersion(ipAddr, REQUEST_TIMEOUT).shortValue();

    if (serverVersion > Version.CURRENT_ORDINAL) {
      serverVersion = Version.CURRENT_ORDINAL;
    }

    // establish the old GossipVersion for the server
    int gossipVersion = TcpServer.getCurrentGossipVersion();

    if (Version.GFE_71.compareTo(serverVersion) > 0) {
      gossipVersion = TcpServer.getOldGossipVersion();
    }

    Socket sock=SocketCreator.getDefaultInstance().connect(ipAddr.getIpAddress(), ipAddr.getPort(), timeout, null, false);
    sock.setSoTimeout(timeout);
    try {
      DataOutputStream out=new DataOutputStream(sock.getOutputStream());
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
          return response;
        } catch (EOFException ex) {
          throw new EOFException("Locator at " + ipAddr + " did not respond. This is normal if the locator was shutdown. If it wasn't check its log for exceptions.");
        }
      }
      else {
        return null;
      }
    } catch (UnsupportedVersionException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Remote TcpServer version: " + serverVersion
                + " is higher than local version: " + Version.CURRENT_ORDINAL
                + ". This is never expected as remoteVersion");
      }
      return null;
    } finally {
      try {
        sock.close();
      } catch(Exception e) {
        LOG.error("Error closing socket ", e);
      }
    }
  }

  public static Short getServerVersion(IpAddress ipAddr, int timeout) throws IOException, ClassNotFoundException {

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
    
    Socket sock=SocketCreator.getDefaultInstance().connect(ipAddr.getIpAddress(), ipAddr.getPort(), timeout, null, false);
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
        LOG.error("Error closing socket ", e);
      }
    }
    if (LOG.getLogWriter().fineEnabled()) {
      LOG.getLogWriter().fine("Locator " + ipAddr + " did not respond to a request for its version.  I will assume it is using v5.7 for safety.");
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
