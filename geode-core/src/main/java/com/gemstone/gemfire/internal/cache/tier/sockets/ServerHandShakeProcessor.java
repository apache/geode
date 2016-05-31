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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.IncompatibleVersionException;
import com.gemstone.gemfire.cache.UnsupportedVersionException;
import com.gemstone.gemfire.cache.VersionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataStream;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.Principal;
import java.util.Properties;

/**
 * A <code>ServerHandShakeProcessor</code> verifies the client's version compatibility with server.
 *
 * @since GemFire 5.7
 */


public class ServerHandShakeProcessor {
  private static final Logger logger = LogService.getLogger();

  protected static final byte REPLY_REFUSED = (byte)60;

  protected static final byte REPLY_INVALID = (byte)61;

  public static Version currentServerVersion = Acceptor.VERSION;

  /**
   * Test hook for server version support
   * 
   * @since GemFire 5.7
   */
  public static void setSeverVersionForTesting(short ver) {
    currentServerVersion = Version.fromOrdinalOrCurrent(ver);
  }

  public static boolean readHandShake(ServerConnection connection) {
    boolean validHandShake = false;
    Version clientVersion = null;
    try {
      // Read the version byte from the socket
      clientVersion = readClientVersion(connection);
    }
    catch (IOException e) {
      //Only log an exception if the server is still running.
      if(connection.getAcceptor().isRunning()) {
      // Server logging
        logger.warn("{} {}", connection.getName(), e.getMessage(), e);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      validHandShake = false;
    }
    catch (UnsupportedVersionException uve) {
      // Server logging
      logger.warn("{} {}", connection.getName(), uve.getMessage(), uve);
      // Client logging
      connection.refuseHandshake(uve.getMessage(), REPLY_REFUSED);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      validHandShake = false;
    }
    catch (Exception e) {
      // Server logging
      logger.warn("{} {}", connection.getName(), e.getMessage(), e);
      // Client logging
      connection
          .refuseHandshake(
              LocalizedStrings.ServerHandShakeProcessor_0_SERVERS_CURRENT_VERSION_IS_1
                  .toLocalizedString(new Object[] { e.getMessage(),
                      Acceptor.VERSION.toString() }), REPLY_REFUSED);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      validHandShake = false;
    }
    
    if (clientVersion != null) {
  
      if (logger.isDebugEnabled())
        logger.debug("Client version: {}", clientVersion);
  
      // Read the appropriate handshake
      if (clientVersion.compareTo(Version.GFE_57) >= 0) {
          validHandShake = readGFEHandshake(connection, clientVersion);
      } else {
          connection.refuseHandshake("Unsupported version "
              + clientVersion + "Server's current version "
              + Acceptor.VERSION, REPLY_REFUSED);
      }
    }
    
    return validHandShake;
  }

  /**
   * Refuse a received handshake.
   * 
   * @param out
   *                the Stream to the waiting greeter.
   * @param message
   *                providing details about the refusal reception, mainly for
   *                client logging.
   * @throws IOException
   */
  public static void refuse(OutputStream out, String message) throws IOException
  {
    refuse(out,message,REPLY_REFUSED);
  }
  
  /**
   * Refuse a received handshake.
   * 
   * @param out
   *                the Stream to the waiting greeter.
   * @param message
   *                providing details about the refusal reception, mainly for
   *                client logging.
   * @param exception
   *                providing details about exception occurred.
   * @throws IOException
   */
  public static void refuse(OutputStream out, String message, byte exception)
      throws IOException {

    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    DataOutputStream dos = new DataOutputStream(hdos);
    // Write refused reply
    dos.writeByte(exception);

    // write dummy epType
    dos.writeByte(0);
    // write dummy qSize
    dos.writeInt(0);

    // Write the server's member
    DistributedMember member = InternalDistributedSystem.getAnyInstance()
        .getDistributedMember();
    writeServerMember(member, dos);

    // Write the refusal message
    if (message == null) {
      message = "";
    }
    dos.writeUTF(message);

    // Write dummy delta-propagation property value. This will never be read at
    // receiver because the exception byte above will cause the receiver code
    // throw an exception before the below byte could be read.
    dos.writeBoolean(Boolean.TRUE);

    out.write(hdos.toByteArray());
    out.flush();
  }
  // Keep the writeServerMember/readServerMember compatible with C++ native
  // client
  protected static void writeServerMember(DistributedMember member,
      DataOutputStream dos) throws IOException {

    Version v = Version.CURRENT;
    if (dos instanceof VersionedDataStream) {
      v = ((VersionedDataStream) dos).getVersion();
    }
    HeapDataOutputStream hdos = new HeapDataOutputStream(v);
    DataSerializer.writeObject(member, hdos);
    DataSerializer.writeByteArray(hdos.toByteArray(), dos);
    hdos.close();
  }
  
  private static boolean readGFEHandshake(ServerConnection connection,
      Version clientVersion) {
    int handShakeTimeout = connection.getHandShakeTimeout();
    InternalLogWriter securityLogWriter = connection.getSecurityLogWriter();
    try {
      Socket socket = connection.getSocket();
      DistributedSystem system = connection.getDistributedSystem();
      //hitesh:it will set credentials and principals
      HandShake handshake = new HandShake(socket, handShakeTimeout, system,
          clientVersion, connection.getCommunicationMode());
      connection.setHandshake(handshake);
      ClientProxyMembershipID proxyId = handshake.getMembership();
      connection.setProxyId(proxyId);
      //hitesh: it gets principals
      //Hitesh:for older version we should set this 
      if (clientVersion.compareTo(Version.GFE_65) < 0
          || connection.getCommunicationMode() == Acceptor.GATEWAY_TO_GATEWAY) {
       /* Principal principal = handshake.verifyCredentials();
        connection.setPrincipal(principal);
         if (principal != null) {
          if (connection.getSecurityLogger().fineEnabled())
            securityLogger.fine(connection.getName()
                + ": successfully verified credentials for proxyID [" + proxyId
                + "] having principal: " + principal.getName());
        } else if (socket instanceof SSLSocket) {
          // Test whether we are using SSL connection in mutual authentication
          // mode and use its principal.
          SSLSocket sslSocket = (SSLSocket) socket;
          SSLSession sslSession = sslSocket.getSession();
          if (!sslSession.getCipherSuite().equals("SSL_NULL_WITH_NULL_NULL")
              && sslSocket.getNeedClientAuth()) {
            try {
              Certificate[] certs = sslSession.getPeerCertificates();
              if (certs[0] instanceof X509Certificate) {
                principal = ((X509Certificate) certs[0])
                    .getSubjectX500Principal();
                if (securityLogger.fineEnabled())
                  securityLogger.fine(connection.getName()
                      + ": successfully verified credentials for proxyID ["
                      + proxyId
                      + "] using SSL mutual authentication with principal: "
                      + principal.getName());
              } else {
                if (securityLogger.warningEnabled())
                  securityLogger.warning(
                      LocalizedStrings.ServerHandShakeProcessor_0_UNEXPECTED_CERTIFICATE_TYPE_1_FOR_PROXYID_2,
                      new Object[] {connection.getName(), certs[0].getType(), proxyId});
              }
            } catch (SSLPeerUnverifiedException ex) {
              // this is the case where client has not verified itself
              // i.e. not in mutual authentication mode
              if (securityLogger.errorEnabled())
                securityLogger.error(
                    LocalizedStrings.ServerHandShakeProcessor_SSL_EXCEPTION_SHOULD_NOT_HAVE_HAPPENED,
                    ex);
              connection.setPrincipal(null);//TODO:hitesh ??
            }
          }
        }
        */
         long uniqueId = setAuthAttributes(connection);
         connection.setUserAuthId(uniqueId);//for older clients < 6.5

      }
    }
    catch (SocketTimeoutException timeout) {
      logger.warn(LocalizedMessage.create(
        LocalizedStrings.ServerHandShakeProcessor_0_HANDSHAKE_REPLY_CODE_TIMEOUT_NOT_RECEIVED_WITH_IN_1_MS,
        new Object[] {connection.getName(), Integer.valueOf(handShakeTimeout)}));
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    }
    catch (EOFException e) {
      // no need to warn client just gave up on this server before we could
      // handshake
      logger.info("{} {}", connection.getName(), e);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    }
    catch (SocketException e) { // no need to warn client just gave up on this
      // server before we could handshake
      logger.info("{} {}", connection.getName(), e);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    }
    catch (IOException e) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.ServerHandShakeProcessor_0_RECEIVED_NO_HANDSHAKE_REPLY_CODE,
          connection.getName()), e);
      connection.stats.incFailedConnectionAttempts();
      connection.cleanup();
      return false;
    }
    catch (AuthenticationRequiredException noauth) {
      String exStr = noauth.getLocalizedMessage();
      if (noauth.getCause() != null) {
        exStr += " : " + noauth.getCause().getLocalizedMessage();
      }
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(
            LocalizedStrings.ONE_ARG, 
            connection.getName() + ": Security exception: " + exStr);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.refuseHandshake(noauth.getMessage(),
          HandShake.REPLY_EXCEPTION_AUTHENTICATION_REQUIRED);
      connection.cleanup();
      return false;
    }
    catch (AuthenticationFailedException failed) {
      String exStr = failed.getLocalizedMessage();
      if (failed.getCause() != null) {
        exStr += " : " + failed.getCause().getLocalizedMessage();
      }
      if (securityLogWriter.warningEnabled()) {
        securityLogWriter.warning(
          LocalizedStrings.ONE_ARG,
          connection.getName() + ": Security exception: " + exStr);
      }
      connection.stats.incFailedConnectionAttempts();
      connection.refuseHandshake(failed.getMessage(),
          HandShake.REPLY_EXCEPTION_AUTHENTICATION_FAILED);
      connection.cleanup();
      return false;
    }
    catch (Exception ex) {
      logger.warn("{} {}", connection.getName(), ex.getLocalizedMessage());
      connection.stats.incFailedConnectionAttempts();
      connection.refuseHandshake(ex.getMessage(), REPLY_REFUSED);
      connection.cleanup();
      return false;
    }
    return true;
  }
  
  public static long setAuthAttributes(ServerConnection connection)
    throws Exception{
    try {
      logger.debug("setAttributes()");
      Principal principal = ((HandShake)connection.getHandshake()).verifyCredentials();
      connection.setPrincipal(principal);//TODO:hitesh is this require now ???
      return getUniqueId(connection, principal);
    }catch(Exception ex) {
      throw ex;
    }
  }
  
  public static long getUniqueId(ServerConnection connection, Principal principal ) 
  throws Exception{
    try {
      InternalLogWriter securityLogWriter = connection.getSecurityLogWriter();
      DistributedSystem system = connection.getDistributedSystem();
      Properties systemProperties = system.getProperties();
      //hitesh:auth callbacks
      String authzFactoryName = systemProperties
          .getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME);
      String postAuthzFactoryName = systemProperties
          .getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME);
      AuthorizeRequest authzRequest = null;
      AuthorizeRequestPP postAuthzRequest = null;
      
      if (authzFactoryName != null && authzFactoryName.length() > 0) {
        if (securityLogWriter.fineEnabled())
          securityLogWriter.fine(connection.getName()
              + ": Setting pre-process authorization callback to: "
              + authzFactoryName);
        if (principal == null) {
          if (securityLogWriter.warningEnabled()) {
            securityLogWriter.warning(
                LocalizedStrings.ServerHandShakeProcessor_0_AUTHORIZATION_ENABLED_BUT_AUTHENTICATION_CALLBACK_1_RETURNED_WITH_NULL_CREDENTIALS_FOR_PROXYID_2,
                new Object[] {connection.getName(), DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, connection.getProxyID()});
          }
        }
        authzRequest = new AuthorizeRequest(authzFactoryName,
            connection.getProxyID(), principal, connection.getCache());
       // connection.setAuthorizeRequest(authzRequest);
      }
      if (postAuthzFactoryName != null && postAuthzFactoryName.length() > 0) {
        if (securityLogWriter.fineEnabled())
          securityLogWriter.fine(connection.getName()
              + ": Setting post-process authorization callback to: "
              + postAuthzFactoryName);
        if (principal == null) {
          if (securityLogWriter.warningEnabled()) {
            securityLogWriter.warning(
              LocalizedStrings.ServerHandShakeProcessor_0_POSTPROCESS_AUTHORIZATION_ENABLED_BUT_NO_AUTHENTICATION_CALLBACK_2_IS_CONFIGURED,
              new Object[] {connection.getName(), DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME});
          }      
        }
        postAuthzRequest = new AuthorizeRequestPP(
            postAuthzFactoryName, connection.getProxyID(), principal, connection.getCache());
       // connection.setPostAuthorizeRequest(postAuthzRequest);
      }
      return connection.setUserAuthorizeAndPostAuthorizeRequest(authzRequest, postAuthzRequest);
      }catch(Exception ex) {
        throw ex;
      }
  }
  
  private static Version readClientVersion(ServerConnection connection)
      throws IOException, VersionException {
    
    Socket socket = connection.getSocket();
    int timeout = connection.getHandShakeTimeout();

    int soTimeout = -1;
    try {
      soTimeout = socket.getSoTimeout();
      socket.setSoTimeout(timeout);
      InputStream is = socket.getInputStream();
      short clientVersionOrdinal = Version.readOrdinalFromInputStream(is);
      if (clientVersionOrdinal == -1) {
        throw new EOFException(
          LocalizedStrings.ServerHandShakeProcessor_HANDSHAKEREADER_EOF_REACHED_BEFORE_CLIENT_VERSION_COULD_BE_READ.toLocalizedString()); 
      }
      Version clientVersion = null;      
      try{
        clientVersion = Version.fromOrdinal(clientVersionOrdinal, true);
      }
      catch (UnsupportedVersionException uve) {
        // Allows higher version of wan site to connect to server
        if(connection.getCommunicationMode() == Acceptor.GATEWAY_TO_GATEWAY 
            && ! (clientVersionOrdinal == Version.NOT_SUPPORTED_ORDINAL)) {
          return Acceptor.VERSION;
        } else {
          SocketAddress sa = socket.getRemoteSocketAddress();
          String sInfo = "";
          if (sa != null) {
            sInfo = " Client: " + sa.toString() + ".";
          }
          throw new UnsupportedVersionException(uve.getMessage() + sInfo);
        }
      }
    
      if (!clientVersion.compatibleWith(Acceptor.VERSION)) {
        throw new IncompatibleVersionException(clientVersion, Acceptor.VERSION);//we can throw this to restrict
      }                                                 // Backward Compatibilty Support to limited no of versions          
      return clientVersion;                             
    } finally {
      if (soTimeout != -1) {
        try {        
          socket.setSoTimeout(soTimeout);
        }
        catch (IOException ignore) {
        }
      }
    }
  }
}
