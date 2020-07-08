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

package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;

import java.util.Properties;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.command.PutUserCredentials;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.NotAuthorizedException;

/**
 * Authenticates this client (or a user) on a server. This op ideally should get executed
 * once-per-server.
 *
 * When multiuser-authentication is set to false, this op gets executed immedialtely after a
 * client-to-server connection is established.
 *
 * When multiuser-authentication is set to true, this op gets executed before the user attempts to
 * perform an op whose {@link AbstractOp#needsUserId()} returns true.
 *
 * @see PutUserCredentials
 * @see ProxyCache
 * @since GemFire 6.5
 */
public class AuthenticateUserOp {

  /**
   * Sends the auth credentials to the server. Used in single user mode of authentication.
   *
   * @param con The connection to use for this operation.
   * @param pool The connection pool to use for this operation.
   * @return Object unique user-id.
   */
  public static Object executeOn(Connection con, ExecutablePool pool) {
    AbstractOp op = new AuthenticateUserOpImpl(con);
    return pool.executeOn(con, op);
  }

  /**
   * Sends the auth credentials to the server for a particular user. Used in multiple user mode of
   * authentication.
   *
   * @param location The ServerLocation instance whose connection instance will be used to perform
   *        the operation.
   * @param pool The connection pool to use for this operation.
   * @return Object unique user-id.
   */
  public static Object executeOn(ServerLocation location, ExecutablePool pool,
      Properties securityProps) {
    AbstractOp op = new AuthenticateUserOpImpl(securityProps);
    return pool.executeOn(location, op);
  }

  private AuthenticateUserOp() {
    // no instances allowed
  }

  static class AuthenticateUserOpImpl extends AbstractOp {

    private Properties securityProperties = null;
    private boolean needsServerLocation = false;

    AuthenticateUserOpImpl(Connection con) {
      super(MessageType.USER_CREDENTIAL_MESSAGE, 1);
      byte[] credentialBytes;
      DistributedMember server = new InternalDistributedMember(con.getSocket().getInetAddress(),
          con.getSocket().getPort(), false);
      DistributedSystem sys = InternalDistributedSystem.getConnectedInstance();
      String authInitMethod = sys.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
      Properties tmpSecurityProperties = sys.getSecurityProperties();

      // LOG: following passes the DS API LogWriters into the security API
      Properties credentials = Handshake.getCredentials(authInitMethod, tmpSecurityProperties,
          server, false, (InternalLogWriter) sys.getLogWriter(),
          (InternalLogWriter) sys.getSecurityLogWriter());

      getMessage().setMessageHasSecurePartFlag();
      try (HeapDataOutputStream heapdos = new HeapDataOutputStream(KnownVersion.CURRENT)) {
        DataSerializer.writeProperties(credentials, heapdos);
        credentialBytes = ((ConnectionImpl) con).encryptBytes(heapdos.toByteArray());
      } catch (Exception e) {
        throw new ServerOperationException(e);
      }
      getMessage().addBytesPart(credentialBytes);
    }

    AuthenticateUserOpImpl(Properties securityProps) {
      this(securityProps, false);
    }

    AuthenticateUserOpImpl(Properties securityProps,
        boolean needsServer) {
      super(MessageType.USER_CREDENTIAL_MESSAGE, 1);
      securityProperties = securityProps;
      needsServerLocation = needsServer;

      getMessage().setMessageHasSecurePartFlag();
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
      byte[] secureBytes;
      hdos.writeLong(cnx.getConnectionID());
      if (securityProperties != null) {
        DistributedMember server = new InternalDistributedMember(cnx.getSocket().getInetAddress(),
            cnx.getSocket().getPort(), false);
        DistributedSystem sys = InternalDistributedSystem.getConnectedInstance();
        String authInitMethod = sys.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);

        Properties credentials = Handshake.getCredentials(authInitMethod, securityProperties,
            server, false, (InternalLogWriter) sys.getLogWriter(),
            (InternalLogWriter) sys.getSecurityLogWriter());
        byte[] credentialBytes;
        try (HeapDataOutputStream heapdos = new HeapDataOutputStream(KnownVersion.CURRENT)) {
          DataSerializer.writeProperties(credentials, heapdos);
          credentialBytes = ((ConnectionImpl) cnx).encryptBytes(heapdos.toByteArray());
        }
        getMessage().addBytesPart(credentialBytes);
      }
      try {
        secureBytes = ((ConnectionImpl) cnx).encryptBytes(hdos.toByteArray());
      } finally {
        hdos.close();
      }
      getMessage().setSecurePart(secureBytes);
      getMessage().send(false);
    }

    @Override
    public Object attempt(Connection cnx) throws Exception {
      if (cnx.getServer().getRequiresCredentials()) {
        return super.attempt(cnx);
      } else {
        return null;
      }
    }

    @Override
    protected Object processResponse(Message msg, Connection cnx) throws Exception {
      byte[] bytes;
      Part part = msg.getPart(0);
      final int msgType = msg.getMessageType();
      long userId = -1;
      if (msgType == MessageType.RESPONSE) {
        bytes = (byte[]) part.getObject();
        if (bytes.length == 0) {
          cnx.getServer().setRequiresCredentials(false);
        } else {
          cnx.getServer().setRequiresCredentials(true);
          byte[] decrypted = ((ConnectionImpl) cnx).decryptBytes(bytes);
          ByteArrayDataInput dis = new ByteArrayDataInput(decrypted);
          userId = dis.readLong();
        }
        if (needsServerLocation) {
          return new Object[] {cnx.getServer(), userId};
        } else {
          return userId;
        }
      } else if (msgType == MessageType.EXCEPTION) {
        Object result = part.getObject();
        String s = "While performing a remote authenticate";
        if (result instanceof AuthenticationFailedException) {
          final AuthenticationFailedException afe = (AuthenticationFailedException) result;
          if ("REPLY_REFUSED".equals(afe.getMessage())) {
            throw new AuthenticationFailedException(s, afe.getCause());
          } else {
            throw new AuthenticationFailedException(s, afe);
          }
        } else if (result instanceof AuthenticationRequiredException) {
          throw new AuthenticationRequiredException(s, (AuthenticationRequiredException) result);
        } else if (result instanceof NotAuthorizedException) {
          throw new NotAuthorizedException(s, (NotAuthorizedException) result);
        } else {
          throw new ServerOperationException(s, (Throwable) result);
        }
        // Get the exception toString part.
        // This was added for c++ thin client and not used in java
      } else if (isErrorResponse(msgType)) {
        throw new ServerOperationException(part.getString());
      } else {
        throw new InternalGemFireError("Unexpected message type " + MessageType.getString(msgType));
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGet();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGet(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      return null;
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }
  }

}
