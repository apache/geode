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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.internal.ClientSideHandshakeImpl;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassLoadUtil;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.ConnectionProxy;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.CallbackInstantiator;
import org.apache.geode.internal.security.Credentials;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.Authenticator;
import org.apache.geode.security.GemFireSecurityException;

public abstract class Handshake {
  private static final Logger logger = LogService.getLogger();

  protected static final byte REPLY_OK = (byte) 59;

  protected static final byte REPLY_REFUSED = (byte) 60;

  protected static final byte REPLY_INVALID = (byte) 61;

  protected static final byte REPLY_EXCEPTION_AUTHENTICATION_REQUIRED = (byte) 62;

  protected static final byte REPLY_EXCEPTION_AUTHENTICATION_FAILED = (byte) 63;

  protected static final byte REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT = (byte) 64;

  protected static final byte REPLY_WAN_CREDENTIALS = (byte) 65;

  protected static final byte REPLY_AUTH_NOT_REQUIRED = (byte) 66;

  public static final byte REPLY_SERVER_IS_LOCATOR = (byte) 67;
  /**
   * Test hook for client version support
   *
   * @since GemFire 5.7
   */
  protected static Version currentClientVersion = ConnectionProxy.VERSION;

  protected SecurityService securityService;

  /** This is used as part of our hash, so it must be a stable value */
  protected abstract byte getReplyCode();

  protected int clientReadTimeout = PoolFactory.DEFAULT_READ_TIMEOUT;

  protected DistributedSystem system;

  protected ClientProxyMembershipID id;

  protected Properties credentials;

  protected EncryptorImpl encryptor;

  // Security mode flags

  /** No credentials being sent */
  public static final byte CREDENTIALS_NONE = (byte) 0;

  /** Credentials being sent without encryption on the wire */
  public static final byte CREDENTIALS_NORMAL = (byte) 1;

  /** Credentials being sent with Diffie-Hellman key encryption */
  public static final byte CREDENTIALS_DHENCRYPT = (byte) 2;

  public static final byte SECURITY_MULTIUSER_NOTIFICATIONCHANNEL = (byte) 3;

  public static final String PUBLIC_KEY_FILE_PROP = "security-client-kspath";

  public static final String PUBLIC_KEY_PASSWD_PROP = "security-client-kspasswd";

  public static final String PRIVATE_KEY_FILE_PROP = "security-server-kspath";

  public static final String PRIVATE_KEY_ALIAS_PROP = "security-server-ksalias";

  public static final String PRIVATE_KEY_PASSWD_PROP = "security-server-kspasswd";

  /** @since GemFire 5.7 */
  public static final byte CONFLATION_DEFAULT = 0;
  /** @since GemFire 5.7 */
  public static final byte CONFLATION_ON = 1;
  /** @since GemFire 5.7 */
  public static final byte CONFLATION_OFF = 2;
  /** @since GemFire 5.7 */
  protected byte clientConflation = CONFLATION_DEFAULT;

  /**
   * @since GemFire 6.0.3 List of per client property override bits.
   */
  protected byte[] overrides;

  /**
   * Test hooks for per client conflation
   *
   * @since GemFire 5.7
   */
  public static byte clientConflationForTesting = 0;
  public static boolean setClientConflationForTesting = false;

  /** Constructor used for subclasses */
  protected Handshake() {}

  /**
   * Clone a HandShake to be used in creating other connections
   */
  protected Handshake(Handshake handshake) {
    this.clientConflation = handshake.clientConflation;
    this.clientReadTimeout = handshake.clientReadTimeout;
    this.credentials = handshake.credentials;
    this.overrides = handshake.overrides;
    this.system = handshake.system;
    this.id = handshake.id;
    this.securityService = handshake.securityService;
    this.encryptor = new EncryptorImpl(handshake.encryptor);
  }

  protected void setClientConflation(byte value) {
    this.clientConflation = value;
    switch (this.clientConflation) {
      case CONFLATION_DEFAULT:
      case CONFLATION_OFF:
      case CONFLATION_ON:
        break;
      default:
        throw new IllegalArgumentException("Illegal clientConflation");
    }
  }

  protected byte[] getOverrides() {
    return overrides;
  }

  protected void setOverrides(byte[] values) {
    byte override = values[0];
    setClientConflation(((byte) (override & 0x03)));
  }

  // used by CacheClientNotifier's handshake reading code
  public static byte[] extractOverrides(byte[] values) {
    byte override = values[0];
    byte[] overrides = new byte[1];
    for (int item = 0; item < overrides.length; item++) {
      overrides[item] = (byte) (override & 0x03);
      override = (byte) (override >>> 2);
    }
    return overrides;
  }

  /**
   * This method writes what readCredential() method expects to read. (Note the use of singular
   * credential). It is similar to writeCredentials(), except that it doesn't write
   * credential-properties.
   *
   * This is only used by the {@link ClientSideHandshakeImpl}.
   */
  protected byte writeCredential(DataOutputStream dos, DataInputStream dis, String authInit,
      boolean isNotification, DistributedMember member, HeapDataOutputStream heapdos)
      throws IOException, GemFireSecurityException {

    if (!encryptor.isEnabled()) {
      heapdos.writeByte(CREDENTIALS_NORMAL);
      encryptor.setAppSecureMode(CREDENTIALS_NORMAL);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return -1;
    }
    byte acceptanceCode = -1;
    acceptanceCode = encryptor.writeEncryptedCredential(dos, dis, heapdos);
    if (acceptanceCode != REPLY_OK && acceptanceCode != REPLY_AUTH_NOT_REQUIRED) {
      // Ignore the useless data
      dis.readByte();
      dis.readInt();
      if (!isNotification) {
        DataSerializer.readByteArray(dis);
      }
      readMessage(dis, dos, acceptanceCode, member);
    }
    dos.flush();
    return acceptanceCode;
  }

  public void writeCredentials(DataOutputStream dos, DataInputStream dis, Properties p_credentials,
      boolean isNotification, DistributedMember member)
      throws IOException, GemFireSecurityException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    try {
      writeCredentials(dos, dis, p_credentials, isNotification, member, hdos);
    } finally {
      hdos.close();
    }
  }

  /**
   * This assumes that authentication is the last piece of info in handshake
   */
  public void writeCredentials(DataOutputStream dos, DataInputStream dis, Properties p_credentials,
      boolean isNotification, DistributedMember member, HeapDataOutputStream heapdos)
      throws IOException, GemFireSecurityException {

    if (p_credentials == null) {
      // No credentials indicator
      heapdos.writeByte(CREDENTIALS_NONE);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return;
    }

    if (!encryptor.isEnabled()) {
      // Normal credentials without encryption indicator
      heapdos.writeByte(CREDENTIALS_NORMAL);
      DataSerializer.writeProperties(p_credentials, heapdos);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return;
    }

    byte acceptanceCode = encryptor.writeEncryptedCredentials(dos, dis, p_credentials, heapdos);
    if (acceptanceCode != REPLY_OK && acceptanceCode != REPLY_AUTH_NOT_REQUIRED) {
      // Ignore the useless data
      dis.readByte();
      dis.readInt();
      if (!isNotification) {
        DataSerializer.readByteArray(dis);
      }
      readMessage(dis, dos, acceptanceCode, member);
    }
    dos.flush();
  }

  /**
   * Throws AuthenticationRequiredException if authentication is required but there are no
   * credentials.
   */
  static void throwIfMissingRequiredCredentials(boolean requireAuthentication,
      boolean hasCredentials) {
    if (requireAuthentication && !hasCredentials) {
      throw new AuthenticationRequiredException(
          "No security credentials are provided");
    }
  }

  // This assumes that authentication is the last piece of info in handshake
  Properties readCredential(DataInputStream dis, DataOutputStream dos, DistributedSystem system)
      throws GemFireSecurityException, IOException {

    Properties credentials = null;
    boolean requireAuthentication = securityService.isClientSecurityRequired();
    try {
      byte secureMode = dis.readByte();
      throwIfMissingRequiredCredentials(requireAuthentication, secureMode != CREDENTIALS_NONE);
      if (secureMode == CREDENTIALS_NORMAL) {
        encryptor.setAppSecureMode(CREDENTIALS_NORMAL);
      } else if (secureMode == CREDENTIALS_DHENCRYPT) {
        encryptor.readEncryptedCredentials(dis, dos, system, requireAuthentication);
      }
    } catch (IOException ex) {
      throw ex;
    } catch (GemFireSecurityException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AuthenticationFailedException(
          "Failure in reading credentials", ex);
    }
    return credentials;
  }


  protected void readMessage(DataInputStream dis, DataOutputStream dos, byte acceptanceCode,
      DistributedMember member) throws IOException, AuthenticationRequiredException,
      AuthenticationFailedException, ServerRefusedConnectionException {

    String message = dis.readUTF();
    if (message.length() == 0 && acceptanceCode != REPLY_WAN_CREDENTIALS) {
      return; // success
    }

    switch (acceptanceCode) {
      case REPLY_EXCEPTION_AUTHENTICATION_REQUIRED:
        throw new AuthenticationRequiredException(message);
      case REPLY_EXCEPTION_AUTHENTICATION_FAILED:
        throw new AuthenticationFailedException(message);
      case REPLY_EXCEPTION_DUPLICATE_DURABLE_CLIENT:
        throw new ServerRefusedConnectionException(member, message);
      case REPLY_WAN_CREDENTIALS:
        checkIfAuthenticWanSite(dis, dos, member);
        break;
      default:
        throw new ServerRefusedConnectionException(member, message);
    }
  }

  public boolean isOK() {
    return getReplyCode() == REPLY_OK;
  }

  public void setClientReadTimeout(int clientReadTimeout) {
    this.clientReadTimeout = clientReadTimeout;
  }

  public int getClientReadTimeout() {
    return this.clientReadTimeout;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param other the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (!(other instanceof Handshake))
      return false;
    final Handshake that = (Handshake) other;

    if (this.id.isSameDSMember(that.id) && getReplyCode() == that.getReplyCode()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int mult = 37;

    int result = this.id.hashCode();
    result = mult * result + getReplyCode();

    return result;
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer().append("HandShake@").append(System.identityHashCode(this))
        .append(" code: ").append(getReplyCode());
    if (this.id != null) {
      buf.append(" identity: ");
      buf.append(this.id.toString());
    }
    return buf.toString();
  }

  public ClientProxyMembershipID getMembershipId() {
    return this.id;
  }

  public static Properties getCredentials(String authInitMethod, Properties securityProperties,
      DistributedMember server, boolean isPeer, InternalLogWriter logWriter,
      InternalLogWriter securityLogWriter) throws AuthenticationRequiredException {

    Properties credentials = null;
    // if no authInit, Try to extract the credentials directly from securityProps
    if (StringUtils.isBlank(authInitMethod)) {
      return Credentials.getCredentials(securityProperties);
    }

    // if authInit exists
    try {
      AuthInitialize auth =
          CallbackInstantiator.getObjectOfType(authInitMethod, AuthInitialize.class);
      auth.init(logWriter, securityLogWriter);
      try {
        credentials = auth.getCredentials(securityProperties, server, isPeer);
      } finally {
        auth.close();
      }
    } catch (GemFireSecurityException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AuthenticationRequiredException(
          String.format("Failed to acquire AuthInitialize method %s",
              authInitMethod),
          ex);
    }
    return credentials;
  }

  protected Properties getCredentials(DistributedMember member) {

    String authInitMethod = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
    return getCredentials(authInitMethod, this.system.getSecurityProperties(), member, false,
        (InternalLogWriter) this.system.getLogWriter(),
        (InternalLogWriter) this.system.getSecurityLogWriter());
  }

  /**
   * this static method is used by CacheClientNotifier in registerClient when creating a queue for a
   * client. This assumes that authentication is the last piece of info in handshake
   */
  public static Properties readCredentials(DataInputStream dis, DataOutputStream dos,
      DistributedSystem system, SecurityService securityService)
      throws GemFireSecurityException, IOException {

    boolean requireAuthentication = securityService.isClientSecurityRequired();
    Properties credentials = null;
    try {
      byte secureMode = dis.readByte();
      throwIfMissingRequiredCredentials(requireAuthentication, secureMode != CREDENTIALS_NONE);
      if (secureMode == CREDENTIALS_NORMAL) {
        if (requireAuthentication) {
          credentials = DataSerializer.readProperties(dis);
        } else {
          DataSerializer.readProperties(dis); // ignore the credentials
        }
      } else if (secureMode == CREDENTIALS_DHENCRYPT) {
        credentials = EncryptorImpl.getDecryptedCredentials(dis, dos, system, requireAuthentication,
            credentials);
      } else if (secureMode == SECURITY_MULTIUSER_NOTIFICATIONCHANNEL) {
        // hitesh there will be no credential CCP will get credential(Principal) using
        // ServerConnection..
        logger.debug("readCredential where multiuser mode creating callback connection");
      }
    } catch (IOException ex) {
      throw ex;
    } catch (GemFireSecurityException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AuthenticationFailedException(
          "Failure in reading credentials", ex);
    }
    return credentials;
  }

  /**
   * this could return either a Subject or a Principal depending on if it's integrated security or
   * not
   */
  public static Object verifyCredentials(String authenticatorMethod, Properties credentials,
      Properties securityProperties, InternalLogWriter logWriter,
      InternalLogWriter securityLogWriter, DistributedMember member,
      SecurityService securityService)
      throws AuthenticationRequiredException, AuthenticationFailedException {

    if (!AcceptorImpl.isAuthenticationRequired()) {
      return null;
    }

    Authenticator auth = null;
    try {
      if (securityService.isIntegratedSecurity()) {
        return securityService.login(credentials);
      } else {
        Method instanceGetter = ClassLoadUtil.methodFromName(authenticatorMethod);
        auth = (Authenticator) instanceGetter.invoke(null, (Object[]) null);
        auth.init(securityProperties, logWriter, securityLogWriter);
        return auth.authenticate(credentials, member);
      }
    } catch (AuthenticationFailedException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AuthenticationFailedException(ex.getMessage(), ex);
    } finally {
      if (auth != null) {
        auth.close();
      }
    }
  }

  public Object verifyCredentials()
      throws AuthenticationRequiredException, AuthenticationFailedException {

    String methodName = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    return verifyCredentials(methodName, this.credentials, this.system.getSecurityProperties(),
        (InternalLogWriter) this.system.getLogWriter(),
        (InternalLogWriter) this.system.getSecurityLogWriter(), this.id.getDistributedMember(),
        this.securityService);
  }

  private void checkIfAuthenticWanSite(DataInputStream dis, DataOutputStream dos,
      DistributedMember member) throws GemFireSecurityException, IOException {

    if (this.credentials == null) {
      return;
    }
    String authenticator = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    Properties peerWanProps = readCredentials(dis, dos, this.system, this.securityService);
    verifyCredentials(authenticator, peerWanProps, this.system.getSecurityProperties(),
        (InternalLogWriter) this.system.getLogWriter(),
        (InternalLogWriter) this.system.getSecurityLogWriter(), member, this.securityService);
  }
}
