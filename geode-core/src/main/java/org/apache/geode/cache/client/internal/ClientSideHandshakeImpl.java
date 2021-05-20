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

import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLSocket;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.CancelCriterion;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.GatewayConfigurationException;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.EncryptorImpl;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.internal.serialization.VersioningIO;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;

public class ClientSideHandshakeImpl extends Handshake implements ClientSideHandshake {
  /**
   * If true, the client has configured multi-user security, meaning that each thread holds its own
   * security principal.
   */
  private final boolean multiuserSecureMode;

  /**
   * Another test hook, holding a version ordinal that is higher than CURRENT
   */
  @MutableForTesting
  private static short overrideClientVersion = -1;

  private final byte replyCode;

  @Override
  protected byte getReplyCode() {
    return replyCode;
  }

  public ClientSideHandshakeImpl(ClientProxyMembershipID proxyId,
      InternalDistributedSystem distributedSystem, SecurityService securityService,
      boolean multiuserSecureMode) {
    this.multiuserSecureMode = multiuserSecureMode;
    id = proxyId;
    system = distributedSystem;
    this.securityService = securityService;
    replyCode = REPLY_OK;
    setOverrides();
    credentials = null;
    encryptor = new EncryptorImpl(distributedSystem.getSecurityLogWriter());
  }

  /**
   * Clone a HandShake to be used in creating other connections
   */
  public ClientSideHandshakeImpl(ClientSideHandshakeImpl handshake) {
    super(handshake);
    multiuserSecureMode = handshake.multiuserSecureMode;
    replyCode = handshake.getReplyCode();
  }

  public static void setVersionForTesting(short ver) {
    if (ver > KnownVersion.CURRENT_ORDINAL) {
      overrideClientVersion = ver;
    } else {
      currentClientVersion =
          Versioning.getKnownVersionOrDefault(Versioning.getVersion(ver),
              KnownVersion.CURRENT);
      overrideClientVersion = -1;
    }
  }

  private void setOverrides() {
    clientConflation = determineClientConflation();
    overrides = new byte[] {clientConflation};
  }

  // used by the client side
  private byte determineClientConflation() {
    byte result = CONFLATION_DEFAULT;

    String clientConflationValue = system.getProperties().getProperty(CONFLATE_EVENTS);
    if (DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_ON
        .equalsIgnoreCase(clientConflationValue)) {
      result = CONFLATION_ON;
    } else if (DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_OFF
        .equalsIgnoreCase(clientConflationValue)) {
      result = CONFLATION_OFF;
    }
    return result;
  }


  /**
   * Return fake, temporary DistributedMember to represent the other vm this vm is connecting to
   *
   * @param sock the socket this handshake is operating on
   * @return temporary id to reprent the other vm
   */
  private InternalDistributedMember getIDForSocket(Socket sock) {
    return new InternalDistributedMember(sock.getInetAddress(), sock.getPort(), false);
  }

  /**
   * Client-side handshake with a Server
   */
  @Override
  public ServerQueueStatus handshakeWithServer(Connection conn, ServerLocation location,
      CommunicationMode communicationMode) throws IOException, AuthenticationRequiredException,
      AuthenticationFailedException, ServerRefusedConnectionException {
    try {
      Socket sock = conn.getSocket();
      DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
      final InputStream in = sock.getInputStream();
      DataInputStream dis = new DataInputStream(in);
      InternalDistributedMember member = getIDForSocket(sock);
      // if running in a loner system, use the new port number in the ID to
      // help differentiate from other clients
      DistributionManager dm = ((InternalDistributedSystem) system).getDistributionManager();
      final InternalDistributedMember idm = dm.getDistributionManagerId();
      synchronized (idm) {
        if (idm.getMembershipPort() == 0 && dm instanceof LonerDistributionManager) {
          int port = sock.getLocalPort();
          ((LonerDistributionManager) dm).updateLonerPort(port);
          id.updateID(dm.getDistributionManagerId());
        }
      }
      if (communicationMode.isWAN()) {
        credentials = getCredentials(member);
      }
      byte intermediateAcceptanceCode = write(dos, dis, communicationMode, REPLY_OK,
          clientReadTimeout, null, credentials, member, false);

      String authInit = system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
      if (!communicationMode.isWAN()
          && intermediateAcceptanceCode != REPLY_AUTH_NOT_REQUIRED
          && (StringUtils.isNotBlank(authInit) || multiuserSecureMode)) {
        location.compareAndSetRequiresCredentials(true);
      }
      // Read the acceptance code
      byte acceptanceCode = dis.readByte();
      if (acceptanceCode == (byte) 21 && !(sock instanceof SSLSocket)) {
        // This is likely the case of server setup with SSL and client not using
        // SSL
        throw new AuthenticationRequiredException(
            "Server expecting SSL connection");
      }
      if (acceptanceCode == REPLY_SERVER_IS_LOCATOR) {
        throw new GemFireConfigException("Improperly configured client detected.  " + "Server at "
            + location + " is actually a locator.  Use addPoolLocator to configure locators.");
      }

      // Successful handshake for GATEWAY_TO_GATEWAY mode sets the peer version in connection
      if (communicationMode.isWAN() && !(acceptanceCode == REPLY_EXCEPTION_AUTHENTICATION_REQUIRED
          || acceptanceCode == REPLY_EXCEPTION_AUTHENTICATION_FAILED)) {
        short wanSiteVersion = VersioningIO.readOrdinal(dis);
        conn.setWanSiteVersion(wanSiteVersion);
        // establish a versioned stream for the other site, if necessary
        if (wanSiteVersion < KnownVersion.CURRENT_ORDINAL) {
          dis = new VersionedDataInputStream(dis, Versioning
              .getKnownVersionOrDefault(Versioning.getVersion(wanSiteVersion),
                  KnownVersion.CURRENT));
        }
      }

      // No need to check for return value since DataInputStream already throws
      // EOFException in case of EOF
      final byte endpointType = dis.readByte();
      final int queueSize = dis.readInt();

      member = readServerMember(dis);

      final ServerQueueStatus serverQStatus =
          new ServerQueueStatus(endpointType, queueSize, member);

      // Read the message (if any)
      readMessage(dis, dos, acceptanceCode, member);

      // Read delta-propagation property value from server.
      if (!communicationMode.isWAN()) {
        ((InternalDistributedSystem) system).setDeltaEnabledOnServer(dis.readBoolean());
      }

      // validate that the remote side has a different distributed system id.
      if (communicationMode.isWAN()) {
        int remoteDistributedSystemId = in.read();
        int localDistributedSystemId =
            ((InternalDistributedSystem) system).getDistributionManager().getDistributedSystemId();
        if (localDistributedSystemId >= 0
            && localDistributedSystemId == remoteDistributedSystemId) {
          throw new GatewayConfigurationException(
              "Remote WAN site's distributed system id " + remoteDistributedSystemId
                  + " matches this sites distributed system id " + localDistributedSystemId);
        }
      }

      // Read the PDX registry size from the remote size
      if (communicationMode.isWAN()) {
        int remotePdxSize = dis.readInt();
        serverQStatus.setPdxSize(remotePdxSize);
      }

      return serverQStatus;
    } catch (IOException ex) {
      CancelCriterion stopper = system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
  }

  private InternalDistributedMember readServerMember(DataInputStream p_dis) throws IOException {

    byte[] memberBytes = DataSerializer.readByteArray(p_dis);
    KnownVersion v = StaticSerialization.getVersionForDataStreamOrNull(p_dis);
    ByteArrayDataInput dis = new ByteArrayDataInput(memberBytes, v);
    try {
      return DataSerializer.readObject(dis);
    } catch (EOFException e) {
      throw e;
    } catch (Exception e) {
      throw new InternalGemFireException(
          "Unable to deserialize member", e);
    }
  }


  /**
   * Used by client-side CacheClientUpdater to handshake with a server in order to receive messages
   * generated by subscriptions (register-interest, continuous query)
   */
  @Override
  public ServerQueueStatus handshakeWithSubscriptionFeed(Socket sock, boolean isPrimary)
      throws IOException, AuthenticationRequiredException, AuthenticationFailedException,
      ServerRefusedConnectionException, ClassNotFoundException {
    try {
      final DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
      final InputStream in = sock.getInputStream();
      final DataInputStream dis = new DataInputStream(in);
      final DistributedMember member = getIDForSocket(sock);
      if (!multiuserSecureMode) {
        credentials = getCredentials(member);
      }
      final CommunicationMode mode = isPrimary ? CommunicationMode.PrimaryServerToClient
          : CommunicationMode.SecondaryServerToClient;
      write(dos, dis, mode, REPLY_OK, 0, new ArrayList<>(), credentials, member, true);

      // Wait here for a reply before continuing. This ensures that the client
      // updater is registered with the server before continuing.
      final byte acceptanceCode = dis.readByte();
      if (acceptanceCode == (byte) 21 && !(sock instanceof SSLSocket)) {
        // This is likely the case of server setup with SSL and client not using
        // SSL
        throw new AuthenticationRequiredException(
            "Server expecting SSL connection");
      }

      final byte endpointType = dis.readByte();
      final int queueSize = dis.readInt();

      // Read the message (if any)
      readMessage(dis, dos, acceptanceCode, member);

      final Map<Integer, List<String>> instantiatorMap = DataSerializer.readHashMap(dis);
      for (final Map.Entry<Integer, List<String>> entry : instantiatorMap.entrySet()) {
        final Integer id = entry.getKey();
        final List<String> instantiatorArguments = entry.getValue();
        InternalInstantiator.register(instantiatorArguments.get(0), instantiatorArguments.get(1),
            id, false);
      }

      final Map<Integer, String> dataSerializersMap = DataSerializer.readHashMap(dis);
      for (final Map.Entry<Integer, String> entry : dataSerializersMap.entrySet()) {
        InternalDataSerializer.register(entry.getValue(), false, null, null, entry.getKey());
      }
      final Map<Integer, List<String>> dsToSupportedClassNames = DataSerializer.readHashMap(dis);
      InternalDataSerializer.updateSupportedClassesMap(dsToSupportedClassNames);

      // the server's ping interval is only sent to subscription feeds so we can't read it as
      // part of a "standard" server response along with the other status data.
      final int pingInterval = dis.readInt();
      return new ServerQueueStatus(endpointType, queueSize, member, pingInterval);

    } catch (IOException | ClassNotFoundException ex) {
      CancelCriterion stopper = system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
  }

  /**
   * client-to-server handshake. Nothing is sent to the server prior to invoking this method.
   */
  private byte write(DataOutputStream dos, DataInputStream dis, CommunicationMode communicationMode,
      int replyCode, int readTimeout, List<String> ports, Properties p_credentials,
      DistributedMember member, boolean isCallbackConnection) throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, KnownVersion.CURRENT);
    byte acceptanceCode = -1;
    try {
      hdos.writeByte(communicationMode.getModeNumber());
      if (overrideClientVersion > 0) {
        // for testing
        VersioningIO.writeOrdinal(hdos, overrideClientVersion, true);
      } else {
        VersioningIO.writeOrdinal(hdos,
            communicationMode.isWAN() ? KnownVersion.CURRENT_ORDINAL
                : currentClientVersion.ordinal(),
            true);
      }

      hdos.writeByte(replyCode);
      if (ports != null) {
        hdos.writeInt(ports.size());
        for (String port : ports) {
          hdos.writeInt(Integer.parseInt(port));
        }
      } else {
        hdos.writeInt(readTimeout);
      }
      // we do not know the receiver's version at this point, but the on-wire
      // form of InternalDistributedMember changed in 9.0, so we must serialize
      // it using the previous version
      DataOutput idOut = new VersionedDataOutputStream(hdos, KnownVersion.GFE_81);
      DataSerializer.writeObject(id, idOut);

      byte[] overrides = getOverrides();
      for (final byte override : overrides) {
        hdos.writeByte(override);
      }

      if (isCallbackConnection || communicationMode.isWAN()) {
        if (isCallbackConnection && multiuserSecureMode && !communicationMode.isWAN()) {
          hdos.writeByte(SECURITY_MULTIUSER_NOTIFICATIONCHANNEL);
          hdos.flush();
          dos.write(hdos.toByteArray());
          dos.flush();
        } else {
          writeCredentials(dos, dis, p_credentials, ports != null, member, hdos);
        }
      } else {
        String authInitMethod = system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
        acceptanceCode = writeCredential(dos, dis, authInitMethod, ports != null, member, hdos);
      }
    } finally {
      hdos.close();
    }
    return acceptanceCode;
  }

  @Override
  protected byte writeCredential(DataOutputStream dos, DataInputStream dis, String authInit,
      boolean isNotification, DistributedMember member, HeapDataOutputStream heapdos)
      throws IOException, GemFireSecurityException {

    if (!multiuserSecureMode && (authInit == null || authInit.length() == 0)) {
      // No credentials indicator
      heapdos.writeByte(CREDENTIALS_NONE);
      heapdos.flush();
      dos.write(heapdos.toByteArray());
      dos.flush();
      return -1;
    }

    return super.writeCredential(dos, dis, authInit, isNotification, member, heapdos);
  }

  @Override
  public Encryptor getEncryptor() {
    return encryptor;
  }
}
