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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLSocket;

import org.apache.geode.CancelCriterion;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.InternalGemFireException;
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
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedDataOutputStream;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.EncryptorImpl;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.security.SecurityService;
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
    this.id = proxyId;
    this.system = distributedSystem;
    this.securityService = securityService;
    this.replyCode = REPLY_OK;
    setOverrides();
    this.credentials = null;
    this.encryptor = new EncryptorImpl(distributedSystem.getSecurityLogWriter());
  }

  /**
   * Clone a HandShake to be used in creating other connections
   */
  public ClientSideHandshakeImpl(ClientSideHandshakeImpl handshake) {
    super(handshake);
    this.multiuserSecureMode = handshake.multiuserSecureMode;
    this.replyCode = handshake.getReplyCode();
  }

  public static void setVersionForTesting(short ver) {
    if (ver > Version.CURRENT_ORDINAL) {
      overrideClientVersion = ver;
    } else {
      currentClientVersion = Version.fromOrdinalOrCurrent(ver);
      overrideClientVersion = -1;
    }
  }

  private void setOverrides() {
    this.clientConflation = determineClientConflation();

    // As of May 2009 ( GFE 6.0 ):
    // Note that this.clientVersion is used by server side for accepting
    // handshakes.
    // Client side handshake code uses this.currentClientVersion which can be
    // set via tests.
    if (currentClientVersion.compareTo(Version.GFE_603) >= 0) {
      this.overrides = new byte[] {this.clientConflation};
    }
  }

  // used by the client side
  private byte determineClientConflation() {
    byte result = CONFLATION_DEFAULT;

    String clientConflationValue = this.system.getProperties().getProperty(CONFLATE_EVENTS);
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
      ServerQueueStatus serverQStatus = null;
      Socket sock = conn.getSocket();
      DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
      final InputStream in = sock.getInputStream();
      DataInputStream dis = new DataInputStream(in);
      InternalDistributedMember member = getIDForSocket(sock);
      // if running in a loner system, use the new port number in the ID to
      // help differentiate from other clients
      DistributionManager dm = ((InternalDistributedSystem) this.system).getDistributionManager();
      InternalDistributedMember idm = dm.getDistributionManagerId();
      synchronized (idm) {
        if (idm.getPort() == 0 && dm instanceof LonerDistributionManager) {
          int port = sock.getLocalPort();
          ((LonerDistributionManager) dm).updateLonerPort(port);
          this.id.updateID(dm.getDistributionManagerId());
        }
      }
      if (communicationMode.isWAN()) {
        this.credentials = getCredentials(member);
      }
      byte intermediateAcceptanceCode = write(dos, dis, communicationMode, REPLY_OK,
          this.clientReadTimeout, null, this.credentials, member, false);

      String authInit = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
      if (!communicationMode.isWAN() && intermediateAcceptanceCode != REPLY_AUTH_NOT_REQUIRED
          && (authInit != null && authInit.length() != 0)) {
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
        short wanSiteVersion = Version.readOrdinal(dis);
        conn.setWanSiteVersion(wanSiteVersion);
        // establish a versioned stream for the other site, if necessary
        if (wanSiteVersion < Version.CURRENT_ORDINAL) {
          dis = new VersionedDataInputStream(dis, Version.fromOrdinalOrCurrent(wanSiteVersion));
        }
      }

      // No need to check for return value since DataInputStream already throws
      // EOFException in case of EOF
      byte endpointType = dis.readByte();
      int queueSize = dis.readInt();

      member = readServerMember(dis);

      serverQStatus = new ServerQueueStatus(endpointType, queueSize, member);

      // Read the message (if any)
      readMessage(dis, dos, acceptanceCode, member);

      // Read delta-propagation property value from server.
      // [sumedh] Static variable below? Client can connect to different
      // DSes with different values of this. It shoule be a member variable.
      if (!communicationMode.isWAN() && currentClientVersion.compareTo(Version.GFE_61) >= 0) {
        ((InternalDistributedSystem) system).setDeltaEnabledOnServer(dis.readBoolean());
      }

      // validate that the remote side has a different distributed system id.
      if (communicationMode.isWAN() && Version.GFE_66.compareTo(conn.getWanSiteVersion()) <= 0
          && currentClientVersion.compareTo(Version.GFE_66) >= 0) {
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
      if (communicationMode.isWAN() && Version.GFE_80.compareTo(conn.getWanSiteVersion()) <= 0
          && currentClientVersion.compareTo(Version.GFE_80) >= 0) {
        int remotePdxSize = dis.readInt();
        serverQStatus.setPdxSize(remotePdxSize);
      }

      return serverQStatus;
    } catch (IOException ex) {
      CancelCriterion stopper = this.system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
  }

  private InternalDistributedMember readServerMember(DataInputStream p_dis) throws IOException {

    byte[] memberBytes = DataSerializer.readByteArray(p_dis);
    ByteArrayInputStream bais = new ByteArrayInputStream(memberBytes);
    DataInputStream dis = new DataInputStream(bais);
    Version v = InternalDataSerializer.getVersionForDataStreamOrNull(p_dis);
    if (v != null) {
      dis = new VersionedDataInputStream(dis, v);
    }
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
    ServerQueueStatus serverQueueStatus = null;
    try {
      DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
      final InputStream in = sock.getInputStream();
      DataInputStream dis = new DataInputStream(in);
      DistributedMember member = getIDForSocket(sock);
      if (!this.multiuserSecureMode) {
        this.credentials = getCredentials(member);
      }
      CommunicationMode mode = isPrimary ? CommunicationMode.PrimaryServerToClient
          : CommunicationMode.SecondaryServerToClient;
      write(dos, dis, mode, REPLY_OK, 0, new ArrayList(), this.credentials, member, true);

      // Wait here for a reply before continuing. This ensures that the client
      // updater is registered with the server before continuing.
      byte acceptanceCode = dis.readByte();
      if (acceptanceCode == (byte) 21 && !(sock instanceof SSLSocket)) {
        // This is likely the case of server setup with SSL and client not using
        // SSL
        throw new AuthenticationRequiredException(
            "Server expecting SSL connection");
      }

      byte endpointType = dis.readByte();
      int queueSize = dis.readInt();

      // Read the message (if any)
      readMessage(dis, dos, acceptanceCode, member);

      // [sumedh] nothing more to be done for older clients used in tests
      // there is a difference in serializer map registration for >= 6.5.1.6
      // clients but that is not used in tests
      if (currentClientVersion.compareTo(Version.GFE_61) < 0) {
        return new ServerQueueStatus(endpointType, queueSize, member);
      }
      HashMap instantiatorMap = DataSerializer.readHashMap(dis);
      for (Iterator itr = instantiatorMap.entrySet().iterator(); itr.hasNext();) {
        Map.Entry instantiator = (Map.Entry) itr.next();
        Integer id = (Integer) instantiator.getKey();
        ArrayList instantiatorArguments = (ArrayList) instantiator.getValue();
        InternalInstantiator.register((String) instantiatorArguments.get(0),
            (String) instantiatorArguments.get(1), id, false);
      }

      HashMap dataSerializersMap = DataSerializer.readHashMap(dis);
      for (Iterator itr = dataSerializersMap.entrySet().iterator(); itr.hasNext();) {
        Map.Entry dataSerializer = (Map.Entry) itr.next();
        Integer id = (Integer) dataSerializer.getKey();
        InternalDataSerializer.register((String) dataSerializer.getValue(), false, null, null, id);
      }
      Map<Integer, List<String>> dsToSupportedClassNames = DataSerializer.readHashMap(dis);
      InternalDataSerializer.updateSupportedClassesMap(dsToSupportedClassNames);

      // the server's ping interval is only sent to subscription feeds so we can't read it as
      // part of a "standard" server response along with the other status data.
      int pingInterval = dis.readInt();
      serverQueueStatus = new ServerQueueStatus(endpointType, queueSize, member, pingInterval);

    } catch (IOException ex) {
      CancelCriterion stopper = this.system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    } catch (ClassNotFoundException ex) {
      CancelCriterion stopper = this.system.getCancelCriterion();
      stopper.checkCancelInProgress(null);
      throw ex;
    }
    return serverQueueStatus;
  }

  /**
   * client-to-server handshake. Nothing is sent to the server prior to invoking this method.
   */
  private byte write(DataOutputStream dos, DataInputStream dis, CommunicationMode communicationMode,
      int replyCode, int readTimeout, List ports, Properties p_credentials,
      DistributedMember member, boolean isCallbackConnection) throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    byte acceptanceCode = -1;
    try {
      hdos.writeByte(communicationMode.getModeNumber());
      if (overrideClientVersion > 0) {
        // for testing
        Version.writeOrdinal(hdos, overrideClientVersion, true);
      } else {
        Version.writeOrdinal(hdos, currentClientVersion.ordinal(), true);
      }

      hdos.writeByte(replyCode);
      if (ports != null) {
        hdos.writeInt(ports.size());
        for (int i = 0; i < ports.size(); i++) {
          hdos.writeInt(Integer.parseInt((String) ports.get(i)));
        }
      } else {
        hdos.writeInt(readTimeout);
      }
      // we do not know the receiver's version at this point, but the on-wire
      // form of InternalDistributedMember changed in 9.0, so we must serialize
      // it using the previous version
      DataOutput idOut = new VersionedDataOutputStream(hdos, Version.GFE_82);
      DataSerializer.writeObject(this.id, idOut);

      if (currentClientVersion.compareTo(Version.GFE_603) >= 0) {
        byte[] overrides = getOverrides();
        for (int bytes = 0; bytes < overrides.length; bytes++) {
          hdos.writeByte(overrides[bytes]);
        }
      } else {
        // write the client conflation setting byte
        if (setClientConflationForTesting) {
          hdos.writeByte(clientConflationForTesting);
        } else {
          hdos.writeByte(this.clientConflation);
        }
      }

      if (isCallbackConnection || communicationMode.isWAN()) {
        if (isCallbackConnection && this.multiuserSecureMode && !communicationMode.isWAN()) {
          hdos.writeByte(SECURITY_MULTIUSER_NOTIFICATIONCHANNEL);
          hdos.flush();
          dos.write(hdos.toByteArray());
          dos.flush();
        } else {
          writeCredentials(dos, dis, p_credentials, ports != null, member, hdos);
        }
      } else {
        String authInitMethod = this.system.getProperties().getProperty(SECURITY_CLIENT_AUTH_INIT);
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

    if (!this.multiuserSecureMode && (authInit == null || authInit.length() == 0)) {
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
