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

import static org.apache.geode.cache.Region.SEPARATOR_CHAR;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class represents a ConnectionProxy of the CacheClient
 */
public class ClientProxyMembershipID
    implements DataSerializableFixedID, Serializable, Externalizable {
  public static final long serialVersionUID = 7144300815346556370L;

  private static final Logger logger = LogService.getLogger();

  private static final ThreadLocal<String> POOL_NAME = new ThreadLocal<>();

  public static void setPoolName(String poolName) {
    POOL_NAME.set(poolName);
  }

  public static String getPoolName() {
    return POOL_NAME.get();
  }

  @MakeNotStatic
  public static volatile DistributedSystem system = null;

  /**
   * the membership id of the distributed system in this client (if running in a client)
   */
  @MakeNotStatic
  public static DistributedMember systemMemberId;

  // durable_synch_counter=1 is reserved for durable clients
  // so that when pools are being created and deleted the same client
  // session is selected on the serverside by always using the
  // same uniqueID value which is set via the synch_counter
  private static final int durable_synch_counter = 1;

  @MakeNotStatic
  private static int synch_counter = 0;

  protected byte[] identity;

  /**
   * cached membership identifier
   */
  private transient DistributedMember memberId;

  /**
   * cached tostring of the memberID
   */
  private transient String memberIdString;

  protected int uniqueId;

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    if (isDurable()) {
      result = mult * result + getDurableId().hashCode();
    } else {
      if (identity != null && identity.length > 0) {
        for (final byte b : identity) {
          result = mult * result + b;
        }
      }
    }
    // we can't use unique_id in hashCode
    // because of HandShake's hashCode using our HashCode but
    // its equals using our isSameDSMember which ignores unique_id
    // result = mult * result + this.unique_id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ClientProxyMembershipID)) {
      return false;
    }
    ClientProxyMembershipID that = (ClientProxyMembershipID) obj;
    if (uniqueId != that.uniqueId) {
      return false;
    }
    boolean isDurable = isDurable();
    if (isDurable && !that.isDurable()) {
      return false;
    }
    if (isDurable) {
      return getDurableId().equals(that.getDurableId());
    }
    return Arrays.equals(identity, that.identity);
  }

  /**
   * Return true if "that" can be used in place of "this" when canonicalizing.
   */
  private boolean isCanonicalEquals(ClientProxyMembershipID that) {
    if (this == that) {
      return true;
    }
    if (uniqueId != that.uniqueId) {
      return false;
    }
    return Arrays.equals(identity, that.identity);
  }

  boolean isSameDSMember(ClientProxyMembershipID that) {
    if (that != null) {
      // Test whether:
      // - the durable ids are equal (if durable) or
      // - the identities are equal (if non-durable)
      return isDurable() ? getDurableId().equals(that.getDurableId())
          : Arrays.equals(identity, that.identity);
    } else {
      return false;
    }
  }

  /**
   * method to obtain ClientProxyMembership for client side
   */
  public static synchronized ClientProxyMembershipID getNewProxyMembership(DistributedSystem sys) {
    byte[] ba = initializeAndGetDSIdentity(sys);
    return new ClientProxyMembershipID(++synch_counter, ba);
  }

  public static ClientProxyMembershipID getClientId(DistributedMember member) {
    return new ClientProxyMembershipID(member);
  }

  public static byte[] initializeAndGetDSIdentity(final DistributedSystem sys) {
    final byte[] client_side_identity;
    if (sys == null) {
      // DistributedSystem is required now before handshaking
      throw new IllegalStateException(
          "Attempting to handshake with CacheServer before creating DistributedSystem and Cache.");
    }
    systemMemberId = sys.getDistributedMember();
    try (HeapDataOutputStream hdos = new HeapDataOutputStream(256, KnownVersion.CURRENT)) {
      if (systemMemberId != null) {
        // update the durable id of the member identifier before serializing in case
        // a pool name has been established
        DurableClientAttributes attributes = systemMemberId.getDurableClientAttributes();
        if (attributes != null && attributes.getId().length() > 0) {
          ((InternalDistributedMember) systemMemberId).setDurableId(attributes.getId());
        }
      }
      DataSerializer.writeObject(systemMemberId, hdos);
      client_side_identity = hdos.toByteArray();
    } catch (IOException ioe) {
      throw new InternalGemFireException("Unable to serialize identity", ioe);
    }
    system = sys;
    return client_side_identity;
  }

  private ClientProxyMembershipID(int id, byte[] clientSideIdentity) {
    this(clientSideIdentity, getUniqueId(id), systemMemberId);
  }

  private static int getUniqueId(final int id) {
    final boolean specialCase =
        Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "SPECIAL_DURABLE");
    final String durableID = system.getProperties().getProperty(DURABLE_CLIENT_ID);
    if (specialCase && durableID != null && (!durableID.equals(""))) {
      return durable_synch_counter;
    } else {
      return id;
    }
  }

  public ClientProxyMembershipID() {}

  public ClientProxyMembershipID(final DistributedMember member) {
    this(null, 1, member);
    updateID(member);
  }

  @VisibleForTesting
  ClientProxyMembershipID(final byte[] identity, final int uniqueId,
      final DistributedMember memberId) {
    this.identity = identity;
    this.uniqueId = uniqueId;
    this.memberId = memberId;
  }

  private transient String _toString;

  @Override
  public String toString() {
    if (identity != null
        && ((InternalDistributedMember) getDistributedMember()).getMembershipPort() == 0) {
      return toStringNoCache();
    }
    if (_toString == null) {
      _toString = toStringNoCache();
    }
    return _toString;
  }

  /**
   * returns a string representation of this identifier, ignoring the toString cache
   */
  public String toStringNoCache() {
    StringBuilder sb =
        new StringBuilder("identity(").append(getDSMembership()).append(",connection=")
            .append(uniqueId);
    if (identity != null) {
      DurableClientAttributes dca = getDurableAttributes();
      if (dca.getId().length() > 0) {
        sb.append(",durableAttributes=").append(dca).append(')');
      }
    }
    return sb.toString();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (identity.length > Short.MAX_VALUE) {
      throw new IOException("HandShake identity length is too big");
    }

    out.writeShort(identity.length);
    out.write(identity);
    out.writeInt(uniqueId);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int identityLength = in.readShort();
    if (identityLength < 0) {
      throw new IOException("HandShake identity length is too small");
    }
    identity = new byte[identityLength];
    in.readFully(identity);
    uniqueId = in.readInt();
    if (uniqueId == -1) {
      throw new IOException("Unexpected EOF reached. Unique ID could not be read");
    }
  }

  @Override
  public int getDSFID() {
    return CLIENT_PROXY_MEMBERSHIPID;
  }

  @Override
  public void toData(final DataOutput out, final SerializationContext context) throws IOException {
    DataSerializer.writeByteArray(identity, out);
    out.writeInt(uniqueId);
  }

  @Override
  public void fromData(final DataInput in, final DeserializationContext context)
      throws IOException, ClassNotFoundException {
    identity = DataSerializer.readByteArray(in);
    uniqueId = in.readInt();
  }

  public KnownVersion getClientVersion() {
    return Versioning.getKnownVersionOrDefault(
        ((InternalDistributedMember) getDistributedMember()).getVersion(),
        KnownVersion.CURRENT);
  }

  public String getDSMembership() {
    if (identity == null) {
      // some unit tests create IDs that have no real identity, so return null
      return "null";
    }
    // don't cache if we haven't connected to the server yet
    if (((InternalDistributedMember) getDistributedMember()).getMembershipPort() == 0) {
      return getMemberIdAsString();
    }
    if (memberIdString == null) {
      memberIdString = getMemberIdAsString();
    }
    return memberIdString;
  }

  private String getMemberIdAsString() {
    final String memberIdAsString;
    InternalDistributedMember idm = (InternalDistributedMember) getDistributedMember();
    if (getClientVersion().isOlderThan(KnownVersion.GFE_90)) {
      memberIdAsString = idm.toString();
    } else {
      StringBuilder sb = new StringBuilder();
      idm.addFixedToString(sb, !SocketCreator.resolve_dns);
      memberIdAsString = sb.toString();
    }
    return memberIdAsString;
  }

  /**
   * this method uses CacheClientNotifier to try to obtain an ID that is equal to this one. This is
   * used during deserialization to reduce storage overhead.
   */
  private ClientProxyMembershipID canonicalReference() {
    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
    if (ccn != null) {
      CacheClientProxy cp = ccn.getClientProxy(this, true);
      if (cp != null) {
        if (isCanonicalEquals(cp.getProxyID())) {
          return cp.getProxyID();
        }
      }
    }
    return this;
  }

  /**
   * deserializes the membership id, if necessary, and returns it. All access to membershipId should
   * be through this method
   */

  public DistributedMember getDistributedMember() {
    if (memberId == null) {
      try (ByteArrayDataInput dataInput = new ByteArrayDataInput(identity)) {
        memberId = DataSerializer.readObject(dataInput);
      } catch (Exception e) {
        logger.error("Unable to deserialize membership id", e);
      }
    }
    return memberId;
  }

  /**
   * Returns whether this <code>ClientProxyMembershipID</code> is durable.
   *
   * @return whether this <code>ClientProxyMembershipID</code> is durable
   * @since GemFire 5.5
   */
  public boolean isDurable() {
    String durableClientId = getDistributedMember().getDurableClientAttributes().getId();
    return durableClientId != null && !(durableClientId.length() == 0);
  }

  /**
   * Returns this <code>ClientProxyMembershipID</code>'s durable attributes.
   *
   * @return this <code>ClientProxyMembershipID</code>'s durable attributes
   * @since GemFire 5.5
   */
  protected DurableClientAttributes getDurableAttributes() {
    return getDistributedMember().getDurableClientAttributes();
  }

  /**
   * Returns this <code>ClientProxyMembershipID</code>'s durable id.
   *
   * @return this <code>ClientProxyMembershipID</code>'s durable id
   * @since GemFire 5.5
   */
  public String getDurableId() {
    DurableClientAttributes dca = getDurableAttributes();
    return dca == null ? "" : dca.getId();
  }

  /**
   * Returns this <code>ClientProxyMembershipID</code>'s durable timeout.
   *
   * @return this <code>ClientProxyMembershipID</code>'s durable timeout
   * @since GemFire 5.5
   */
  protected int getDurableTimeout() {
    DurableClientAttributes dca = getDurableAttributes();
    return dca == null ? 0 : dca.getTimeout();
  }

  /**
   * Used to update the timeout when a durable client comes back to a server
   */
  public void updateDurableTimeout(int newValue) {
    InternalDistributedMember member = (InternalDistributedMember) getDistributedMember();
    member.setDurableTimeout(newValue);
  }

  /**
   * call this when the distributed system ID has been modified
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "Only applicable in client DS and in that case too multiple instances do not modify it at the same time.")
  public void updateID(DistributedMember idm) {
    try (HeapDataOutputStream hdos = new HeapDataOutputStream(256, KnownVersion.CURRENT)) {
      try {
        DataSerializer.writeObject(idm, hdos);
      } catch (IOException e) {
        throw new InternalGemFireException("Unable to serialize member: " + memberId, e);
      }
      identity = hdos.toByteArray();
    }
    if (memberId != null && memberId == systemMemberId) {
      systemMemberId = idm;
      // client_side_identity = this.identity;
    }
    memberId = idm;
    _toString = null; // make sure we don't retain the old ID representation in toString
  }


  /**
   * Return the name of the <code>HARegion</code> queueing this proxy's messages. This is name is
   * generated based on whether or not this proxy id is durable. If this proxy id is durable, then
   * the durable client id is used. If this proxy id is not durable, then
   * the<code>DistributedMember</code> string is used.
   *
   * @return the name of the <code>HARegion</code> queueing this proxy's messages.
   * @since GemFire 5.5
   */
  protected String getHARegionName() {
    return getBaseRegionName() + "_queue";
  }

  /**
   * Return the name of the region used for communicating interest changes between servers.
   *
   * @return the name of the region used for communicating interest changes between servers
   * @since GemFire 5.6
   */
  protected String getInterestRegionName() {
    return getBaseRegionName() + "_interest";
  }

  private String getBaseRegionName() {
    return "_gfe_" +
        (isDurable() ? "" : "non_") +
        "durable_client_" +
        "with_id_" +
        (isDurable() ? getDurableId() : getDSMembership()).replace(SEPARATOR_CHAR, ':') +
        "_" +
        uniqueId;
  }

  /**
   * Resets the unique id counter. This is done for durable clients that stops/starts its cache.
   * When it restarts its cache, it needs to maintain the same unique id
   *
   * @since GemFire 5.5
   */
  public static synchronized void resetUniqueIdCounter() {
    synch_counter = 0;
  }

  public Identity getIdentity() {
    return new Identity();
  }

  /**
   * Used to represent a unique identity of this ClientProxyMembershipID. It does this by ignoring
   * the durable id and only respecting the unique_id and identity.
   * <p>
   * This class is used to clean up resources associated with a particular client and thus does not
   * want to limit itself to the durable id.
   *
   * @since GemFire 5.7
   */
  public class Identity {
    public int getUniqueId() {
      return uniqueId;
    }

    public byte[] getMemberIdBytes() {
      return identity;
    }

    @Override
    public int hashCode() {
      int result = 17;
      final int mult = 37;
      byte[] idBytes = getMemberIdBytes();
      if (idBytes != null && idBytes.length > 0) {
        for (final byte idByte : idBytes) {
          result = mult * result + idByte;
        }
      }
      result = mult * result + uniqueId;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Identity)) {
        return false;
      }
      ClientProxyMembershipID.Identity that = (ClientProxyMembershipID.Identity) obj;
      return (getUniqueId() == that.getUniqueId()
          && Arrays.equals(getMemberIdBytes(), that.getMemberIdBytes()));
    }

  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  public static ClientProxyMembershipID readCanonicalized(DataInput in)
      throws IOException, ClassNotFoundException {

    ClientProxyMembershipID result = DataSerializer.readObject(in);
    // We can't canonicalize if we have no identity.
    // I only saw this happen in unit tests that serialize "new ClientProxyMembershipID()".
    if (result == null || result.identity == null) {
      return result;
    }
    return result.canonicalReference();
  }
}
