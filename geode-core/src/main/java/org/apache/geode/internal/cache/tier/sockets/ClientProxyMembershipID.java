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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.logging.LogService;

/**
 * This class represents a ConnectionProxy of the CacheClient
 *
 *
 *
 */
public class ClientProxyMembershipID
    implements DataSerializableFixedID, Serializable, Externalizable {

  private static final Logger logger = LogService.getLogger();

  private static ThreadLocal<String> POOL_NAME = new ThreadLocal<String>();

  public static void setPoolName(String poolName) {
    POOL_NAME.set(poolName);
  }

  public static String getPoolName() {
    return POOL_NAME.get();
  }

  private static final int BYTES_32KB = 32768;

  public static volatile DistributedSystem system = null;

  /**
   * the membership id of the distributed system in this client (if running in a client)
   */
  public static DistributedMember systemMemberId;

  // durable_synch_counter=1 is reserved for durable clients
  // so that when pools are being created and deleted the same client
  // session is selected on the serverside by always using the
  // same uniqueID value which is set via the synch_counter
  private static final int durable_synch_counter = 1;
  private static int synch_counter = 0;

  protected byte[] identity;

  /** cached membership identifier */
  private transient DistributedMember memberId;

  /** cached tostring of the memberID */
  private transient String memberIdString;

  protected int uniqueId;

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    if (isDurable()) {
      result = mult * result + getDurableId().hashCode();
    } else {
      if (this.identity != null && this.identity.length > 0) {
        for (int i = 0; i < this.identity.length; i++) {
          result = mult * result + this.identity[i];
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
    if ((obj == null) || !(obj instanceof ClientProxyMembershipID)) {
      return false;
    }
    ClientProxyMembershipID that = (ClientProxyMembershipID) obj;
    if (this.uniqueId != that.uniqueId) {
      return false;
    }
    boolean isDurable = this.isDurable();
    if (isDurable && !that.isDurable()) {
      return false;
    }
    if (isDurable) {
      return this.getDurableId().equals(that.getDurableId());
    }
    return Arrays.equals(this.identity, that.identity);
  }

  /**
   * Return true if "that" can be used in place of "this" when canonicalizing.
   */
  private boolean isCanonicalEquals(ClientProxyMembershipID that) {
    if (this == that) {
      return true;
    }
    if (this.uniqueId != that.uniqueId) {
      return false;
    }
    return Arrays.equals(this.identity, that.identity);
  }

  boolean isSameDSMember(ClientProxyMembershipID that) {
    if (that != null) {
      // Test whether:
      // - the durable ids are equal (if durable) or
      // - the identities are equal (if non-durable)
      return isDurable() ? this.getDurableId().equals(that.getDurableId())
          : Arrays.equals(this.identity, that.identity);
    } else {
      return false;
    }
  }

  /** method to obtain ClientProxyMembership for client side */
  public static synchronized ClientProxyMembershipID getNewProxyMembership(DistributedSystem sys) {
    byte[] ba = initializeAndGetDSIdentity(sys);
    return new ClientProxyMembershipID(++synch_counter, ba);
  }

  public static ClientProxyMembershipID getClientId(DistributedMember member) {
    return new ClientProxyMembershipID(member);
  }

  public static byte[] initializeAndGetDSIdentity(DistributedSystem sys) {
    byte[] client_side_identity = null;
    if (sys == null) {
      // DistributedSystem is required now before handshaking -Kirk
      throw new IllegalStateException(
          "Attempting to handshake with CacheServer before creating DistributedSystem and Cache.");
    }
    // if (system != sys)
    {
      // DS already exists... make sure it's for current DS connection
      systemMemberId = sys.getDistributedMember();
      try {
        HeapDataOutputStream hdos = new HeapDataOutputStream(256, Version.CURRENT);
        DataSerializer.writeObject(systemMemberId, hdos);
        client_side_identity = hdos.toByteArray();
      } catch (IOException ioe) {
        throw new InternalGemFireException(
            "Unable to serialize identity",
            ioe);
      }

      system = sys;
    }
    return client_side_identity;

  }

  private ClientProxyMembershipID(int id, byte[] clientSideIdentity) {
    Boolean specialCase = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "SPECIAL_DURABLE");
    String durableID = this.system.getProperties().getProperty(DURABLE_CLIENT_ID);
    if (specialCase.booleanValue() && durableID != null && (!durableID.equals(""))) {
      this.uniqueId = durable_synch_counter;
    } else {
      this.uniqueId = id;
    }
    this.identity = clientSideIdentity;
    this.memberId = systemMemberId;
  }

  public ClientProxyMembershipID() {}

  public ClientProxyMembershipID(DistributedMember member) {
    this.uniqueId = 1;
    this.memberId = member;
    updateID(member);
  }


  private transient String _toString;

  // private transient int transientPort; // variable for debugging member ID issues

  @Override
  public String toString() {
    if (this.identity != null
        && ((InternalDistributedMember) getDistributedMember()).getPort() == 0) {
      return this.toStringNoCache();
    }
    if (this._toString == null) {
      this._toString = this.toStringNoCache();
    }
    return this._toString;
  }

  /**
   * returns a string representation of this identifier, ignoring the toString cache
   */
  public String toStringNoCache() {
    StringBuffer sb = new StringBuffer("identity(").append(getDSMembership()).append(",connection=")
        .append(uniqueId);
    if (identity != null) {
      DurableClientAttributes dca = getDurableAttributes();
      if (dca.getId().length() > 0) {
        sb.append(",durableAttributes=").append(getDurableAttributes()).append(')').toString();
      }
    }
    return sb.toString();
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    // if (this.transientPort == 0) {
    // InternalDistributedSystem.getLogger().warning(
    // String.format("%s",
    // "externalizing a client ID with zero port: " + this.toString(),
    // new Exception("Stack trace")));
    // }
    Assert.assertTrue(this.identity.length <= BYTES_32KB);
    out.writeShort(this.identity.length);
    out.write(this.identity);
    out.writeInt(this.uniqueId);

  }

  /** returns the externalized size of this object */
  public int getSerializedSize() {
    return 4 + identity.length + 4;
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int identityLength = in.readShort();
    if (identityLength > BYTES_32KB) {
      throw new IOException(
          "HandShake identity length is too big");
    }
    this.identity = new byte[identityLength];
    read(in, this.identity);
    this.uniqueId = in.readInt();
    if (this.uniqueId == -1) {
      throw new IOException(
          "Unexpected EOF reached. Unique ID could not be read");
    }
    // {toString(); this.transientPort = ((InternalDistributedMember)this.memberId).getPort();}
  }

  private void read(ObjectInput dis, byte[] toFill) throws IOException {

    int idBytes = 0;
    int toFillLength = toFill.length;
    while (idBytes < toFillLength) {
      // idBytes += dis.read(toFill, idBytes, (toFillLength - idBytes));
      int dataRead = dis.read(toFill, idBytes, (toFillLength - idBytes));
      if (dataRead == -1) {
        throw new IOException(
            "Unexpected EOF reached. Distributed MembershipID could not be read");
      }
      idBytes += dataRead;
    }
  }

  public int getDSFID() {
    return CLIENT_PROXY_MEMBERSHIPID;
  }

  public void toData(DataOutput out) throws IOException {
    // if (this.transientPort == 0) {
    // InternalDistributedSystem.getLogger().warning(
    // String.format("%s",
    // "serializing a client ID with zero port: " + this.toString(),
    // new Exception("Stack trace")));
    // }
    DataSerializer.writeByteArray(this.identity, out);
    out.writeInt(this.uniqueId);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.identity = DataSerializer.readByteArray(in);
    this.uniqueId = in.readInt();
    // {toString(); this.transientPort = ((InternalDistributedMember)this.memberId).getPort();}
  }

  public Version getClientVersion() {
    return ((InternalDistributedMember) getDistributedMember()).getVersionObject();
  }

  public String getDSMembership() {
    if (identity == null) {
      // some unit tests create IDs that have no real identity, so return null
      return "null";
    }
    // don't cache if we haven't connected to the server yet
    if (((InternalDistributedMember) getDistributedMember()).getPort() == 0) {
      return getMemberIdAsString();
    }
    if (memberIdString == null) {
      memberIdString = getMemberIdAsString();
    }
    return memberIdString;
  }

  private String getMemberIdAsString() {
    String memberIdAsString = null;
    InternalDistributedMember idm = (InternalDistributedMember) getDistributedMember();
    if (getClientVersion().compareTo(Version.GFE_90) < 0) {
      memberIdAsString = idm.toString();
    } else {
      StringBuilder sb = new StringBuilder();
      idm.addFixedToString(sb);
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
        if (this.isCanonicalEquals(cp.getProxyID())) {
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
      ByteArrayInputStream bais = new ByteArrayInputStream(identity);
      DataInputStream dis = new VersionedDataInputStream(bais, Version.CURRENT);
      try {
        memberId = (DistributedMember) DataSerializer.readObject(dis);
      } catch (Exception e) {
        logger.error("Unable to deserialize membership id", e);
      }
    }
    return memberId;
  }

  /** Returns the byte-array for membership identity */
  byte[] getMembershipByteArray() {
    return this.identity;
  }

  /**
   * Returns whether this <code>ClientProxyMembershipID</code> is durable.
   *
   * @return whether this <code>ClientProxyMembershipID</code> is durable
   *
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
   *
   * @since GemFire 5.5
   */
  protected DurableClientAttributes getDurableAttributes() {
    return getDistributedMember().getDurableClientAttributes();
  }

  /**
   * Returns this <code>ClientProxyMembershipID</code>'s durable id.
   *
   * @return this <code>ClientProxyMembershipID</code>'s durable id
   *
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
   *
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
    DurableClientAttributes dca = getDurableAttributes();
    if (dca != null) {
      dca.updateTimeout(newValue);
    }
  }

  /**
   * call this when the distributed system ID has been modified
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "Only applicable in client DS and in that case too multiple instances do not modify it at the same time.")
  public void updateID(DistributedMember idm) {
    // this.transientPort = ((InternalDistributedMember)this.memberId).getPort();
    // if (this.transientPort == 0) {
    // InternalDistributedSystem.getLogger().warning(
    // String.format("%s",
    // "updating client ID when member port is zero: " + this.memberId,
    // new Exception("stack trace")
    // ));
    // }
    HeapDataOutputStream hdos = new HeapDataOutputStream(256, Version.CURRENT);
    try {
      DataSerializer.writeObject(idm, hdos);
    } catch (IOException e) {
      throw new InternalGemFireException("Unable to serialize member: " + this.memberId, e);
    }
    this.identity = hdos.toByteArray();
    if (this.memberId != null && this.memberId == systemMemberId) {
      systemMemberId = idm;
      // client_side_identity = this.identity;
    }
    this.memberId = idm;
    this._toString = null; // make sure we don't retain the old ID representation in toString
  }


  /**
   * Return the name of the <code>HARegion</code> queueing this proxy's messages. This is name is
   * generated based on whether or not this proxy id is durable. If this proxy id is durable, then
   * the durable client id is used. If this proxy id is not durable, then
   * the<code>DistributedMember</code> string is used.
   *
   * @return the name of the <code>HARegion</code> queueing this proxy's messages.
   *
   * @since GemFire 5.5
   */
  protected String getHARegionName() {
    return getBaseRegionName() + "_queue";
  }

  /**
   * Return the name of the region used for communicating interest changes between servers.
   *
   * @return the name of the region used for communicating interest changes between servers
   *
   * @since GemFire 5.6
   */
  protected String getInterestRegionName() {
    return getBaseRegionName() + "_interest";
  }

  private String getBaseRegionName() {
    String id = isDurable() ? getDurableId() : getDSMembership();
    if (id.indexOf('/') >= 0) {
      id = id.replace('/', ':');
    }
    StringBuffer buffer = new StringBuffer().append("_gfe_").append(isDurable() ? "" : "non_")
        .append("durable_client_").append("with_id_" + id).append("_").append(this.uniqueId);
    return buffer.toString();
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
        for (int i = 0; i < idBytes.length; i++) {
          result = mult * result + idBytes[i];
        }
      }
      result = mult * result + uniqueId;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if ((obj == null) || !(obj instanceof ClientProxyMembershipID.Identity)) {
        return false;
      }
      ClientProxyMembershipID.Identity that = (ClientProxyMembershipID.Identity) obj;
      return (getUniqueId() == that.getUniqueId()
          && Arrays.equals(getMemberIdBytes(), that.getMemberIdBytes()));
    }

    public ClientProxyMembershipID getClientProxyID() {
      return ClientProxyMembershipID.this;
    }

  }

  @Override
  public Version[] getSerializationVersions() {
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
