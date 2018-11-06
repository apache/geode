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
package org.apache.geode.internal.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.ObjToByteArraySerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * This class is used to hold the information about the servers and their Filters (CQs and Interest
 * List) that are satisfied by the cache update operation.
 *
 * @since GemFire 6.5
 */
public class FilterRoutingInfo implements VersionedDataSerializable {

  private static boolean OLD_MEMBERS_OPTIMIZED = Boolean.getBoolean("optimized-cq-serialization");

  private static Version[] serializationVersions = new Version[] {Version.GFE_71};

  /** Set to true if any peer members has any filters. */
  private boolean memberWithFilterInfoExists = false;

  /** Holds filter local filter information. */
  private transient FilterInfo localFilterInfo;

  /** whether local interest has been computed (not CQs - just interest) */
  private transient boolean hasLocalInterestBeenComputed;

  /**
   * Map containing peer-server to filter message.
   */
  private HashMap<InternalDistributedMember, FilterInfo> serverFilterInfo =
      new HashMap<InternalDistributedMember, FilterInfo>();

  /**
   * Sets the local CQ filter information.
   *
   * @param cqInfo map of server side CQ Name to CQ event type.
   */
  public void setLocalCqInfo(HashMap cqInfo) {
    if (this.localFilterInfo == null) {
      this.localFilterInfo = new FilterInfo();
    }
    this.localFilterInfo.cqs = cqInfo;
    this.localFilterInfo.filterProcessedLocally = true;
  }

  /**
   * Sets the local Interest information.
   *
   * @param clients interested clients with receiveValues=true.
   * @param clientsInv interested clients with receiveValues=false;
   */
  public void setLocalInterestedClients(Set clients, Set clientsInv) {
    if (this.localFilterInfo == null) {
      this.localFilterInfo = new FilterInfo();
    }
    this.localFilterInfo.setInterestedClients(clients);
    this.localFilterInfo.setInterestedClientsInv(clientsInv);
    this.localFilterInfo.filterProcessedLocally = true;
    this.hasLocalInterestBeenComputed = true;
  }

  /**
   * returns true if local interest has been computed
   */
  public boolean hasLocalInterestBeenComputed() {
    return this.hasLocalInterestBeenComputed;
  }

  /**
   * Returns local Filter information.
   *
   * @return FilterInfo local filter info having CQs and interested client info.
   */
  public FilterInfo getLocalFilterInfo() {
    return this.localFilterInfo;
  }

  /**
   * Sets CQ routing information.
   *
   * @param member for which CQs are satisfied.
   * @param cqInfo map of server side CQ Name to CQ event type.
   */
  public void setCqRoutingInfo(InternalDistributedMember member, HashMap cqInfo) {
    FilterInfo fInfo = new FilterInfo();
    fInfo.setCQs(cqInfo);
    this.serverFilterInfo.put(member, fInfo);
    if (cqInfo.size() > 0) {
      this.memberWithFilterInfoExists = true;
    }
  }

  /**
   * Sets interested clients routing information
   *
   * @param member on which the client interests are satisfied
   * @param clients Set containing interested clients with receiveValues=true
   * @param clientsInv Set containing interested clients with receiveValues=false
   * @param longIDs whether the client IDs may be long integers
   */
  public void addInterestedClients(InternalDistributedMember member, Set clients, Set clientsInv,
      boolean longIDs) {
    this.memberWithFilterInfoExists = true;
    FilterInfo fInfo = this.serverFilterInfo.get(member);
    if (fInfo == null) {
      fInfo = new FilterInfo();
      this.serverFilterInfo.put(member, fInfo);
    }
    if (clients != null && clients.size() > 0) {
      fInfo.setInterestedClients(clients);
    }
    if (clientsInv != null && clientsInv.size() > 0) {
      fInfo.setInterestedClientsInv(clientsInv);
    }
    if (longIDs) {
      fInfo.longIDs = longIDs;
    }
  }

  /**
   * Returns the members list that has filters satisfied.
   *
   * @return the members who have filter routing information
   */
  public Set<InternalDistributedMember> getMembers() {
    return this.serverFilterInfo.keySet();
  }

  /**
   * Returns true if the local filter information is set.
   *
   * @return whether there is local routing information in this object
   */
  public boolean hasLocalFilterInfo() {
    return this.localFilterInfo != null;
  }

  /**
   * Returns true if there is any member with filters satisfied.
   *
   * @return true if we have any filter information in this object
   */
  public boolean hasMemberWithFilterInfo() {
    return this.memberWithFilterInfoExists;
  }

  /**
   * Returns the Filter Information for the member.
   *
   * @param member the member whose filter information is desired
   * @return the filter information for the given member
   */
  public FilterInfo getFilterInfo(InternalDistributedMember member) {
    return this.serverFilterInfo.get(member);
  }

  /**
   * This adds the filter information from the given routing object to this object's tables. This is
   * used to merge routing information for putAll operations.
   *
   * @param eventRouting the routing information for a single putAll event
   */
  public void addFilterInfo(FilterRoutingInfo eventRouting) {
    for (Map.Entry<InternalDistributedMember, FilterInfo> entry : eventRouting.serverFilterInfo
        .entrySet()) {
      FilterInfo existing = this.serverFilterInfo.get(entry.getKey());
      if (existing == null) {
        existing = new FilterInfo();
        this.serverFilterInfo.put(entry.getKey(), existing);
      }
      existing.addFilterInfo(entry.getValue());
    }
    if (eventRouting.localFilterInfo != null) {
      if (this.localFilterInfo == null) {
        this.localFilterInfo = new FilterInfo();
      }
      this.localFilterInfo.addFilterInfo(eventRouting.localFilterInfo);
    }
    this.memberWithFilterInfoExists |= eventRouting.memberWithFilterInfoExists;
  }

  /** DataSerializable methods */
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    DistributedMember myID = null;
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      myID = cache.getMyId();
    }
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      InternalDistributedMember member = InternalDistributedMember.readEssentialData(in);
      FilterInfo fInfo = new FilterInfo();
      InternalDataSerializer.invokeFromData(fInfo, in);
      // we only need to retain the recipient's entry
      if (myID == null || myID.equals(member)) {
        this.serverFilterInfo.put(member, fInfo);
      }
    }
  }

  public void toData(DataOutput out) throws IOException {
    int size = this.serverFilterInfo.size();
    out.writeInt(size);
    for (Map.Entry<InternalDistributedMember, FilterInfo> e : this.serverFilterInfo.entrySet()) {
      InternalDistributedMember member = e.getKey();
      member.writeEssentialData(out);
      FilterInfo fInfo = e.getValue();
      InternalDataSerializer.invokeToData(fInfo, out);
    }
  }

  public Version[] getSerializationVersions() {
    return serializationVersions;
  }

  public void fromDataPre_GFE_7_1_0_0(DataInput in) throws IOException, ClassNotFoundException {
    DistributedMember myID = null;
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      myID = cache.getMyId();
    }
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      InternalDistributedMember member = new InternalDistributedMember();
      InternalDataSerializer.invokeFromData(member, in);
      FilterInfo fInfo = new FilterInfo();
      InternalDataSerializer.invokeFromData(fInfo, in);
      // we only need to retain the recipient's entry
      if (myID == null || myID.equals(member)) {
        this.serverFilterInfo.put(member, fInfo);
      }
    }
  }

  public void toDataPre_GFE_7_1_0_0(DataOutput out) throws IOException {
    int size = this.serverFilterInfo.size();
    out.writeInt(size);
    for (Map.Entry<InternalDistributedMember, FilterInfo> e : this.serverFilterInfo.entrySet()) {
      InternalDistributedMember member = e.getKey();
      InternalDataSerializer.invokeToData(member, out);
      FilterInfo fInfo = e.getValue();
      InternalDataSerializer.invokeToData(fInfo, out);
    }
  }

  @Override
  public String toString() {
    String result = "FilterRoutingInfo(";
    if (this.localFilterInfo != null) {
      result += "local=";
      result += this.localFilterInfo;
      if (this.serverFilterInfo != null) {
        result += ", ";
      }
    }
    if (this.serverFilterInfo != null) {
      result += "remote=";
      result += this.serverFilterInfo;
    }
    return result + ")";
  }


  /**
   * This holds the information about the CQs and interest list.
   */
  public static class FilterInfo implements VersionedDataSerializable {

    public boolean longIDs;

    private static final long serialVersionUID = 0;

    /** Map holding Cq filterID and CqEvent Type */
    private HashMap<Long, Integer> cqs;

    /**
     * serialized routing data. This is only deserialized when requested so that routing information
     * for other members included in a cach op message stays serialized, reducing the cost of having
     * to send all routing info to all members
     */
    private transient byte[] myData;

    /** version of creator of myData, needed for deserialization */
    private transient Version myDataVersion;

    /** Clients that are interested in the event and want values */
    private Set interestedClients;

    /** Clients that are interested in the event and want invalidations */
    private Set interestedClientsInv;

    /** To identify where the filter is processed, locally or in remote node */
    public boolean filterProcessedLocally = false;

    /** adds the content from another FilterInfo object. */
    public void addFilterInfo(FilterInfo other) {
      if (other.cqs != null) {
        if (this.cqs == null) {
          this.cqs = new HashMap();
        }
        for (Map.Entry<Long, Integer> entry : other.cqs.entrySet()) {
          this.cqs.put(entry.getKey(), entry.getValue());
        }
      }
      if (other.interestedClients != null) {
        if (this.interestedClients == null) {
          this.interestedClients = new HashSet();
        }
        this.interestedClients.addAll(other.interestedClients);
      }
      if (other.interestedClientsInv != null) {
        if (this.interestedClientsInv == null) {
          this.interestedClientsInv = new HashSet();
        }
        this.interestedClientsInv.addAll(other.interestedClientsInv);
      }
    }

    /** clears CQ routing information */
    public void clearCQRouting() {
      this.cqs = null;
    }

    private static Version[] serializationVersions = new Version[] {Version.GFE_80};

    public Version[] getSerializationVersions() {
      return serializationVersions;
    }

    /** DataSerializable methods */
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.myData = DataSerializer.readByteArray(in);
    }

    public void toData(DataOutput out) throws IOException {
      HeapDataOutputStream hdos;
      int size = 9;
      size += interestedClients == null ? 4 : interestedClients.size() * 8 + 5;
      size += interestedClientsInv == null ? 4 : interestedClientsInv.size() * 8 + 5;
      size += cqs == null ? 0 : cqs.size() * 12;
      hdos = new HeapDataOutputStream(size, null);
      if (this.cqs == null) {
        hdos.writeBoolean(false);
      } else {
        hdos.writeBoolean(true);
        InternalDataSerializer.writeArrayLength(cqs.size(), hdos);
        for (Iterator it = this.cqs.entrySet().iterator(); it.hasNext();) {
          Map.Entry e = (Map.Entry) it.next();
          // most cq IDs and all event types are small ints, so we use an optimized
          // write that serializes 7 bits at a time in a compact form
          InternalDataSerializer.writeUnsignedVL((Long) e.getKey(), hdos);
          InternalDataSerializer.writeUnsignedVL((Integer) e.getValue(), hdos);
        }
      }
      InternalDataSerializer.writeSetOfLongs(this.interestedClients, this.longIDs, hdos);
      InternalDataSerializer.writeSetOfLongs(this.interestedClientsInv, this.longIDs, hdos);
      if (out instanceof HeapDataOutputStream) {
        ((ObjToByteArraySerializer) out).writeAsSerializedByteArray(hdos);
      } else {
        byte[] myData = hdos.toByteArray();
        DataSerializer.writeByteArray(myData, out);
      }
    }

    public void fromDataPre_GFE_8_0_0_0(DataInput in) throws IOException, ClassNotFoundException {
      if (OLD_MEMBERS_OPTIMIZED) {
        this.myDataVersion = InternalDataSerializer.getVersionForDataStreamOrNull(in);
        this.myData = DataSerializer.readByteArray(in);
      } else {
        this.cqs = DataSerializer.readHashMap(in);
        this.interestedClients = InternalDataSerializer.readSetOfLongs(in);
        this.interestedClientsInv = InternalDataSerializer.readSetOfLongs(in);
      }
    }

    public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
      if (OLD_MEMBERS_OPTIMIZED) {
        HeapDataOutputStream hdos =
            new HeapDataOutputStream(1000, InternalDataSerializer.getVersionForDataStream(out));
        if (this.cqs == null) {
          hdos.writeBoolean(false);
        } else {
          hdos.writeBoolean(true);
          InternalDataSerializer.writeArrayLength(cqs.size(), hdos);
          for (Iterator it = this.cqs.entrySet().iterator(); it.hasNext();) {
            Map.Entry e = (Map.Entry) it.next();
            // most cq IDs and all event types are small ints, so we use an optimized
            // write that serializes 7 bits at a time in a compact form
            InternalDataSerializer.writeUnsignedVL((Long) e.getKey(), hdos);
            InternalDataSerializer.writeUnsignedVL((Integer) e.getValue(), hdos);
          }
        }
        InternalDataSerializer.writeSetOfLongs(this.interestedClients, this.longIDs, hdos);
        InternalDataSerializer.writeSetOfLongs(this.interestedClientsInv, this.longIDs, hdos);
        if (out instanceof HeapDataOutputStream) {
          ((ObjToByteArraySerializer) out).writeAsSerializedByteArray(hdos);
        } else {
          byte[] myData = hdos.toByteArray();
          DataSerializer.writeByteArray(myData, out);
        }
      } else {
        DataSerializer.writeHashMap(this.cqs, out);
        InternalDataSerializer.writeSetOfLongs(this.interestedClients, this.longIDs, out);
        InternalDataSerializer.writeSetOfLongs(this.interestedClientsInv, this.longIDs, out);
      }
    }

    public HashMap<Long, Integer> getCQs() {
      if (this.cqs == null && this.myData != null) {
        deserialize();
      }
      return this.cqs;
    }

    public void setCQs(HashMap<Long, Integer> cqs) {
      this.cqs = cqs;
    }

    public Set getInterestedClients() {
      if (this.interestedClients == null && this.myData != null) {
        deserialize();
      }
      return this.interestedClients;
    }

    public void setInterestedClients(Set clients) {
      this.interestedClients = clients;
    }

    public Set getInterestedClientsInv() {
      if (this.interestedClientsInv == null && this.myData != null) {
        deserialize();
      }
      return this.interestedClientsInv;
    }

    public void setInterestedClientsInv(Set clients) {
      this.interestedClientsInv = clients;
    }

    /**
     * FilterInfo fields are only deserialized if they are needed. We send all FilterInfo routings
     * to all members that receive a cach op message but each member is only interested in its own
     * routing, so there is no need to deserialize the routings for other members
     */
    private void deserialize() {
      try {
        InputStream is = new ByteArrayInputStream(myData);
        DataInputStream dis;
        if (this.myDataVersion != null) {
          dis = new VersionedDataInputStream(is, this.myDataVersion);
        } else {
          dis = new DataInputStream(is);
        }
        boolean hasCQs = dis.readBoolean();
        if (hasCQs) {
          int numEntries = InternalDataSerializer.readArrayLength(dis);
          this.cqs = new HashMap(numEntries);
          for (int i = 0; i < numEntries; i++) {
            Long key = InternalDataSerializer.readUnsignedVL(dis);
            Integer value = (int) InternalDataSerializer.readUnsignedVL(dis);
            this.cqs.put(key, value);
          }
        }
        this.interestedClients = InternalDataSerializer.readSetOfLongs(dis);
        this.interestedClientsInv = InternalDataSerializer.readSetOfLongs(dis);
        this.myData = null; // prevent future deserializations by setting this to null
      } catch (IOException e) {
        throw new InternalGemFireError(e);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (this.interestedClients != null && this.interestedClients.size() > 0) {
        sb.append("interestedClients:");
        sb.append(this.interestedClients);
      }
      if (this.interestedClientsInv != null && this.interestedClientsInv.size() > 0) {
        sb.append(", interestedClientsInv:");
        sb.append(this.interestedClientsInv);
      }
      if (InternalDistributedSystem.getLogger().finerEnabled()) {
        if (this.cqs != null) {
          sb.append(", cqs=");
          sb.append(this.cqs.keySet());
        }
      } else {
        if (this.cqs != null) {
          sb.append(", ").append(this.cqs.size()).append(" cqs");
        }
      }
      return sb.toString();
    }
  }

}
