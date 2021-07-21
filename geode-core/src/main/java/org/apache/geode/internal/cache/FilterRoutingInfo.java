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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.VersionedDataSerializable;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * This class is used to hold the information about the servers and their Filters (CQs and Interest
 * List) that are satisfied by the cache update operation.
 *
 * @since GemFire 6.5
 */
public class FilterRoutingInfo implements VersionedDataSerializable {

  @Immutable
  private static final KnownVersion[] serializationVersions = new KnownVersion[0];

  /** Set to true if any peer members has any filters. */
  private boolean memberWithFilterInfoExists = false;

  /** Holds filter local filter information. */
  private transient FilterInfo localFilterInfo;

  /**
   * Map containing peer-server to filter message.
   */
  private final HashMap<InternalDistributedMember, FilterInfo> serverFilterInfo = new HashMap<>();

  /**
   * Sets the local CQ filter information.
   *
   * @param cqInfo map of server side CQ Name to CQ event type.
   */
  public void setLocalCqInfo(HashMap<Long, Integer> cqInfo) {
    if (localFilterInfo == null) {
      localFilterInfo = new FilterInfo();
    }
    localFilterInfo.cqs = cqInfo;
    localFilterInfo.filterProcessedLocally = true;
  }

  /**
   * Sets the local Interest information.
   *
   * @param clients interested clients with receiveValues=true.
   * @param clientsInv interested clients with receiveValues=false;
   */
  public void setLocalInterestedClients(Set<Long> clients, Set<Long> clientsInv) {
    if (localFilterInfo == null) {
      localFilterInfo = new FilterInfo();
    }
    localFilterInfo.setInterestedClients(clients);
    localFilterInfo.setInterestedClientsInv(clientsInv);
    localFilterInfo.filterProcessedLocally = true;
  }

  /**
   * Returns local Filter information.
   *
   * @return FilterInfo local filter info having CQs and interested client info.
   */
  public FilterInfo getLocalFilterInfo() {
    return localFilterInfo;
  }

  /**
   * Sets CQ routing information.
   *
   * @param member for which CQs are satisfied.
   * @param cqInfo map of server side CQ Name to CQ event type.
   */
  public void setCqRoutingInfo(InternalDistributedMember member, HashMap<Long, Integer> cqInfo) {
    FilterInfo fInfo = new FilterInfo();
    fInfo.setCQs(cqInfo);
    serverFilterInfo.put(member, fInfo);
    if (cqInfo.size() > 0) {
      memberWithFilterInfoExists = true;
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
  public void addInterestedClients(InternalDistributedMember member, Set<Long> clients,
      Set<Long> clientsInv,
      boolean longIDs) {
    memberWithFilterInfoExists = true;
    FilterInfo fInfo = serverFilterInfo.get(member);
    if (fInfo == null) {
      fInfo = new FilterInfo();
      serverFilterInfo.put(member, fInfo);
    }
    if (clients != null && clients.size() > 0) {
      fInfo.setInterestedClients(clients);
    }
    if (clientsInv != null && clientsInv.size() > 0) {
      fInfo.setInterestedClientsInv(clientsInv);
    }
    if (longIDs) {
      fInfo.longIDs = true;
    }
  }

  /**
   * Returns the members list that has filters satisfied.
   *
   * @return the members who have filter routing information
   */
  public Set<InternalDistributedMember> getMembers() {
    return serverFilterInfo.keySet();
  }

  /**
   * Returns true if there is any member with filters satisfied.
   *
   * @return true if we have any filter information in this object
   */
  public boolean hasMemberWithFilterInfo() {
    return memberWithFilterInfoExists;
  }

  /**
   * Returns the Filter Information for the member.
   *
   * @param member the member whose filter information is desired
   * @return the filter information for the given member
   */
  public FilterInfo getFilterInfo(InternalDistributedMember member) {
    return serverFilterInfo.get(member);
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
      FilterInfo existing = serverFilterInfo.get(entry.getKey());
      if (existing == null) {
        existing = new FilterInfo();
        serverFilterInfo.put(entry.getKey(), existing);
      }
      existing.addFilterInfo(entry.getValue());
    }
    if (eventRouting.localFilterInfo != null) {
      if (localFilterInfo == null) {
        localFilterInfo = new FilterInfo();
      }
      localFilterInfo.addFilterInfo(eventRouting.localFilterInfo);
    }
    memberWithFilterInfoExists |= eventRouting.memberWithFilterInfoExists;
  }

  /** DataSerializable methods */
  @Override
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
        serverFilterInfo.put(member, fInfo);
      }
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    int size = serverFilterInfo.size();
    out.writeInt(size);
    for (Map.Entry<InternalDistributedMember, FilterInfo> e : serverFilterInfo.entrySet()) {
      InternalDistributedMember member = e.getKey();
      member.writeEssentialData(out);
      FilterInfo fInfo = e.getValue();
      InternalDataSerializer.invokeToData(fInfo, out);
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return serializationVersions;
  }

  @Override
  public String toString() {
    String result = "FilterRoutingInfo(";
    if (localFilterInfo != null) {
      result += "local=";
      result += localFilterInfo;
      result += ", ";
    }
    result += "remote=";
    result += serverFilterInfo;
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
     * for other members included in a cache op message stays serialized, reducing the cost of
     * having to send all routing info to all members
     */
    private transient byte[] myData;

    /** Clients that are interested in the event and want values */
    private Set<Long> interestedClients;

    /** Clients that are interested in the event and want invalidations */
    private Set<Long> interestedClientsInv;

    /** To identify where the filter is processed, locally or in remote node */
    public boolean filterProcessedLocally = false;

    /** adds the content from another FilterInfo object. */
    public void addFilterInfo(FilterInfo other) {
      if (other.cqs != null) {
        if (cqs == null) {
          cqs = new HashMap<>();
        }
        for (Map.Entry<Long, Integer> entry : other.cqs.entrySet()) {
          cqs.put(entry.getKey(), entry.getValue());
        }
      }
      if (other.interestedClients != null) {
        if (interestedClients == null) {
          interestedClients = new HashSet<>();
        }
        interestedClients.addAll(other.interestedClients);
      }
      if (other.interestedClientsInv != null) {
        if (interestedClientsInv == null) {
          interestedClientsInv = new HashSet<>();
        }
        interestedClientsInv.addAll(other.interestedClientsInv);
      }
    }

    /** clears CQ routing information */
    public void clearCQRouting() {
      cqs = null;
    }

    @Immutable
    private static final KnownVersion[] serializationVersions = new KnownVersion[0];

    @Override
    public KnownVersion[] getSerializationVersions() {
      return serializationVersions;
    }

    /** DataSerializable methods */
    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      myData = DataSerializer.readByteArray(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      HeapDataOutputStream hdos;
      int size = 9;
      size += interestedClients == null ? 4 : interestedClients.size() * 8 + 5;
      size += interestedClientsInv == null ? 4 : interestedClientsInv.size() * 8 + 5;
      size += cqs == null ? 0 : cqs.size() * 12;
      byte[] myData = StaticSerialization.getThreadLocalByteArray(size);
      hdos = new HeapDataOutputStream(myData);
      hdos.disallowExpansion();
      if (cqs == null) {
        hdos.writeBoolean(false);
      } else {
        hdos.writeBoolean(true);
        InternalDataSerializer.writeArrayLength(cqs.size(), hdos);
        for (final Map.Entry<Long, Integer> longIntegerEntry : cqs.entrySet()) {
          // most cq IDs and all event types are small ints, so we use an optimized
          // write that serializes 7 bits at a time in a compact form
          InternalDataSerializer.writeUnsignedVL(longIntegerEntry.getKey(), hdos);
          InternalDataSerializer.writeUnsignedVL(longIntegerEntry.getValue(), hdos);
        }
      }
      InternalDataSerializer.writeSetOfLongs(interestedClients, longIDs, hdos);
      InternalDataSerializer.writeSetOfLongs(interestedClientsInv, longIDs, hdos);
      hdos.finishWriting();
      DataSerializer.writeByteArray(myData, hdos.size(), out);
    }

    public HashMap<Long, Integer> getCQs() {
      if (cqs == null && myData != null) {
        deserialize();
      }
      return cqs;
    }

    public void setCQs(HashMap<Long, Integer> cqs) {
      this.cqs = cqs;
    }

    public Set<Long> getInterestedClients() {
      if (interestedClients == null && myData != null) {
        deserialize();
      }
      return interestedClients;
    }

    public void setInterestedClients(Set<Long> clients) {
      interestedClients = clients;
    }

    public Set<Long> getInterestedClientsInv() {
      if (interestedClientsInv == null && myData != null) {
        deserialize();
      }
      return interestedClientsInv;
    }

    public void setInterestedClientsInv(Set<Long> clients) {
      interestedClientsInv = clients;
    }

    /**
     * FilterInfo fields are only deserialized if they are needed. We send all FilterInfo routings
     * to all members that receive a cache op message but each member is only interested in its own
     * routing, so there is no need to deserialize the routings for other members
     */
    private void deserialize() {
      try (ByteArrayDataInput dis = new ByteArrayDataInput(myData)) {
        boolean hasCQs = dis.readBoolean();
        if (hasCQs) {
          int numEntries = InternalDataSerializer.readArrayLength(dis);
          cqs = new HashMap<>(numEntries);
          for (int i = 0; i < numEntries; i++) {
            Long key = InternalDataSerializer.readUnsignedVL(dis);
            Integer value = (int) InternalDataSerializer.readUnsignedVL(dis);
            cqs.put(key, value);
          }
        }
        interestedClients = InternalDataSerializer.readSetOfLongs(dis);
        interestedClientsInv = InternalDataSerializer.readSetOfLongs(dis);
        myData = null; // prevent future deserializations by setting this to null
      } catch (IOException e) {
        throw new InternalGemFireError(e);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (interestedClients != null && interestedClients.size() > 0) {
        sb.append("interestedClients:");
        sb.append(interestedClients);
      }
      if (interestedClientsInv != null && interestedClientsInv.size() > 0) {
        sb.append(", interestedClientsInv:");
        sb.append(interestedClientsInv);
      }
      if (InternalDistributedSystem.getLogger().finerEnabled()) {
        if (cqs != null) {
          sb.append(", cqs=");
          sb.append(cqs.keySet());
        }
      } else {
        if (cqs != null) {
          sb.append(", ").append(cqs.size()).append(" cqs");
        }
      }
      return sb.toString();
    }
  }

}
