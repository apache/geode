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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ExternalizableDSFID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.util.Versionable;
import org.apache.geode.internal.util.VersionedArrayList;

/**
 * Maintains configuration information for a PartitionedRegion. Instances are stored in the
 * allPartitionedRegion.
 */
public class PartitionRegionConfig extends ExternalizableDSFID implements Versionable {

  private int prId;

  private PartitionAttributesImpl pAttrs;

  private Scope scope = null;

  /** Nodes participating in this PartitionedRegion */
  private VersionedArrayList nodes = null;

  /**
   * Flag to indicate whether the PartitionedRegion's destruction responsibility is taken up by
   * someone
   */
  private boolean isDestroying = false;

  /**
   * Flag to indicate whether this region has been created on all of the members that host the
   * region that this region is colocated with. Once a region is in this state, new members will
   * have to create this region before they can host any data for the colocated regions
   */
  private boolean isColocationComplete;

  private volatile boolean firstDataStoreCreated = false;

  /**
   * The full path of the region. Used for resolving colocation chains.
   */
  private String fullPath = null;

  private String partitionResolver = null;

  private String colocatedWith = null;

  private EvictionAttributes ea = new EvictionAttributesImpl();

  private ExpirationAttributes regionTimeToLive = null;

  private ExpirationAttributes regionIdleTimeout = null;

  private ExpirationAttributes entryTimeToLive = null;

  private ExpirationAttributes entryIdleTimeout = null;

  private Set<FixedPartitionAttributesImpl> elderFPAs = null;

  private ArrayList<String> partitionListenerClassNames = new ArrayList<>();

  public void setGatewaySenderIds(Set<String> gatewaySenderIds) {
    this.gatewaySenderIds = Collections.unmodifiableSet(gatewaySenderIds);
  }

  private Set<String> gatewaySenderIds = Collections.emptySet();

  /**
   * Default constructor for DataSerializer
   */
  public PartitionRegionConfig() {
    // nothing
  }

  PartitionRegionConfig(int prId, String path, PartitionAttributes prAtt, Scope sc,
      EvictionAttributes ea, final ExpirationAttributes regionIdleTimeout,
      final ExpirationAttributes regionTimeToLive, final ExpirationAttributes entryIdleTimeout,
      final ExpirationAttributes entryTimeToLive, Set<String> gatewaySenderIds) {
    this.prId = prId;
    pAttrs = (PartitionAttributesImpl) prAtt;
    scope = sc;
    isDestroying = false;
    nodes = new VersionedArrayList();
    if (prAtt.getPartitionResolver() != null) {
      partitionResolver = prAtt.getPartitionResolver().getClass().getName();
    }
    colocatedWith = prAtt.getColocatedWith();
    if (prAtt.getLocalMaxMemory() > 0) {
      this.ea = ea;
      firstDataStoreCreated = prAtt.getLocalMaxMemory() > 0;
    }
    this.regionIdleTimeout = regionIdleTimeout;
    this.regionTimeToLive = regionTimeToLive;
    this.entryIdleTimeout = entryIdleTimeout;
    this.entryTimeToLive = entryTimeToLive;
    isColocationComplete = colocatedWith == null;
    fullPath = path;
    elderFPAs = new LinkedHashSet<>();
    PartitionListener[] prListeners = prAtt.getPartitionListeners();
    if (prListeners != null && prListeners.length != 0) {
      for (int i = 0; i < prListeners.length; i++) {
        PartitionListener listener = prListeners[i];
        partitionListenerClassNames.add(listener.getClass().getName());
      }
    }
    this.gatewaySenderIds = gatewaySenderIds;
  }

  public Set<String> getGatewaySenderIds() {
    return gatewaySenderIds;
  }

  /**
   * Returns a the list of nodes that participate in the PartitionedRegion
   *
   * @return a copy of the list of nodes that the caller is free to modify
   */
  Set<Node> getNodes() {
    if (nodes != null) {
      return nodes.getListCopy();
    }
    return null;
  }

  /**
   * Return a safe, light weight size of the nodes
   *
   * @return number of VMs that participate in the PartitionedRegion
   */
  int getNumberOfNodes() {
    if (nodes != null) {
      return nodes.size();
    } else {
      return 0;
    }
  }

  /**
   * Safely checks to see if the provided Node participates in the PartitionedRegion return true if
   * the Node participates in the PartitionedRegion
   */
  boolean containsNode(Node check) {
    if (nodes != null) {
      return nodes.contains(check);
    } else {
      return false;
    }
  }

  /**
   * Safely checks to see if the provided Node participates in the PartitionedRegion return true if
   * the Node participates in the PartitionedRegion
   */
  boolean containsMember(InternalDistributedMember memberId) {
    if (nodes != null) {
      for (Node node : nodes) {
        if (memberId.equals(node.getMemberId())) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Adds a new node to this configuration
   */
  void addNode(Node newNode) {
    if (nodes != null) {
      nodes.add(newNode);
    }
  }

  /**
   * Removes a node from this configuration
   */
  void removeNode(Node targetNode) {
    if (nodes != null) {
      nodes.remove(targetNode);
    }
  }

  public int getPRId() {
    return prId;
  }

  PartitionAttributes getPartitionAttrs() {
    return pAttrs;
  }

  Scope getScope() {
    return scope;
  }

  @Override
  public String toString() {
    String ret = "PartitionRegionConfig@" + System.identityHashCode(this) + ";prId=" + prId
        + ";scope=" + scope + ";partition attributes=" + pAttrs + ";partitionResolver="
        + partitionResolver + ";colocatedWith=" + colocatedWith + ";eviction attributes="
        + ea + ";regionIdleTimeout= " + regionIdleTimeout + ";regionTimeToLive= "
        + regionTimeToLive + ";entryIdleTimeout= " + entryIdleTimeout
        + ";entryTimeToLive= " + entryTimeToLive + "'elderFPAs=" + elderFPAs
        + "'gatewaySenderIds=" + gatewaySenderIds + ";nodes=";
    if (nodes != null) {
      return ret + nodes;
    } else {
      return ret + "null";
    }
  }

  @Override
  public int getDSFID() {
    return PARTITION_REGION_CONFIG;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(prId);
    out.writeByte(scope.ordinal);
    InternalDataSerializer.invokeToData(pAttrs, out);
    out.writeBoolean(isDestroying);
    out.writeBoolean(isColocationComplete);
    InternalDataSerializer.invokeToData(nodes, out);
    DataSerializer.writeString(partitionResolver, out);
    DataSerializer.writeString(colocatedWith, out);
    DataSerializer.writeString(fullPath, out);
    InternalDataSerializer.invokeToData(ea, out);
    InternalDataSerializer.invokeToData(regionIdleTimeout, out);
    InternalDataSerializer.invokeToData(regionTimeToLive, out);
    InternalDataSerializer.invokeToData(entryIdleTimeout, out);
    InternalDataSerializer.invokeToData(entryTimeToLive, out);
    out.writeBoolean(firstDataStoreCreated);
    DataSerializer.writeObject(elderFPAs, out);
    DataSerializer.writeArrayList(partitionListenerClassNames, out);
    if (gatewaySenderIds.isEmpty()) {
      DataSerializer.writeObject(null, out);
    } else {
      DataSerializer.writeObject(gatewaySenderIds, out);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    prId = in.readInt();
    scope = Scope.fromOrdinal(in.readByte());
    pAttrs = PartitionAttributesImpl.createFromData(in);
    isDestroying = in.readBoolean();
    isColocationComplete = in.readBoolean();
    nodes = new VersionedArrayList();
    InternalDataSerializer.invokeFromData(nodes, in);
    partitionResolver = DataSerializer.readString(in);
    colocatedWith = DataSerializer.readString(in);
    fullPath = DataSerializer.readString(in);
    ea = EvictionAttributesImpl.createFromData(in);
    regionIdleTimeout = ExpirationAttributes.createFromData(in);
    regionTimeToLive = ExpirationAttributes.createFromData(in);
    entryIdleTimeout = ExpirationAttributes.createFromData(in);
    entryTimeToLive = ExpirationAttributes.createFromData(in);
    firstDataStoreCreated = in.readBoolean();
    elderFPAs = DataSerializer.readObject(in);
    if (elderFPAs == null) {
      elderFPAs = new LinkedHashSet<>();
    }
    partitionListenerClassNames = DataSerializer.readArrayList(in);
    gatewaySenderIds = DataSerializer.readObject(in);
    if (gatewaySenderIds == null) {
      gatewaySenderIds = Collections.emptySet();
    }
  }

  /**
   * This method returns true is a node has taken a responsibility of destroying the
   * PartitionedRegion globally
   *
   * @return true, if a node has taken a responsibility of destroying the PartitionedRegion globally
   *         else it returns false
   */
  boolean getIsDestroying() {
    return isDestroying;
  }

  /**
   * This method sets the isDestroying flag to true, to indicate that the PartitionedRegion's
   * destruction responsibility is taken up by a node.
   *
   */
  void setIsDestroying() {
    isDestroying = true;
  }

  void setColocationComplete(PartitionedRegion partitionedRegion) {
    isColocationComplete = true;
    partitionedRegion.executeColocationCallbacks();
  }

  public void setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    entryIdleTimeout = idleTimeout;
  }

  public void setEntryTimeToLive(ExpirationAttributes timeToLive) {
    entryTimeToLive = timeToLive;
  }

  public boolean isGreaterNodeListVersion(final PartitionRegionConfig other) {
    return nodes.isNewerThan(other.nodes);
  }

  @Override
  public Comparable getVersion() {
    return nodes.getVersion();
  }

  @Override
  public boolean isNewerThan(Versionable other) {
    return nodes.isNewerThan(other);
  }

  @Override
  public boolean isSame(Versionable other) {
    return nodes.isSame(other);
  }

  @Override
  public boolean isOlderThan(Versionable other) {
    return nodes.isOlderThan(other);
  }

  public String getResolverClassName() {
    return partitionResolver;
  }

  public String getColocatedWith() {
    return colocatedWith;
  }

  public String getFullPath() {
    return fullPath;
  }

  public boolean isColocationComplete() {
    return isColocationComplete;
  }

  public EvictionAttributes getEvictionAttributes() {
    return ea;
  }

  public ExpirationAttributes getEntryIdleTimeout() {
    return entryIdleTimeout;
  }

  public ExpirationAttributes getEntryTimeToLive() {
    return entryTimeToLive;
  }

  public ExpirationAttributes getRegionIdleTimeout() {
    return regionIdleTimeout;
  }

  public ExpirationAttributes getRegionTimeToLive() {
    return regionTimeToLive;
  }

  public boolean isFirstDataStoreCreated() {
    return firstDataStoreCreated;
  }

  public void addFPAs(List<FixedPartitionAttributesImpl> fpaList) {
    if (elderFPAs != null) {
      elderFPAs.addAll(fpaList);
    }
  }

  public Set<FixedPartitionAttributesImpl> getElderFPAs() {
    return elderFPAs;
  }

  public ArrayList<String> getPartitionListenerClassNames() {
    return partitionListenerClassNames;
  }

  public boolean hasSameDataStoreMembers(PartitionRegionConfig prConfig) {
    for (Node node : getNodes()) {
      if (!prConfig.containsMember(node.getMemberId())
          && ((node.getPRType() == Node.ACCESSOR_DATASTORE)
              || (node.getPRType() == Node.FIXED_PR_DATASTORE))) {
        return false;
      }
    }
    for (Node node : prConfig.getNodes()) {
      if (!containsMember(node.getMemberId()) && ((node.getPRType() == Node.ACCESSOR_DATASTORE)
          || (node.getPRType() == Node.FIXED_PR_DATASTORE))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  public void setDatastoreCreated(EvictionAttributes evictionAttributes) {
    firstDataStoreCreated = true;
    ea = evictionAttributes;
  }
}
