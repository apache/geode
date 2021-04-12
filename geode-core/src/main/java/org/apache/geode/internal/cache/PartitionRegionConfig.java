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
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectSerializer;
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

  private ArrayList<String> partitionListenerClassNames = new ArrayList<String>();

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
    this.pAttrs = (PartitionAttributesImpl) prAtt;
    this.scope = sc;
    this.isDestroying = false;
    this.nodes = new VersionedArrayList();
    if (prAtt.getPartitionResolver() != null) {
      this.partitionResolver = prAtt.getPartitionResolver().getClass().getName();
    }
    this.colocatedWith = prAtt.getColocatedWith();
    if (prAtt.getLocalMaxMemory() > 0) {
      this.ea = ea;
      this.firstDataStoreCreated = prAtt.getLocalMaxMemory() > 0;
    }
    this.regionIdleTimeout = regionIdleTimeout;
    this.regionTimeToLive = regionTimeToLive;
    this.entryIdleTimeout = entryIdleTimeout;
    this.entryTimeToLive = entryTimeToLive;
    this.isColocationComplete = colocatedWith == null;
    this.fullPath = path;
    this.elderFPAs = new LinkedHashSet<FixedPartitionAttributesImpl>();
    PartitionListener[] prListeners = prAtt.getPartitionListeners();
    if (prListeners != null && prListeners.length != 0) {
      for (int i = 0; i < prListeners.length; i++) {
        PartitionListener listener = prListeners[i];
        this.partitionListenerClassNames.add(listener.getClass().getName());
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
    if (this.nodes != null) {
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
    if (this.nodes != null) {
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
    if (this.nodes != null) {
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
    if (this.nodes != null) {
      for (Node node : this.nodes) {
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
    String ret = "PartitionRegionConfig@" + System.identityHashCode(this) + ";prId=" + this.prId
        + ";scope=" + this.scope + ";partition attributes=" + this.pAttrs + ";partitionResolver="
        + this.partitionResolver + ";colocatedWith=" + this.colocatedWith + ";eviction attributes="
        + this.ea + ";regionIdleTimeout= " + this.regionIdleTimeout + ";regionTimeToLive= "
        + this.regionTimeToLive + ";entryIdleTimeout= " + this.entryIdleTimeout
        + ";entryTimeToLive= " + this.entryTimeToLive + "'elderFPAs=" + elderFPAs
        + "'gatewaySenderIds=" + gatewaySenderIds + ";nodes=";
    if (this.nodes != null) {
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
    final ObjectSerializer serializer = context.getSerializer();
    out.writeInt(this.prId);
    out.writeByte(this.scope.ordinal);
    serializer.invokeToData(this.pAttrs, out);
    out.writeBoolean(this.isDestroying);
    out.writeBoolean(this.isColocationComplete);
    serializer.invokeToData(this.nodes, out);
    DataSerializer.writeString(this.partitionResolver, out);
    DataSerializer.writeString(this.colocatedWith, out);
    DataSerializer.writeString(this.fullPath, out);
    serializer.invokeToData(this.ea, out);
    serializer.invokeToData(this.regionIdleTimeout, out);
    serializer.invokeToData(this.regionTimeToLive, out);
    serializer.invokeToData(this.entryIdleTimeout, out);
    serializer.invokeToData(this.entryTimeToLive, out);
    out.writeBoolean(this.firstDataStoreCreated);
    DataSerializer.writeObject(elderFPAs, out);
    DataSerializer.writeArrayList(this.partitionListenerClassNames, out);
    if (this.gatewaySenderIds.isEmpty()) {
      DataSerializer.writeObject(null, out);
    } else {
      DataSerializer.writeObject(this.gatewaySenderIds, out);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.prId = in.readInt();
    this.scope = Scope.fromOrdinal(in.readByte());
    this.pAttrs = PartitionAttributesImpl.createFromData(in);
    this.isDestroying = in.readBoolean();
    this.isColocationComplete = in.readBoolean();
    this.nodes = new VersionedArrayList();
    context.getDeserializer().invokeFromData(this.nodes, in);
    this.partitionResolver = DataSerializer.readString(in);
    this.colocatedWith = DataSerializer.readString(in);
    this.fullPath = DataSerializer.readString(in);
    this.ea = EvictionAttributesImpl.createFromData(in);
    this.regionIdleTimeout = ExpirationAttributes.createFromData(in);
    this.regionTimeToLive = ExpirationAttributes.createFromData(in);
    this.entryIdleTimeout = ExpirationAttributes.createFromData(in);
    this.entryTimeToLive = ExpirationAttributes.createFromData(in);
    this.firstDataStoreCreated = in.readBoolean();
    this.elderFPAs = DataSerializer.readObject(in);
    if (this.elderFPAs == null) {
      this.elderFPAs = new LinkedHashSet<FixedPartitionAttributesImpl>();
    }
    this.partitionListenerClassNames = DataSerializer.readArrayList(in);
    this.gatewaySenderIds = DataSerializer.readObject(in);
    if (this.gatewaySenderIds == null) {
      this.gatewaySenderIds = Collections.emptySet();
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
    this.isColocationComplete = true;
    partitionedRegion.executeColocationCallbacks();
  }

  public void setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    this.entryIdleTimeout = idleTimeout;
  }

  public void setEntryTimeToLive(ExpirationAttributes timeToLive) {
    this.entryTimeToLive = timeToLive;
  }

  public boolean isGreaterNodeListVersion(final PartitionRegionConfig other) {
    return this.nodes.isNewerThan(other.nodes);
  }

  @Override
  public Comparable getVersion() {
    return this.nodes.getVersion();
  }

  @Override
  public boolean isNewerThan(Versionable other) {
    return this.nodes.isNewerThan(other);
  }

  @Override
  public boolean isSame(Versionable other) {
    return this.nodes.isSame(other);
  }

  @Override
  public boolean isOlderThan(Versionable other) {
    return this.nodes.isOlderThan(other);
  }

  public String getResolverClassName() {
    return this.partitionResolver;
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
    if (this.elderFPAs != null) {
      this.elderFPAs.addAll(fpaList);
    }
  }

  public Set<FixedPartitionAttributesImpl> getElderFPAs() {
    return this.elderFPAs;
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
      if (!this.containsMember(node.getMemberId()) && ((node.getPRType() == Node.ACCESSOR_DATASTORE)
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
    this.firstDataStoreCreated = true;
    this.ea = evictionAttributes;
  }
}
