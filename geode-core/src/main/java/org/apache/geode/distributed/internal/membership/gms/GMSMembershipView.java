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
package org.apache.geode.distributed.internal.membership.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

/**
 * The GMSMembershipView class represents a membership view. Note that this class is not
 * synchronized, so take
 * that under advisement if you decide to modify a view with add() or remove().
 *
 */
public class GMSMembershipView implements DataSerializableFixedID {
  private static final Logger logger = LogService.getLogger();

  private int viewId;
  private List<GMSMember> members;
  // TODO this should be a List
  private final Map<GMSMember, Object> publicKeys = new ConcurrentHashMap<>();
  private int[] failureDetectionPorts = new int[10];
  private Set<GMSMember> shutdownMembers;
  private Set<GMSMember> crashedMembers;
  private GMSMember creator;
  private Set<GMSMember> hashedMembers;
  private final Object membersLock = new Object();
  @Immutable
  public static final Random RANDOM = new Random();


  public GMSMembershipView() {
    viewId = 0;
    members = new ArrayList<>(4);
    this.hashedMembers = new HashSet<>(members);
    shutdownMembers = Collections.emptySet();
    crashedMembers = new HashSet<>();
    creator = null;
    Arrays.fill(failureDetectionPorts, -1);
  }

  public GMSMembershipView(GMSMember creator) {
    viewId = 0;
    members = new ArrayList<>(4);
    members.add(creator);
    hashedMembers = new HashSet<>(members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    this.creator = creator;
    Arrays.fill(failureDetectionPorts, -1);
  }

  public GMSMembershipView(GMSMember creator, int viewId,
      List<GMSMember> members) {
    this.viewId = viewId;
    this.members = new ArrayList<>(members);
    hashedMembers = new HashSet<>(this.members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    this.creator = creator;
    Arrays.fill(failureDetectionPorts, -1);

  }

  /**
   * Test method
   *
   * @param size size of the view, used for presizing collections
   * @param viewId the ID of the view
   */
  public GMSMembershipView(int size, long viewId) {
    this.viewId = (int) viewId;
    members = new ArrayList<>(size);
    this.hashedMembers = new HashSet<>();
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    creator = null;
    Arrays.fill(failureDetectionPorts, -1);
  }

  /**
   * Create a new view with the contents of the given view and the specified view ID
   */
  public GMSMembershipView(GMSMembershipView other, int viewId) {
    this.creator = other.creator;
    this.viewId = viewId;
    this.members = new ArrayList<>(other.members);
    this.hashedMembers = new HashSet<>(other.members);
    this.failureDetectionPorts = new int[other.failureDetectionPorts.length];
    System.arraycopy(other.failureDetectionPorts, 0, this.failureDetectionPorts, 0,
        other.failureDetectionPorts.length);
    this.shutdownMembers = new HashSet<>(other.shutdownMembers);
    this.crashedMembers = new HashSet<>(other.crashedMembers);
    this.publicKeys.putAll(other.publicKeys);
  }

  public GMSMembershipView(GMSMember creator, int viewId,
      List<GMSMember> mbrs, Set<GMSMember> shutdowns,
      Set<GMSMember> crashes) {
    this.creator = creator;
    this.viewId = viewId;
    this.members = mbrs;
    this.hashedMembers = new HashSet<>(mbrs);
    this.shutdownMembers = shutdowns;
    this.crashedMembers = crashes;
    this.failureDetectionPorts = new int[mbrs.size() + 10];
    Arrays.fill(this.failureDetectionPorts, -1);
  }


  public int getViewId() {
    return this.viewId;
  }


  public GMSMember getCreator() {
    return this.creator;
  }

  public void setCreator(GMSMember creator) {
    this.creator = creator;
  }

  public Object getPublicKey(GMSMember mbr) {
    return publicKeys.get(mbr);
  }

  public void setPublicKey(GMSMember mbr, Object key) {
    if (mbr != null && key != null) {
      publicKeys.put(mbr, key);
    }
  }

  public void setPublicKeys(GMSMembershipView otherView) {
    if (otherView.publicKeys != null) {
      this.publicKeys.putAll(otherView.publicKeys);
    }
  }

  public void setViewId(int viewId) {
    this.viewId = viewId;
  }



  public int[] getFailureDetectionPorts() {
    return this.failureDetectionPorts;
  }

  public int getFailureDetectionPort(GMSMember mbr) {
    int idx = members.indexOf(mbr);
    if (idx < 0 || idx >= failureDetectionPorts.length) {
      return -1;
    }
    return failureDetectionPorts[idx];
  }


  public void setFailureDetectionPort(GMSMember mbr, int port) {
    int idx = members.indexOf(mbr);
    if (idx < 0) {
      throw new IllegalArgumentException("element not found in members list:" + mbr);
    }
    ensureFDCapacity(idx);
    failureDetectionPorts[idx] = port;
  }

  /**
   * Transfer the failure-detection ports from another view to this one
   */
  public void setFailureDetectionPorts(GMSMembershipView otherView) {
    int[] ports = otherView.getFailureDetectionPorts();
    if (ports != null) {
      int idx = 0;
      int portsSize = ports.length;
      for (GMSMember mbr : otherView.getMembers()) {
        if (contains(mbr)) {
          // unit tests create views w/o failure detection ports, so we must check the length
          // of the array
          if (idx < portsSize) {
            setFailureDetectionPort(mbr, ports[idx]);
          } else {
            setFailureDetectionPort(mbr, -1);
          }
        }
        idx += 1;
      }
    }
  }

  /**
   * ensures that there is a slot at idx to store an int
   */
  private void ensureFDCapacity(int idx) {
    if (idx >= failureDetectionPorts.length) {
      int[] p = new int[idx + 10];
      if (failureDetectionPorts.length > 0) {
        System.arraycopy(failureDetectionPorts, 0, p, 0, failureDetectionPorts.length);
      }
      Arrays.fill(p, idx, idx + 9, -1);
      failureDetectionPorts = p;
    }
  }

  public Object get(int i) {
    return this.members.get(i);
  }

  public void add(GMSMember mbr) {
    this.hashedMembers.add(mbr);
    this.members.add(mbr);
    int idx = members.size() - 1;
    ensureFDCapacity(idx);
    this.failureDetectionPorts[idx] = -1;
  }

  public void addCrashedMembers(Set<GMSMember> mbr) {
    this.crashedMembers.addAll(mbr);
  }

  /**
   * return members that are i this view but not the given old view
   */
  public List<GMSMember> getNewMembers(GMSMembershipView olderView) {
    List<GMSMember> result = new ArrayList<>(members);
    result.removeAll(olderView.getMembers());
    return result;
  }

  /**
   * return members added in this view
   */
  public List<GMSMember> getNewMembers() {
    List<GMSMember> result = new ArrayList<>(5);
    result.addAll(this.members.stream().filter(mbr -> mbr.getVmViewId() == this.viewId)
        .collect(Collectors.toList()));
    return result;
  }

  public boolean remove(GMSMember mbr) {
    this.hashedMembers.remove(mbr);
    int idx = this.members.indexOf(mbr);
    if (idx >= 0) {
      System.arraycopy(failureDetectionPorts, idx + 1, failureDetectionPorts, idx,
          failureDetectionPorts.length - idx - 1);
      failureDetectionPorts[failureDetectionPorts.length - 1] = -1;
    }
    return this.members.remove(mbr);
  }

  public void removeAll(Collection<GMSMember> ids) {
    this.hashedMembers.removeAll(ids);
    ids.forEach(this::remove);
  }

  public boolean contains(GMSMember mbr) {
    return this.hashedMembers.contains(mbr);
  }

  public int size() {
    return this.members.size();
  }

  public GMSMember getLeadMember() {
    for (GMSMember mbr : this.members) {
      if (mbr.getVmKind() == GMSMember.NORMAL_DM_TYPE) {
        return mbr;
      }
    }
    return null;
  }

  public GMSMember getCoordinator() {
    synchronized (membersLock) {
      for (GMSMember addr : members) {
        if (addr.preferredForCoordinator()) {
          return addr;
        }
      }
      if (members.size() > 0) {
        return members.get(0);
      }
    }
    return null;
  }

  /**
   * Returns the coordinator of this view, rejecting any in the given collection of IDs
   */
  public GMSMember getCoordinator(
      Collection<GMSMember> rejections) {
    if (rejections == null) {
      return getCoordinator();
    }
    synchronized (membersLock) {
      for (GMSMember addr : members) {
        if (addr.preferredForCoordinator() && !rejections.contains(addr)) {
          return addr;
        }
      }
      for (GMSMember addr : members) {
        if (!rejections.contains(addr)) {
          return addr;
        }
      }
    }
    return null;
  }

  /***
   * This functions returns the list of preferred coordinators. One random member from list of
   * non-preferred member list. It make sure that random member is not in suspected Set. And local
   * member.
   *
   * @param filter Suspect member set.
   * @param localAddress the address of this member
   * @param maxNumberDesired number of preferred coordinators to return
   * @return list of preferred coordinators
   */
  public List<GMSMember> getPreferredCoordinators(
      Set<GMSMember> filter, GMSMember localAddress,
      int maxNumberDesired) {
    List<GMSMember> results = new ArrayList<>();
    List<GMSMember> notPreferredCoordinatorList = new ArrayList<>();

    synchronized (membersLock) {
      for (GMSMember addr : members) {
        if (addr.equals(localAddress)) {
          continue;// this is must to add
        }
        if (addr.preferredForCoordinator() && !filter.contains(addr)) {
          results.add(addr);
          if (results.size() >= maxNumberDesired) {
            break;
          }
        } else if (!filter.contains(addr)) {
          notPreferredCoordinatorList.add(addr);
        }
      }

      results.add(localAddress);// to add local address

      if (results.size() < maxNumberDesired && notPreferredCoordinatorList.size() > 0) {
        Iterator<GMSMember> it = notPreferredCoordinatorList.iterator();
        while (it.hasNext() && results.size() < maxNumberDesired) {
          results.add(it.next());
        }
      }
    }

    return results;
  }

  /* NetView implementation method */

  public List<GMSMember> getGMSMembers() {
    return (List<GMSMember>) (List<?>) Collections.unmodifiableList(this.members);
  }


  public List<GMSMember> getMembers() {
    return Collections.unmodifiableList(this.members);
  }

  public Set<GMSMember> getShutdownMembers() {
    return this.shutdownMembers;
  }

  public Set<GMSMember> getCrashedMembers() {
    return this.crashedMembers;
  }

  /** check to see if the given address is next in line to be coordinator */
  public boolean shouldBeCoordinator(GMSMember who) {
    Iterator<GMSMember> it = this.members.iterator();
    GMSMember firstNonPreferred = null;
    while (it.hasNext()) {
      GMSMember mbr = it.next();
      if (mbr.preferredForCoordinator()) {
        return mbr.equals(who);
      } else if (firstNonPreferred == null) {
        firstNonPreferred = mbr;
      }
    }
    return (firstNonPreferred == null || firstNonPreferred.equals(who));
  }

  /**
   * returns the weight of the members in this membership view
   */
  public int memberWeight() {
    int result = 0;
    GMSMember lead = getLeadMember();
    for (GMSMember mbr : this.members) {
      result += mbr.getMemberWeight();
      switch (mbr.getVmKind()) {
        case GMSMember.NORMAL_DM_TYPE:
          result += 10;
          if (lead != null && mbr.equals(lead)) {
            result += 5;
          }
          break;
        case GMSMember.LOCATOR_DM_TYPE:
          result += 3;
          break;
        case GMSMember.ADMIN_ONLY_DM_TYPE:
          break;
        default:
          throw new IllegalStateException("Unknown member type: " + mbr.getVmKind());
      }
    }
    return result;
  }

  /**
   * returns the weight of crashed members in this membership view with respect to the given
   * previous view
   */
  public int getCrashedMemberWeight(GMSMembershipView oldView) {
    int result = 0;
    GMSMember lead = oldView.getLeadMember();
    for (GMSMember mbr : this.crashedMembers) {
      if (!oldView.contains(mbr)) {
        continue;
      }
      result += mbr.getMemberWeight();
      switch (mbr.getVmKind()) {
        case GMSMember.NORMAL_DM_TYPE:
          result += 10;
          if (lead != null && mbr.equals(lead)) {
            result += 5;
          }
          break;
        case GMSMember.LOCATOR_DM_TYPE:
          result += 3;
          break;
        case GMSMember.ADMIN_ONLY_DM_TYPE:
          break;
        default:
          throw new IllegalStateException("Unknown member type: " + mbr.getVmKind());
      }
    }
    return result;
  }

  /**
   * returns the members of this views crashedMembers collection that were members of the given
   * view. Admin-only members are not counted
   */
  public Set<GMSMember> getActualCrashedMembers(GMSMembershipView oldView) {
    Set<GMSMember> result = new HashSet<>(this.crashedMembers.size());
    result.addAll(this.crashedMembers.stream()
        .filter(mbr -> (mbr.getVmKind() != GMSMember.ADMIN_ONLY_DM_TYPE))
        .filter(mbr -> oldView == null || oldView.contains(mbr)).collect(Collectors.toList()));
    return result;
  }

  /**
   * logs the weight of failed members wrt the given previous view
   */
  public void logCrashedMemberWeights(GMSMembershipView oldView, Logger log) {
    GMSMember lead = oldView.getLeadMember();
    for (GMSMember mbr : this.crashedMembers) {
      if (!oldView.contains(mbr)) {
        continue;
      }
      int mbrWeight = mbr.getMemberWeight();
      switch (mbr.getVmKind()) {
        case GMSMember.NORMAL_DM_TYPE:
          if (lead != null && mbr.equals(lead)) {
            mbrWeight += 15;
          } else {
            mbrWeight += 10;
          }
          break;
        case GMSMember.LOCATOR_DM_TYPE:
          mbrWeight += 3;
          break;
        case GMSMember.ADMIN_ONLY_DM_TYPE:
          break;
        default:
          throw new IllegalStateException("Unknown member type: " + mbr.getVmKind());
      }
      log.info("  " + mbr + " had a weight of " + mbrWeight);
    }
  }

  public String toString() {
    GMSMember lead = getLeadMember();

    StringBuilder sb = new StringBuilder(200);
    sb.append("View[").append(creator).append('|').append(viewId).append("] members: [");
    boolean first = true;
    for (GMSMember mbr : this.members) {
      if (!first)
        sb.append(", ");
      sb.append(mbr);
      if (mbr == lead) {
        sb.append("{lead}");
      }
      first = false;
    }
    if (!this.shutdownMembers.isEmpty()) {
      sb.append("]  shutdown: [");
      first = true;
      for (GMSMember mbr : this.shutdownMembers) {
        if (!first)
          sb.append(", ");
        sb.append(mbr);
        first = false;
      }
    }
    if (!this.crashedMembers.isEmpty()) {
      sb.append("]  crashed: [");
      first = true;
      for (GMSMember mbr : this.crashedMembers) {
        if (!first)
          sb.append(", ");
        sb.append(mbr);
        first = false;
      }
    }
    // sb.append("] fd ports: [");
    // int[] ports = getFailureDetectionPorts();
    // int numMembers = size();
    // for (int i=0; i<numMembers; i++) {
    // if (i > 0) {
    // sb.append(' ');
    // }
    // sb.append(ports[i]);
    // }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Returns the ID from this view that is equal to the argument. If no such ID exists the argument
   * is returned.
   */
  public synchronized GMSMember getCanonicalID(GMSMember id) {
    if (hashedMembers.contains(id)) {
      for (GMSMember m : this.members) {
        if (id.equals(m)) {
          return m;
        }
      }
    }
    return id;
  }

  @Override
  public synchronized boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof GMSMembershipView) {
      return this.members.equals(((GMSMembershipView) other).getGMSMembers());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return this.members.hashCode();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    GMSUtil.writeMemberID(creator, out);
    out.writeInt(viewId);
    writeAsArrayList(members, out);
    GMSUtil.writeSetOfMemberIDs(shutdownMembers, out);
    GMSUtil.writeSetOfMemberIDs(crashedMembers, out);
    DataSerializer.writeIntArray(failureDetectionPorts, out);
    // TODO expensive serialization
    DataSerializer.writeHashMap(publicKeys, out);
  }


  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    creator = GMSUtil.readMemberID(in);
    viewId = in.readInt();
    members = GMSUtil.readArrayOfIDs(in);
    assert members != null;
    this.hashedMembers = new HashSet<>(members);
    shutdownMembers = GMSUtil.readHashSetOfMemberIDs(in);
    crashedMembers = GMSUtil.readHashSetOfMemberIDs(in);
    failureDetectionPorts = DataSerializer.readIntArray(in);
    Map pubkeys = DataSerializer.readHashMap(in);
    if (pubkeys != null) {
      publicKeys.putAll(pubkeys);
    }
  }

  /** this will deserialize as an ArrayList */
  private void writeAsArrayList(List<GMSMember> list, DataOutput out) throws IOException {
    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        GMSUtil.writeMemberID(list.get(i), out);
      }
    }
  }

  private void writeAsSet(Set<GMSMember> set, DataOutput out) throws IOException {
    int size;
    if (set == null) {
      size = -1;
    } else {
      size = set.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      for (GMSMember member : set) {
        GMSUtil.writeMemberID(member, out);
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return NETVIEW;
  }

}
