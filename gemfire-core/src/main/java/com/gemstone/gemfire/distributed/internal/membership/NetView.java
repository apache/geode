/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership;

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
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;

/**
 * The NetView class represents a membership view. Note that
 * this class is not synchronized, so take that under advisement
 * if you decide to modify a view with add() or remove().
 * 
 * @since 5.5
 */
public class NetView implements DataSerializableFixedID {

  private int viewId;
  private List<InternalDistributedMember> members;
  private int[] failureDetectionPorts = new int[10];
  private Set<InternalDistributedMember> shutdownMembers;
  private Set<InternalDistributedMember> crashedMembers;
  private InternalDistributedMember creator;
  private Set<InternalDistributedMember> hashedMembers;
  static public final Random RANDOM = new Random();

  
  public NetView() {
    viewId = 0;
    members = new ArrayList<InternalDistributedMember>(4);
    this.hashedMembers = new HashSet<>(members);
    shutdownMembers = Collections.emptySet();
    crashedMembers = new HashSet<>();
    creator = null;
    Arrays.fill(failureDetectionPorts, -1);
  }

  public NetView(InternalDistributedMember creator) {
    viewId = 0;
    members = new ArrayList<InternalDistributedMember>(4);
    members.add(creator);
    hashedMembers = new HashSet<>(members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    this.creator = creator;
    Arrays.fill(failureDetectionPorts, -1);
  }

  public NetView(InternalDistributedMember creator, int viewId, List<InternalDistributedMember> members) {
    this.viewId = viewId;
    this.members = new ArrayList<InternalDistributedMember>(members);
    hashedMembers = new HashSet<>(this.members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    this.creator = creator;
    Arrays.fill(failureDetectionPorts, -1);
  }

  // legacy method for JGMM
  public NetView(int size, long viewId) {
    this.viewId = (int) viewId;
    members = new ArrayList<InternalDistributedMember>(size);
    this.hashedMembers = new HashSet<InternalDistributedMember>();
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    creator = null;
    Arrays.fill(failureDetectionPorts, -1);
  }

  /**
   * Create a new view with the contents of the given view and the
   * specified view ID
   * @param other
   * @param viewId
   */
  public NetView(NetView other, int viewId) {
    this.creator = other.creator;
    this.viewId = viewId;
    this.members = new ArrayList<InternalDistributedMember>(other.members);
    this.hashedMembers = new HashSet<InternalDistributedMember>(other.members);
    this.failureDetectionPorts = new int[other.failureDetectionPorts.length];
    System.arraycopy(other.failureDetectionPorts, 0, this.failureDetectionPorts, 0, other.failureDetectionPorts.length);
    this.shutdownMembers = new HashSet<InternalDistributedMember>(other.shutdownMembers);
    this.crashedMembers = new HashSet<InternalDistributedMember>(other.crashedMembers);
  }

  public NetView(InternalDistributedMember creator, int viewId, List<InternalDistributedMember> mbrs, Set<InternalDistributedMember> shutdowns,
      Set<InternalDistributedMember> crashes) {
    this.creator = creator;
    this.viewId = viewId;
    this.members = mbrs;
    this.hashedMembers = new HashSet<InternalDistributedMember>(mbrs);
    this.shutdownMembers = shutdowns;
    this.crashedMembers = crashes;
    this.failureDetectionPorts = new int[mbrs.size()+10];
    Arrays.fill(this.failureDetectionPorts, -1);
  }

  public int getViewId() {
    return this.viewId;
  }

  public InternalDistributedMember getCreator() {
    return this.creator;
  }

  public void setCreator(InternalDistributedMember creator) {
    this.creator = creator;
  }
  
  public int[] getFailureDetectionPorts() {
    return this.failureDetectionPorts;
  }
  
  public int getFailureDetectionPort(InternalDistributedMember mbr) {
    int idx = members.indexOf(mbr);
    if (idx < 0 || idx >= failureDetectionPorts.length) {
      return -1;
    }
    return failureDetectionPorts[idx];
  }
  
  public void setFailureDetectionPort(InternalDistributedMember mbr, int port) {
    int idx = members.indexOf(mbr);
    if (idx < 0) {
      throw new IllegalArgumentException("element not found in members list:" + mbr);
    }
    ensureFDCapacity(idx);
    failureDetectionPorts[idx] = port;
  }
  
  /**
   * Transfer the failure-detection ports from another view to this one
   * @param otherView
   */
  public void setFailureDetectionPorts(NetView otherView) {
    int[] ports = otherView.getFailureDetectionPorts();
    if (ports != null) {
      int idx = 0;
      int portsSize = ports.length;
      for (InternalDistributedMember mbr: otherView.getMembers()) {
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
      int[] p = new int[idx+10];
      if (failureDetectionPorts.length > 0) {
        System.arraycopy(failureDetectionPorts, 0, p, 0, failureDetectionPorts.length);
      }
      Arrays.fill(p, idx, idx+9, -1);
      failureDetectionPorts = p;
    }
  }
  
  public List<InternalDistributedMember> getMembers() {
    return Collections.unmodifiableList(this.members);
  }
  
  /**
   * return members that are i this view but not the given old view
   */
  public List<InternalDistributedMember> getNewMembers(NetView olderView) {
    List<InternalDistributedMember> result = new ArrayList<InternalDistributedMember>(members);
    result.removeAll(olderView.getMembers());
    return result;
  }

  /**
   * return members added in this view
   */
  public List<InternalDistributedMember> getNewMembers() {
    List<InternalDistributedMember> result = new ArrayList<InternalDistributedMember>(5);
    for (InternalDistributedMember mbr : this.members) {
      if (mbr.getVmViewId() == this.viewId) {
        result.add(mbr);
      }
    }
    return result;
  }

  public Object get(int i) {
    return this.members.get(i);
  }

  public void add(InternalDistributedMember mbr) {
    this.hashedMembers.add(mbr);
    this.members.add(mbr);
    int idx = members.size()-1;
    ensureFDCapacity(idx);
    this.failureDetectionPorts[idx] = -1;
  }
  
  public void addCrashedMembers(Set<InternalDistributedMember> mbr) {
    this.crashedMembers.addAll(mbr);
  }

  public void addShutdownMembers(Set<InternalDistributedMember> mbr) {
    this.shutdownMembers.addAll(mbr);
  }

  public boolean remove(InternalDistributedMember mbr) {
    this.hashedMembers.remove(mbr);
    int idx = this.members.indexOf(mbr);
    if (idx >= 0) {
      System.arraycopy(failureDetectionPorts, idx+1, failureDetectionPorts, idx, failureDetectionPorts.length-idx-1);
      failureDetectionPorts[failureDetectionPorts.length-1] = -1;
    }
    return this.members.remove(mbr);
  }
  
  public void removeAll(Collection<InternalDistributedMember> ids) {
    this.hashedMembers.removeAll(ids);
    for (InternalDistributedMember mbr: ids) {
      remove(mbr);
    }
  }

  public boolean contains(InternalDistributedMember mbr) {
    return this.hashedMembers.contains(mbr);
  }

  public int size() {
    return this.members.size();
  }

  public InternalDistributedMember getLeadMember() {
    for (InternalDistributedMember mbr : this.members) {
      if (mbr.getVmKind() == DistributionManager.NORMAL_DM_TYPE) {
        return mbr;
      }
    }
    return null;
  }

  public InternalDistributedMember getCoordinator() {
    synchronized (members) {
      for (InternalDistributedMember addr : members) {
        if (addr.getNetMember().preferredForCoordinator()) {
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
   * Returns the coordinator of this view, rejecting any in the
   * given collection of IDs
   */
  public InternalDistributedMember getCoordinator(Collection<InternalDistributedMember> rejections) {
    if (rejections == null) {
      return getCoordinator();
    }
    synchronized (members) {
      for (InternalDistributedMember addr : members) {
        if (addr.getNetMember().preferredForCoordinator() && !rejections.contains(addr)) {
          return addr;
        }
      }
      for (InternalDistributedMember addr: members) {
        if (!rejections.contains(addr)) {
          return addr;
        }
      }
    }
    return null;
  }

  /***
   * This functions returns the list of preferred coordinators.
   * One random member from list of non-preferred member list. It make
   * sure that random member is not in suspected Set.
   * And local member.
   * 
   * @param filter Suspect member set.
   * @param localAddress
   * @param maxNumberDesired number of preferred coordinators to return
   * @return list of preferred coordinators
   */
  public List<InternalDistributedMember> getPreferredCoordinators(Set<InternalDistributedMember> filter, InternalDistributedMember localAddress, int maxNumberDesired) {
    List<InternalDistributedMember> results = new ArrayList<InternalDistributedMember>();
    List<InternalDistributedMember> notPreferredCoordinatorList = new ArrayList<InternalDistributedMember>();

    synchronized (members) {
      for (InternalDistributedMember addr : members) {
        if (addr.equals(localAddress)) {
          continue;// this is must to add
        }
        if (addr.getNetMember().preferredForCoordinator()) {
          results.add(addr);
          if (results.size() >= maxNumberDesired) {
            break;
          }
        } else if (!filter.contains(addr)) {
          notPreferredCoordinatorList.add(addr);
        }
      }

      results.add(localAddress);// to add local address

      if (notPreferredCoordinatorList.size() > 0) {
        int idx = RANDOM.nextInt(notPreferredCoordinatorList.size());
        results.add(notPreferredCoordinatorList.get(idx)); // to add non preferred local address
      }
    }

    return results;
  }

  public Set<InternalDistributedMember> getShutdownMembers() {
    return this.shutdownMembers;
  }

  public Set<InternalDistributedMember> getCrashedMembers() {
    return this.crashedMembers;
  }

  /** check to see if the given address is next in line to be coordinator */
  public boolean shouldBeCoordinator(InternalDistributedMember who) {
    Iterator<InternalDistributedMember> it = this.members.iterator();
    InternalDistributedMember firstNonPreferred = null;
    while (it.hasNext()) {
      InternalDistributedMember mbr = it.next();
      if (mbr.getNetMember().preferredForCoordinator()) {
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
    InternalDistributedMember lead = getLeadMember();
    for (InternalDistributedMember mbr : this.members) {
      result += mbr.getNetMember().getMemberWeight();
      switch (mbr.getVmKind()) {
      case DistributionManager.NORMAL_DM_TYPE:
        result += 10;
        if (lead != null && mbr.equals(lead)) {
          result += 5;
        }
        break;
      case DistributionManager.LOCATOR_DM_TYPE:
        result += 3;
        break;
      case DistributionManager.ADMIN_ONLY_DM_TYPE:
        break;
      default:
        throw new IllegalStateException("Unknown member type: " + mbr.getVmKind());
      }
    }
    return result;
  }

  /**
   * returns the weight of crashed members in this membership view
   * with respect to the given previous view
   */
  public int getCrashedMemberWeight(NetView oldView) {
    int result = 0;
    InternalDistributedMember lead = oldView.getLeadMember();
    for (InternalDistributedMember mbr : this.crashedMembers) {
      if (!oldView.contains(mbr)) {
        continue;
      }
      result += mbr.getNetMember().getMemberWeight();
      switch (mbr.getVmKind()) {
      case DistributionManager.NORMAL_DM_TYPE:
        result += 10;
        if (lead != null && mbr.equals(lead)) {
          result += 5;
        }
        break;
      case DistributionManager.LOCATOR_DM_TYPE:
        result += 3;
        break;
      case DistributionManager.ADMIN_ONLY_DM_TYPE:
        break;
      default:
        throw new IllegalStateException("Unknown member type: " + mbr.getVmKind());
      }
    }
    return result;
  }

  /**
   * returns the members of this views crashedMembers collection
   * that were members of the given view. Admin-only members are
   * not counted
   */
  public Set<InternalDistributedMember> getActualCrashedMembers(NetView oldView) {
    Set<InternalDistributedMember> result = new HashSet<>(this.crashedMembers.size());
    for (InternalDistributedMember mbr : this.crashedMembers) {
      if ((mbr.getVmKind() != DistributionManager.ADMIN_ONLY_DM_TYPE)) {
        if (oldView == null || oldView.contains(mbr)) {
          result.add(mbr);
        }
      }
    }
    return result;
  }

  /**
   * logs the weight of failed members wrt the given previous
   * view
   */
  public void logCrashedMemberWeights(NetView oldView, Logger log) {
    InternalDistributedMember lead = oldView.getLeadMember();
    for (InternalDistributedMember mbr : this.crashedMembers) {
      if (!oldView.contains(mbr)) {
        continue;
      }
      int mbrWeight = mbr.getNetMember().getMemberWeight();
      switch (mbr.getVmKind()) {
      case DistributionManager.NORMAL_DM_TYPE:
        if (lead != null && mbr.equals(lead)) {
          mbrWeight += 15;
        } else {
          mbrWeight += 10;
        }
        break;
      case DistributionManager.LOCATOR_DM_TYPE:
        mbrWeight += 3;
        break;
      case DistributionManager.ADMIN_ONLY_DM_TYPE:
        break;
      default:
        throw new IllegalStateException("Unknown member type: " + mbr.getVmKind());
      }
      log.info("  " + mbr + " had a weight of " + mbrWeight);
    }
  }

  public String toString() {
    InternalDistributedMember lead = getLeadMember();

    StringBuilder sb = new StringBuilder(200);
    sb.append("View[").append(creator).append('|').append(viewId).append("] members: [");
    boolean first = true;
    for (InternalDistributedMember mbr : this.members) {
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
      for (InternalDistributedMember mbr : this.shutdownMembers) {
        if (!first)
          sb.append(", ");
        sb.append(mbr);
        first = false;
      }
    }
    if (!this.crashedMembers.isEmpty()) {
      sb.append("]  crashed: [");
      first = true;
      for (InternalDistributedMember mbr : this.crashedMembers) {
        if (!first)
          sb.append(", ");
        sb.append(mbr);
        first = false;
      }
    }
//    sb.append("] fd ports: [");
//    int[] ports = getFailureDetectionPorts();
//    int numMembers = size();
//    for (int i=0; i<numMembers; i++) {
//      if (i > 0) {
//        sb.append(' ');
//      }
//      sb.append(ports[i]);
//    }
    sb.append("]");
    return sb.toString();
  }
  
  /**
   * Returns the ID from this view that is equal to the argument.
   * If no such ID exists the argument is returned.
   */
  public synchronized InternalDistributedMember getCanonicalID(InternalDistributedMember id) {
    if (hashedMembers.contains(id)) {
      for (InternalDistributedMember m: this.members) {
        if (id.equals(m)) {
          return m;
        }
      }
    }
    return id;
  }

  @Override
  public synchronized boolean equals(Object arg0) {
    if (arg0 == this) {
      return true;
    }
    if (!(arg0 instanceof NetView)) {
      return false;
    }
    return this.members.equals(((NetView) arg0).getMembers());
  }

  @Override
  public synchronized int hashCode() {
    return this.members.hashCode();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(creator, out);
    out.writeInt(viewId);
    writeAsArrayList(members, out);
    InternalDataSerializer.writeSet(shutdownMembers, out);
    InternalDataSerializer.writeSet(crashedMembers, out);
    DataSerializer.writeIntArray(failureDetectionPorts, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    creator = DataSerializer.readObject(in);
    viewId = in.readInt();
    members = DataSerializer.readArrayList(in);
    this.hashedMembers = new HashSet<InternalDistributedMember>(members);
    shutdownMembers = InternalDataSerializer.readHashSet(in);
    crashedMembers = InternalDataSerializer.readHashSet(in);
    failureDetectionPorts = DataSerializer.readIntArray(in);
  }

  /** this will deserialize as an ArrayList */
  private void writeAsArrayList(List list, DataOutput out) throws IOException {
    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        DataSerializer.writeObject(list.get(i), out);
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
