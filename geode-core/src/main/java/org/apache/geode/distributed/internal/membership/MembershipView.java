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
package org.apache.geode.distributed.internal.membership;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;

/**
 * The MembershipView class represents a membership view. Note that this class is not synchronized,
 * so take
 * that under advisement if you decide to modify a view with add() or remove().
 *
 * @since GemFire 5.5
 */
public class MembershipView {

  private int viewId;
  private List<InternalDistributedMember> members;
  // TODO this should be a List
  private final Map<InternalDistributedMember, Object> publicKeys = new ConcurrentHashMap<>();
  private Set<InternalDistributedMember> shutdownMembers;
  private Set<InternalDistributedMember> crashedMembers;
  private InternalDistributedMember creator;
  private Set<InternalDistributedMember> hashedMembers;
  private Map<NetMember, InternalDistributedMember> netMemberToDistributedMember = new HashMap<>();
  private final Object membersLock = new Object();


  public static Set<InternalDistributedMember> netMemberSetToInternalDistributedMemberSet(
      Set<NetMember> netMembers) {
    if (netMembers.size() == 0) {
      return Collections.emptySet();
    } else if (netMembers.size() == 1) {
      return Collections.singleton(new InternalDistributedMember(netMembers.iterator().next()));
    } else {
      Set<InternalDistributedMember> idmMembers = new HashSet<>(netMembers.size());
      for (NetMember member : netMembers) {
        idmMembers.add(new InternalDistributedMember(member));
      }
      return idmMembers;
    }
  }

  public static List<InternalDistributedMember> netMemberListToInternalDistributedMemberSet(
      List<NetMember> netMembers) {
    if (netMembers.size() == 0) {
      return Collections.emptyList();
    } else if (netMembers.size() == 1) {
      return Collections.singletonList(new InternalDistributedMember(netMembers.iterator().next()));
    } else {
      List<InternalDistributedMember> idmMembers = new ArrayList<>(netMembers.size());
      for (NetMember member : netMembers) {
        idmMembers.add(new InternalDistributedMember(member));
      }
      return idmMembers;
    }
  }

  public MembershipView() {
    viewId = 0;
    members = new ArrayList<>(4);
    this.hashedMembers = new HashSet<>(members);
    shutdownMembers = Collections.emptySet();
    crashedMembers = new HashSet<>();
    populateReverseMap();
    creator = null;
  }

  public MembershipView(InternalDistributedMember creator) {
    viewId = 0;
    members = new ArrayList<>(4);
    members.add(creator);
    hashedMembers = new HashSet<>(members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    populateReverseMap();
    this.creator = creator;
  }

  public MembershipView(InternalDistributedMember creator, int viewId,
      List<InternalDistributedMember> members) {
    this.viewId = viewId;
    this.members = new ArrayList<>(members);
    hashedMembers = new HashSet<>(this.members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    populateReverseMap();
    this.creator = creator;
  }

  /**
   * Test method
   *
   * @param size size of the view, used for presizing collections
   * @param viewId the ID of the view
   */
  public MembershipView(int size, long viewId) {
    this.viewId = (int) viewId;
    members = new ArrayList<>(size);
    this.hashedMembers = new HashSet<>();
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    creator = null;
  }

  /**
   * Create a new view with the contents of the given view and the specified view ID
   */
  public MembershipView(MembershipView other, int viewId) {
    this.creator = other.creator;
    this.viewId = viewId;
    this.members = new ArrayList<>(other.members);
    this.hashedMembers = new HashSet<>(other.members);
    this.shutdownMembers = new HashSet<>(other.shutdownMembers);
    this.crashedMembers = new HashSet<>(other.crashedMembers);
    populateReverseMap();
    this.publicKeys.putAll(other.publicKeys);
  }

  public MembershipView(InternalDistributedMember creator, int viewId,
      List<InternalDistributedMember> mbrs, Set<InternalDistributedMember> shutdowns,
      Set<InternalDistributedMember> crashes) {
    this.creator = creator;
    this.viewId = viewId;
    this.members = mbrs;
    this.hashedMembers = new HashSet<>(mbrs);
    this.shutdownMembers = shutdowns;
    populateReverseMap();
    this.crashedMembers = crashes;
  }

  public MembershipView(NetView view) {
    this(view.getCreator(), view.getViewId(), view.getNetMembers(), view.getShutdownNetMembers(),
        view.getCrashedNetMembers());
  }

  public MembershipView(NetMember netCreator, int viewId, List<?> netMembers,
      Set<?> netShutdowns, Set<?> netCrashes) {
    this.creator = new InternalDistributedMember(netCreator);
    this.viewId = viewId;
    this.members = new ArrayList<>(netMembers.size());
    for (NetMember member : (List<NetMember>) netMembers) {
      this.members.add(new InternalDistributedMember(member));
    }
    this.hashedMembers = new HashSet<>(this.members);
    this.shutdownMembers =
        netMemberSetToInternalDistributedMemberSet((Set<NetMember>) netShutdowns);
    this.crashedMembers = netMemberSetToInternalDistributedMemberSet((Set<NetMember>) netCrashes);
    populateReverseMap();
  }


  private void populateReverseMap() {
    for (InternalDistributedMember member : members) {
      netMemberToDistributedMember.put(member.getNetMember(), member);
    }
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

  public Object getPublicKey(InternalDistributedMember mbr) {
    return publicKeys.get(mbr);
  }

  public void setPublicKey(InternalDistributedMember mbr, Object key) {
    if (mbr != null && key != null) {
      publicKeys.put(mbr, key);
    }
  }

  public void setPublicKeys(MembershipView otherView) {
    if (otherView.publicKeys != null) {
      this.publicKeys.putAll(otherView.publicKeys);
    }
  }

  public void setViewId(int viewId) {
    this.viewId = viewId;
  }



  public List<InternalDistributedMember> getMembers() {
    return Collections.unmodifiableList(this.members);
  }

  /**
   * return members that are i this view but not the given old view
   */
  public List<InternalDistributedMember> getNewMembers(MembershipView olderView) {
    List<InternalDistributedMember> result = new ArrayList<>(members);
    result.removeAll(olderView.getMembers());
    return result;
  }

  /**
   * return members added in this view
   */
  public List<InternalDistributedMember> getNewMembers() {
    List<InternalDistributedMember> result = new ArrayList<>(5);
    result.addAll(this.members.stream().filter(mbr -> mbr.getVmViewId() == this.viewId)
        .collect(Collectors.toList()));
    return result;
  }

  public Object get(int i) {
    return this.members.get(i);
  }

  public void add(InternalDistributedMember mbr) {
    this.hashedMembers.add(mbr);
    this.members.add(mbr);
  }

  public void addCrashedMembers(Set<InternalDistributedMember> mbr) {
    this.crashedMembers.addAll(mbr);
  }

  public boolean remove(InternalDistributedMember mbr) {
    this.hashedMembers.remove(mbr);
    return this.members.remove(mbr);
  }

  public void removeAll(Collection<InternalDistributedMember> ids) {
    this.hashedMembers.removeAll(ids);
    ids.forEach(this::remove);
  }

  public boolean contains(DistributedMember mbr) {
    assert mbr instanceof InternalDistributedMember;
    return this.hashedMembers.contains(mbr);
  }

  public int size() {
    return this.members.size();
  }

  public InternalDistributedMember getLeadMember() {
    for (InternalDistributedMember mbr : this.members) {
      if (mbr.getVmKind() == ClusterDistributionManager.NORMAL_DM_TYPE) {
        return mbr;
      }
    }
    return null;
  }

  public InternalDistributedMember getCoordinator() {
    synchronized (membersLock) {
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
   * Returns the coordinator of this view, rejecting any in the given collection of IDs
   */
  public InternalDistributedMember getCoordinator(
      Collection<InternalDistributedMember> rejections) {
    if (rejections == null) {
      return getCoordinator();
    }
    synchronized (membersLock) {
      for (InternalDistributedMember addr : members) {
        if (addr.getNetMember().preferredForCoordinator() && !rejections.contains(addr)) {
          return addr;
        }
      }
      for (InternalDistributedMember addr : members) {
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
  public List<InternalDistributedMember> getPreferredCoordinators(
      Set<InternalDistributedMember> filter, InternalDistributedMember localAddress,
      int maxNumberDesired) {
    List<InternalDistributedMember> results = new ArrayList<>();
    List<InternalDistributedMember> notPreferredCoordinatorList = new ArrayList<>();

    synchronized (membersLock) {
      for (InternalDistributedMember addr : members) {
        if (addr.equals(localAddress)) {
          continue;// this is must to add
        }
        if (addr.getNetMember().preferredForCoordinator() && !filter.contains(addr)) {
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
        Iterator<InternalDistributedMember> it = notPreferredCoordinatorList.iterator();
        while (it.hasNext() && results.size() < maxNumberDesired) {
          results.add(it.next());
        }
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
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          result += 10;
          if (lead != null && mbr.equals(lead)) {
            result += 5;
          }
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          result += 3;
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
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
  public int getCrashedMemberWeight(MembershipView oldView) {
    int result = 0;
    InternalDistributedMember lead = oldView.getLeadMember();
    for (InternalDistributedMember mbr : this.crashedMembers) {
      if (!oldView.contains(mbr)) {
        continue;
      }
      result += mbr.getNetMember().getMemberWeight();
      switch (mbr.getVmKind()) {
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          result += 10;
          if (lead != null && mbr.equals(lead)) {
            result += 5;
          }
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          result += 3;
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
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
  public Set<InternalDistributedMember> getActualCrashedMembers(MembershipView oldView) {
    Set<InternalDistributedMember> result = new HashSet<>(this.crashedMembers.size());
    result.addAll(this.crashedMembers.stream()
        .filter(mbr -> (mbr.getVmKind() != ClusterDistributionManager.ADMIN_ONLY_DM_TYPE))
        .filter(mbr -> oldView == null || oldView.contains(mbr)).collect(Collectors.toList()));
    return result;
  }

  /**
   * logs the weight of failed members wrt the given previous view
   */
  public void logCrashedMemberWeights(MembershipView oldView, Logger log) {
    InternalDistributedMember lead = oldView.getLeadMember();
    for (InternalDistributedMember mbr : this.crashedMembers) {
      if (!oldView.contains(mbr)) {
        continue;
      }
      int mbrWeight = mbr.getNetMember().getMemberWeight();
      switch (mbr.getVmKind()) {
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          if (lead != null && mbr.equals(lead)) {
            mbrWeight += 15;
          } else {
            mbrWeight += 10;
          }
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          mbrWeight += 3;
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
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
    sb.append("]");
    return sb.toString();
  }

  @Override
  public synchronized boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof MembershipView) {
      return this.members.equals(((MembershipView) other).getMembers());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return this.members.hashCode();
  }

  /**
   * reverse lookup of InternalDistributedMember given its netMember
   */
  public InternalDistributedMember getMember(GMSMember netMember) {
    return netMemberToDistributedMember.get(netMember);
  }
}
