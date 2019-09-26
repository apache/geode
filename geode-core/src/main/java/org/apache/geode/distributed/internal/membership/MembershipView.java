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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.UnmodifiableException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;

/**
 * The MembershipView class represents a membership view. Note that this class is not synchronized,
 * so take that under advisement if you decide to modify a view with add() or remove().
 */
public class MembershipView {

  private int viewId;
  private List<InternalDistributedMember> members;
  private Set<InternalDistributedMember> shutdownMembers;
  private Set<InternalDistributedMember> crashedMembers;
  private InternalDistributedMember creator;
  private Set<InternalDistributedMember> hashedMembers;
  private volatile boolean unmodifiable;


  public MembershipView() {
    viewId = -1;
    members = new ArrayList<>(0);
    this.hashedMembers = new HashSet<>(members);
    shutdownMembers = Collections.emptySet();
    crashedMembers = new HashSet<>();
    creator = null;
  }

  public MembershipView(InternalDistributedMember creator, int viewId,
      List<InternalDistributedMember> members) {
    this.viewId = viewId;
    this.members = new ArrayList<>(members);
    hashedMembers = new HashSet<>(this.members);
    shutdownMembers = new HashSet<>();
    crashedMembers = Collections.emptySet();
    this.creator = creator;
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
  }

  public MembershipView(InternalDistributedMember creator, int viewId,
      List<InternalDistributedMember> mbrs, Set<InternalDistributedMember> shutdowns,
      Set<InternalDistributedMember> crashes) {
    this.creator = creator;
    this.viewId = viewId;
    this.members = mbrs;
    this.hashedMembers = new HashSet<>(mbrs);
    this.shutdownMembers = shutdowns;
    this.crashedMembers = crashes;
  }

  public void makeUnmodifiable() {
    unmodifiable = true;
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

  public Object get(int i) {
    return this.members.get(i);
  }

  public void add(InternalDistributedMember mbr) {
    if (unmodifiable) {
      throw new UnmodifiableException("this membership view is not modifiable");
    }
    this.hashedMembers.add(mbr);
    this.members.add(mbr);
  }

  public boolean remove(InternalDistributedMember mbr) {
    if (unmodifiable) {
      throw new UnmodifiableException("this membership view is not modifiable");
    }
    this.hashedMembers.remove(mbr);
    return this.members.remove(mbr);
  }

  public void removeAll(Collection<InternalDistributedMember> ids) {
    if (unmodifiable) {
      throw new UnmodifiableException("this membership view is not modifiable");
    }
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


  /**
   * Returns the ID from this view that is equal to the argument. If no such ID exists the argument
   * is returned.
   */
  public synchronized InternalDistributedMember getCanonicalID(InternalDistributedMember id) {
    if (hashedMembers.contains(id)) {
      for (InternalDistributedMember m : this.members) {
        if (id.equals(m)) {
          return m;
        }
      }
    }
    return id;
  }



  public InternalDistributedMember getCoordinator() {
    for (InternalDistributedMember addr : members) {
      if (addr.getNetMember().preferredForCoordinator()) {
        return addr;
      }
    }
    if (members.size() > 0) {
      return members.get(0);
    }
    return null;
  }

  public Set<InternalDistributedMember> getShutdownMembers() {
    return this.shutdownMembers;
  }

  public Set<InternalDistributedMember> getCrashedMembers() {
    return this.crashedMembers;
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

}
