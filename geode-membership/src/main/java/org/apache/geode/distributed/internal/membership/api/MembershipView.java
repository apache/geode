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
package org.apache.geode.distributed.internal.membership.api;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The MembershipView class represents a membership view. A MembershipView defines who is in the
 * cluster and knows which node created the view. It also knows which members left or were removed
 * when the view was created. MemberIdentifiers in the view are marked with the viewId of the
 * MembershipView in which they joined the cluster.
 */
public class MembershipView<ID extends MemberIdentifier> {

  private final int viewId;
  private final List<ID> members;
  private final Set<ID> shutdownMembers;
  private final Set<ID> crashedMembers;
  private final ID creator;
  private final Set<ID> hashedMembers;

  public MembershipView() {
    this(null, -1, Collections.emptyList());
  }

  public MembershipView(final ID creator, final int viewId, final List<ID> members) {
    this(creator, viewId, members, Collections.emptySet(), Collections.emptySet());
  }

  public MembershipView(final ID creator, final int viewId, final List<ID> members,
      final Set<ID> shutdowns, final Set<ID> crashes) {
    this.creator = creator;
    this.viewId = viewId;

    /*
     * Copy each collection, then store the ref to the unmodifiable
     * wrapper so we can expose it.
     */
    this.members = Collections.unmodifiableList(new ArrayList<>(members));
    this.shutdownMembers = Collections.unmodifiableSet(new HashSet<>(shutdowns));
    this.crashedMembers = Collections.unmodifiableSet(new HashSet<>(crashes));

    // make this unmodifiable for good measure (even though we don't expose it)
    this.hashedMembers = Collections.unmodifiableSet(new HashSet<>(members));
  }

  public int getViewId() {
    return this.viewId;
  }

  public ID getCreator() {
    return this.creator;
  }

  public List<ID> getMembers() {
    return members;
  }

  public Object get(int i) {
    return this.members.get(i);
  }

  public MembershipView<ID> createNewViewWithMember(ID member) {
    return new MembershipView<>(creator, viewId,
        Stream.concat(members.stream(), Stream.of(member)).collect(toList()), shutdownMembers,
        crashedMembers);
  }

  public MembershipView<ID> createNewViewWithoutMember(final ID member) {
    return new MembershipView<>(creator, viewId,
        members.stream().filter(m -> !m.equals(member)).collect(toList()), shutdownMembers,
        crashedMembers);
  }

  public boolean contains(MemberIdentifier mbr) {
    return this.hashedMembers.contains(mbr);
  }

  public int size() {
    return this.members.size();
  }

  public ID getLeadMember() {
    for (ID mbr : this.members) {
      if (mbr.getVmKind() == MemberIdentifier.NORMAL_DM_TYPE) {
        return mbr;
      }
    }
    return null;
  }


  /**
   * Returns the ID from this view that is equal to the argument. If no such ID exists the argument
   * is returned.
   */
  public ID getCanonicalID(ID id) {
    if (hashedMembers.contains(id)) {
      for (ID m : this.members) {
        if (id.equals(m)) {
          return m;
        }
      }
    }
    return id;
  }


  public ID getCoordinator() {
    for (ID addr : members) {
      if (addr.preferredForCoordinator()) {
        return addr;
      }
    }
    if (members.size() > 0) {
      return members.get(0);
    }
    return null;
  }

  public Set<ID> getCrashedMembers() {
    return this.crashedMembers;
  }

  public String toString() {
    ID lead = getLeadMember();

    StringBuilder sb = new StringBuilder(200);
    sb.append("View[").append(creator).append('|').append(viewId).append("] members: [");
    boolean first = true;
    for (ID mbr : this.members) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(mbr);
      if (mbr == lead) {
        sb.append("{lead}");
      }
      first = false;
    }
    if (!this.shutdownMembers.isEmpty()) {
      sb.append("]  shutdown: [");
      first = true;
      for (ID mbr : this.shutdownMembers) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(mbr);
        first = false;
      }
    }
    if (!this.crashedMembers.isEmpty()) {
      sb.append("]  crashed: [");
      first = true;
      for (ID mbr : this.crashedMembers) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(mbr);
        first = false;
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof MembershipView) {
      return this.members.equals(((MembershipView<ID>) other).getMembers());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.members.hashCode();
  }

}
