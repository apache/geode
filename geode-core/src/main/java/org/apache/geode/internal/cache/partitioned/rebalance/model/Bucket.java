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
package org.apache.geode.internal.cache.partitioned.rebalance.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.internal.cache.persistence.PersistentMemberID;

/**
 * Represents a single bucket.
 */
public class Bucket implements Comparable<Bucket> {
  private final int id;
  private final Set<Member> membersHosting = new TreeSet<>();
  private final Set<PersistentMemberID> offlineMembers;
  private float load;
  private long bytes;
  private float primaryLoad;
  private int redundancy = -1;
  private Member primary;

  public Bucket(int id) {
    this(id, 0, 0, new HashSet<>());
  }

  public Bucket(int id, float load, long bytes, Set<PersistentMemberID> offlineMembers) {
    this.id = id;
    this.load = load;
    this.bytes = bytes;
    this.offlineMembers = offlineMembers;
  }

  void changeLoad(float change) {
    load += change;
  }

  void changePrimaryLoad(float change) {
    primaryLoad += change;
  }

  void changeBytes(long change) {
    bytes += change;
  }

  void addOfflineMembers(Collection<? extends PersistentMemberID> members) {
    offlineMembers.addAll(members);
  }

  public void setPrimary(Member member, float primaryLoad) {
    if (this.primary == PartitionedRegionLoadModel.INVALID_MEMBER) {
      return;
    }
    if (this.primary != null) {
      this.primary.removePrimary(this);
    }
    this.primary = member;
    this.primaryLoad = primaryLoad;
    if (primary != PartitionedRegionLoadModel.INVALID_MEMBER && primary != null) {
      addMember(primary);
      member.addPrimary(this);
    }
  }

  public boolean addMember(Member targetMember) {
    if (this.getMembersHosting().add(targetMember)) {
      this.redundancy++;
      targetMember.addBucket(this);
      return true;
    }

    return false;
  }

  public boolean removeMember(Member targetMember) {
    if (this.getMembersHosting().remove(targetMember)) {
      if (targetMember == this.primary) {
        setPrimary(null, 0);
      }
      this.redundancy--;
      targetMember.removeBucket(this);
      return true;
    }
    return false;
  }

  public int getRedundancy() {
    return this.redundancy + offlineMembers.size();
  }

  public int getOnlineRedundancy() {
    return this.redundancy;
  }

  public float getLoad() {
    return this.load;
  }

  public int getId() {
    return this.id;
  }

  public long getBytes() {
    return this.bytes;
  }

  @Override
  public String toString() {
    return "Bucket(id=" + getId() + ",load=" + load + ")";
  }

  public float getPrimaryLoad() {
    return this.primaryLoad;
  }

  public Set<Member> getMembersHosting() {
    return this.membersHosting;
  }

  public Member getPrimary() {
    return this.primary;
  }

  public Collection<? extends PersistentMemberID> getOfflineMembers() {
    return offlineMembers;
  }

  @Override
  public int hashCode() {
    return this.id;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Bucket)) {
      return false;
    }
    Bucket o = (Bucket) other;
    return this.id == o.id;
  }

  @Override
  public int compareTo(Bucket other) {
    return Integer.compare(this.id, other.id);
  }
}
