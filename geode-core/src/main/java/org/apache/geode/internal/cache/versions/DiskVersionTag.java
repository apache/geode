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
package org.apache.geode.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * The version tag class for version tags for persistent regions. The VersionSource held in these
 * tags is a disk store id.
 *
 */
public class DiskVersionTag extends VersionTag<DiskStoreID> {

  public DiskVersionTag() {}

  @Override
  public void setMemberID(DiskStoreID memberID) {
    assert (memberID == null) || (memberID instanceof DiskStoreID);
    super.setMemberID(memberID);
  }

  /**
   * For a persistent version tag, the member id should not be null, because we can't fill the
   * member id in later.
   */
  @Override
  public void replaceNullIDs(VersionSource memberID) {
    if (getMemberID() == null) {
      throw new AssertionError("Member id should not be null for persistent version tags");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return DataSerializableFixedID.PERSISTENT_VERSION_TAG;
  }

  @Override
  public void writeMember(DiskStoreID member, DataOutput out) throws IOException {
    out.writeLong(member.getMostSignificantBits());
    out.writeLong(member.getLeastSignificantBits());

  }

  @Override
  public DiskStoreID readMember(DataInput in) throws IOException {
    long mostSignificantBits = in.readLong();
    long leastSignificantBits = in.readLong();
    DiskStoreID member = new DiskStoreID(mostSignificantBits, leastSignificantBits);
    return member;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

}
