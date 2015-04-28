/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;

/**
 * The version tag class for version tags for persistent regions. The VersionSource
 * held in these tags is a disk store id.
 * @author dsmith
 *
 */
public class DiskVersionTag extends VersionTag<DiskStoreID> {

  public DiskVersionTag() {
  }
  
  @Override
  public void setMemberID(DiskStoreID memberID) {
    assert (memberID == null) || (memberID instanceof DiskStoreID);
    super.setMemberID(memberID);
  }
  
  /**
   * For a persistent version tag, the member id should not be null, because
   * we can't fill the member id in later. 
   */
  @Override
  public void replaceNullIDs(VersionSource memberID) {
    if(this.getMemberID() == null) {
      throw new AssertionError("Member id should not be null for persistent version tags");
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
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
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

}
