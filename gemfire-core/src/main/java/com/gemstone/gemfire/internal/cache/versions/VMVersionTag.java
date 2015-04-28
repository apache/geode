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

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

/**
 * The version tag class for version tags for non-persistent regions. The
 * VersionSource held in these tags is an InternalDistributedMember.
 * 
 * @author dsmith
 * 
 */
public class VMVersionTag extends VersionTag<InternalDistributedMember> {
  
  public VMVersionTag() {
    super();
  }

  @Override
  public void writeMember(InternalDistributedMember member, DataOutput out) throws IOException {
    member.writeEssentialData(out);
    
  }
  
  @Override
  public void setCanonicalIDs(DM dm) {
    InternalDistributedMember id = getMemberID();
    if (id != null) {
      setMemberID(dm.getCanonicalId(getMemberID()));
      id = getPreviousMemberID();
      if (id != null) {
        setPreviousMemberID(dm.getCanonicalId(id));
      }
    }
  }

  @Override
  public InternalDistributedMember readMember(DataInput in) throws IOException, ClassNotFoundException {
    return InternalDistributedMember.readEssentialData(in);
  }

  @Override
  public int getDSFID() {
    return VERSION_TAG;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }


}
