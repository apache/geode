/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;

/**
 * @author dsmith
 *
 */
public interface OfflineMemberDetails extends DataSerializable {
  
  public static final OfflineMemberDetails EMPTY_DETAILS = new OfflineMemberDetails() {
    public Set<PersistentMemberID> getOfflineMembers(int bucketId) {
      return Collections.emptySet();
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
    }

    public void toData(DataOutput out) throws IOException {
    }
  };
  
  public Set<PersistentMemberID> getOfflineMembers(int bucketId);
}