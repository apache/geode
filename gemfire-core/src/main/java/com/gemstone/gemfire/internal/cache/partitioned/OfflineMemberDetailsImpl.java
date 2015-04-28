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
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;

/**
 * @author dsmith
 *
 */
public class OfflineMemberDetailsImpl implements OfflineMemberDetails, Serializable, DataSerializable {
  private Set<PersistentMemberID>[] offlineMembers;
  
  //Used for DataSerializer
  public OfflineMemberDetailsImpl() {
    
  }
  
  public OfflineMemberDetailsImpl(Set<PersistentMemberID>[] offlineMembers) {
    this.offlineMembers = offlineMembers;
  }



  public Set<PersistentMemberID> getOfflineMembers(int bucketId) {
    return offlineMembers[bucketId];
  }



  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int offlineMembersLength = in.readInt();
    this.offlineMembers = new Set[offlineMembersLength];
    for(int i = 0; i < offlineMembersLength; i++) {
      int setSize = in.readInt();
      Set<PersistentMemberID> set= new HashSet<PersistentMemberID>(setSize);
      for(int j = 0; j < setSize; j++) {
        PersistentMemberID id = new PersistentMemberID();
        InternalDataSerializer.invokeFromData(id, in);
        set.add(id);
      }
      this.offlineMembers[i] = set;
    }
  }


  public void toData(DataOutput out) throws IOException {
    out.writeInt(offlineMembers.length);
    for(Set<PersistentMemberID> set : offlineMembers) {
      out.writeInt(set.size());
      for(PersistentMemberID id : set) {
        InternalDataSerializer.invokeToData(id, out);
      }
    }
  }
}
