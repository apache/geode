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
package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;

public class OfflineMemberDetailsImpl
    implements OfflineMemberDetails, Serializable, DataSerializable {
  private Set<PersistentMemberID>[] offlineMembers;

  // Used for DataSerializer
  public OfflineMemberDetailsImpl() {

  }

  public OfflineMemberDetailsImpl(Set<PersistentMemberID>[] offlineMembers) {
    this.offlineMembers = offlineMembers;
  }



  @Override
  public Set<PersistentMemberID> getOfflineMembers(int bucketId) {
    return offlineMembers[bucketId];
  }



  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int offlineMembersLength = in.readInt();
    offlineMembers = new Set[offlineMembersLength];
    for (int i = 0; i < offlineMembersLength; i++) {
      int setSize = in.readInt();
      Set<PersistentMemberID> set = new HashSet<PersistentMemberID>(setSize);
      for (int j = 0; j < setSize; j++) {
        PersistentMemberID id = new PersistentMemberID();
        InternalDataSerializer.invokeFromData(id, in);
        set.add(id);
      }
      offlineMembers[i] = set;
    }
  }


  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(offlineMembers.length);
    for (Set<PersistentMemberID> set : offlineMembers) {
      out.writeInt(set.size());
      for (PersistentMemberID id : set) {
        InternalDataSerializer.invokeToData(id, out);
      }
    }
  }
}
