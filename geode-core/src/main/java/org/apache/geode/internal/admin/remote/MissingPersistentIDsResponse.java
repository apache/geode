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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The MissingPersistentIdResonse we return 662 peers. This response includes this list of ids that
 * we have locally.
 *
 */
public class MissingPersistentIDsResponse extends AdminResponse {

  private Set<PersistentID> missingIds;
  private Set<PersistentID> localIds;

  public MissingPersistentIDsResponse() {}

  public MissingPersistentIDsResponse(Set<PersistentID> missingIds, Set<PersistentID> localIds,
      InternalDistributedMember recipient) {
    this.missingIds = missingIds;
    this.localIds = localIds;
    setRecipient(recipient);
  }

  @Override
  public int getDSFID() {
    return MISSING_PERSISTENT_IDS_RESPONSE;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    super.process(dm);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = in.readInt();
    missingIds = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      PersistentMemberPattern pattern = new PersistentMemberPattern();
      InternalDataSerializer.invokeFromData(pattern, in);
      missingIds.add(pattern);
    }
    size = in.readInt();
    localIds = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      PersistentMemberPattern pattern = new PersistentMemberPattern();
      InternalDataSerializer.invokeFromData(pattern, in);
      localIds.add(pattern);
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(missingIds.size());
    for (PersistentID pattern : missingIds) {
      InternalDataSerializer.invokeToData(pattern, out);
    }
    out.writeInt(localIds.size());
    for (PersistentID pattern : localIds) {
      InternalDataSerializer.invokeToData(pattern, out);
    }
  }

  public Set<PersistentID> getMissingIds() {
    return missingIds;
  }

  public Set<PersistentID> getLocalIds() {
    return localIds;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    // TODO Auto-generated method stub
    return super.clone();
  }

  @Override
  public String toString() {
    return getClass().getName() + ": missing=" + missingIds + "local=" + localIds;
  }


}
