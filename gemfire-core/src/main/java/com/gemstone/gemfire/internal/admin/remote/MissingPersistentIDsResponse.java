/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;

/**
 * The MissingPersistentIdResonse we return 662 peers. This response
 * includes this list of ids that we have locally.
 * @author dsmith
 *
 */
public class MissingPersistentIDsResponse extends AdminResponse {

  private Set<PersistentID> missingIds;
  private Set<PersistentID> localIds;
  
  public MissingPersistentIDsResponse() {
  }

  public MissingPersistentIDsResponse(Set<PersistentID> missingIds,
      Set<PersistentID> localIds, InternalDistributedMember recipient) {
    this.missingIds = missingIds;
    this.localIds = localIds;
    this.setRecipient(recipient);
  }

  public int getDSFID() {
    return MISSING_PERSISTENT_IDS_RESPONSE;
  }
  
  @Override
  protected void process(DistributionManager dm) {
    super.process(dm);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    int size = in.readInt();
    missingIds = new HashSet<PersistentID>(size);
    for(int i =0; i < size; i++) {
      PersistentMemberPattern pattern = new PersistentMemberPattern();
      InternalDataSerializer.invokeFromData(pattern,in);
      missingIds.add(pattern);
    }
    size = in.readInt();
    localIds = new HashSet<PersistentID>(size);
    for(int i =0; i < size; i++) {
      PersistentMemberPattern pattern = new PersistentMemberPattern();
      InternalDataSerializer.invokeFromData(pattern,in);
      localIds.add(pattern);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(missingIds.size());
    for(PersistentID pattern : missingIds) {
      InternalDataSerializer.invokeToData(pattern, out);
    }
    out.writeInt(localIds.size());
    for(PersistentID pattern : localIds) {
      InternalDataSerializer.invokeToData(pattern,out);
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
    return getClass().getName() + ": missing=" + missingIds + "local="
        + localIds;
  }
  
  
}
