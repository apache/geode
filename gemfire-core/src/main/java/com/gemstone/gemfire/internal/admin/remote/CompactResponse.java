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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * @author dsmith
 *
 */
public class CompactResponse extends AdminResponse {
  private HashSet<PersistentID> persistentIds;
  
  public CompactResponse() {
  }
  
  public CompactResponse(InternalDistributedMember sender, HashSet<PersistentID> persistentIds) {
    this.setRecipient(sender);
    this.persistentIds = persistentIds;
  }
  
  public HashSet<PersistentID> getPersistentIds() {
    return persistentIds;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    persistentIds = DataSerializer.readHashSet(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);    
    DataSerializer.writeHashSet(persistentIds, out);
  }
  
  public CompactResponse(InternalDistributedMember sender) {
    this.setRecipient(sender);
  }

  public int getDSFID() {
    return COMPACT_RESPONSE;
  }
}
