/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.admin.remote.AdminResponse;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
//NOTE: This is copied from com/gemstone/gemfire/internal/admin/remote/CompactResponse.java
//and modified as per requirements. (original-author Dan Smith)
public class CompactResponse extends AdminResponse {
  private PersistentID persistentId;
  
  public CompactResponse() {
  }
  
  public CompactResponse(InternalDistributedMember sender, PersistentID persistentId) {
    this.setRecipient(sender);
    this.persistentId = persistentId;
  }
  
  public PersistentID getPersistentId() {
    return persistentId;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    persistentId = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);    
    DataSerializer.writeObject(persistentId, out);
  }
  
  public CompactResponse(InternalDistributedMember sender) {
    this.setRecipient(sender);
  }

  public int getDSFID() {
    return MGMT_COMPACT_RESPONSE;
  }
}
