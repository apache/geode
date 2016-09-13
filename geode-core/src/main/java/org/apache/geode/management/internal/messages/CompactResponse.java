/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * 
 * @since GemFire 7.0
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
