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
package com.gemstone.gemfire.admin.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.admin.remote.AdminResponse;

/**
 * The reply for a {@link FinishBackupRequest}. The
 * reply contains the persistent ids of the disk stores
 * that were backed up on this member.
 * 
 *
 */
public class FinishBackupResponse extends AdminResponse {
  
  private HashSet<PersistentID> persistentIds;
  
  public FinishBackupResponse() {
    super();
  }

  public FinishBackupResponse(InternalDistributedMember sender, HashSet<PersistentID> persistentIds) {
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

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public int getDSFID() {
    return FINISH_BACKUP_RESPONSE;
  }
  
  @Override
  public String toString() {
    return getClass().getName() + ": " + persistentIds;
  }
}
