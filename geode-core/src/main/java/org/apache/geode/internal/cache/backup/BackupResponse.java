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
package org.apache.geode.internal.cache.backup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The response to a {@link PrepareBackupRequest}, {@link AbortBackupRequest}, or
 * {@link FinishBackupRequest}.
 */
public class BackupResponse extends AdminResponse {

  private HashSet<PersistentID> persistentIds;

  public BackupResponse() {
    super();
  }

  BackupResponse(InternalDistributedMember sender, HashSet<PersistentID> persistentIds) {
    setRecipient(sender);
    this.persistentIds = persistentIds;
  }

  Set<PersistentID> getPersistentIds() {
    return persistentIds;
  }

  @Override
  public void fromData(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    persistentIds = DataSerializer.readHashSet(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeHashSet(persistentIds, out);
  }

  @Override
  public int getDSFID() {
    return BACKUP_RESPONSE;
  }

  @Override
  public String toString() {
    return getClass().getName() + ": " + persistentIds;
  }
}
