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
package org.apache.geode.management.internal.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 *
 *
 * @since GemFire 7.0
 */
// NOTE: This is copied from org/apache/geode/internal/admin/remote/CompactResponse.java
// and modified as per requirements. (original-author Dan Smith)
public class CompactResponse extends AdminResponse {
  private PersistentID persistentId;

  public CompactResponse() {}

  public CompactResponse(InternalDistributedMember sender, PersistentID persistentId) {
    setRecipient(sender);
    this.persistentId = persistentId;
  }

  public PersistentID getPersistentId() {
    return persistentId;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    persistentId = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(persistentId, out);
  }

  public CompactResponse(InternalDistributedMember sender) {
    setRecipient(sender);
  }

  @Override
  public int getDSFID() {
    return MGMT_COMPACT_RESPONSE;
  }
}
