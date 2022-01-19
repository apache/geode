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

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.GemFireMemberStatus;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular distribution manager to get its current
 * {@link org.apache.geode.admin.GemFireMemberStatus}.
 */
public class RefreshMemberSnapshotResponse extends AdminResponse {

  GemFireMemberStatus snapshot;

  /**
   * Returns a {@code FetchSysCfgResponse} that will be returned to the specified recipient. The
   * message will contains a copy of the local manager's config.
   */
  public static RefreshMemberSnapshotResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    RefreshMemberSnapshotResponse m = new RefreshMemberSnapshotResponse();
    m.setRecipient(recipient);

    try {
      DistributedSystem sys = dm.getSystem();
      InternalCache c = (InternalCache) CacheFactory.getInstance(sys);
      m.snapshot = new GemFireMemberStatus(c);
    } catch (Exception ignore) {
      m.snapshot = null;
    }
    return m;
  }

  /**
   * @return return the snapshot of Gemfire member vm
   */
  public GemFireMemberStatus getSnapshot() {
    return snapshot;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(snapshot, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    snapshot = DataSerializer.readObject(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  @Override
  public int getDSFID() {
    return REFRESH_MEMBER_SNAP_RESPONSE;
  }

  @Override
  public String toString() {
    return "RefreshMemberSnapshotResponse from " + getRecipient() + " snapshot="
        + snapshot;
  }
}
