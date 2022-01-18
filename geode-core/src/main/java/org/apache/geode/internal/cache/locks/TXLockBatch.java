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

package org.apache.geode.internal.cache.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.geode.distributed.internal.locks.DLockBatch;
import org.apache.geode.distributed.internal.locks.DLockBatchId;
import org.apache.geode.distributed.internal.locks.LockGrantorId;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.IdentityArrayList;
import org.apache.geode.internal.cache.TXRegionLockRequestImpl;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Adapts multiple TXRegionLockRequests to one DLockBatch for DLock to use.
 *
 */
public class TXLockBatch implements DLockBatch, DataSerializableFixedID {

  /** Identifies the batch as a single entity */
  private TXLockIdImpl txLockId;

  /** List of <code>TXRegionLockRequests</code> */
  private List reqs;

  /** Identifies the members participating in the transaction */
  private Set participants;

  /**
   * Constructs a <code>TXLockBatch</code> for the list of <code>TXRegionLockRequests</code>
   */
  public TXLockBatch(TXLockId txLockId, List reqs, Set participants) {
    this.txLockId = (TXLockIdImpl) txLockId;
    this.reqs = reqs;
    this.participants = participants;
  }

  @Override
  public InternalDistributedMember getOwner() {
    return txLockId.getMemberId();
  }

  public TXLockId getTXLockId() {
    return txLockId;
  }

  @Override
  public DLockBatchId getBatchId() {
    return txLockId;
  }

  public void setParticipants(Set participants) {
    this.participants = participants;
  }

  @Override
  public void grantedBy(LockGrantorId lockGrantorId) {
    txLockId.setLockGrantorId(lockGrantorId);
  }

  @Override
  public List getReqs() {
    if (reqs != null && !(reqs instanceof IdentityArrayList)) {
      reqs = new IdentityArrayList(reqs);
    }
    return reqs;
  }

  @Override
  public String toString() {
    return "[TXLockBatch: txLockId=" + txLockId + "; reqs=" + reqs + "; participants="
        + participants + "]";
  }

  /**
   * Each lock batch contains a set of distributed system member ids that are participating in the
   * transaction. Public access for testing purposes.
   *
   * @return participants in the transaction
   */
  public Set getParticipants() {
    return participants;
  }

  // -------------------------------------------------------------------------
  // DataSerializable support
  // -------------------------------------------------------------------------

  public TXLockBatch() {}

  @Override
  public int getDSFID() {
    return TX_LOCK_BATCH;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    txLockId = TXLockIdImpl.createFromData(in);
    participants = InternalDataSerializer.readSet(in);
    {
      int reqsSize = in.readInt();
      if (reqsSize >= 0) {
        reqs = new IdentityArrayList(reqsSize);
        for (int i = 0; i < reqsSize; i++) {
          reqs.add(TXRegionLockRequestImpl.createFromData(in));
        }
      }
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    InternalDataSerializer.invokeToData(txLockId, out);
    InternalDataSerializer.writeSet(participants, out);
    if (reqs == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(reqs.size());
      for (final Object req : reqs) {
        TXRegionLockRequestImpl elem = (TXRegionLockRequestImpl) req;
        InternalDataSerializer.invokeToData(elem, out);
      }
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

}
