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

package com.gemstone.gemfire.internal.cache.locks;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.DataSerializer;
import java.io.*;

import com.gemstone.gemfire.distributed.internal.locks.LockGrantorId;
import com.gemstone.gemfire.distributed.internal.membership.*;

/** 
 * Identifies a group of transaction locks.
 *
 */
public final class TXLockIdImpl
  implements TXLockId, DataSerializableFixedID {

  private static final long serialVersionUID = 8579214625084490134L;
  /** DistributionManager id for this member */
  private InternalDistributedMember memberId;

  /** Increments for each txLockId that is generated in this vm */  
  private static int txCount = 0;
  
  /** Unique identifier within this member's vm */
  private int id;
  
  /** Identifies the lock grantor that granted the lock */
  private transient LockGrantorId grantedBy;
  
  /** Creates new instance of TXLockIdImpl */
  public TXLockIdImpl(InternalDistributedMember memberId) {
    this.memberId = memberId;
    synchronized(TXLockIdImpl.class) {
      this.id = txCount++;
    }
  }
  
  public final int getCount() {
    return this.id;
  }

  public final InternalDistributedMember getMemberId() {
    return this.memberId;
  }
  
  public void setLockGrantorId(LockGrantorId lockGrantorId) {
    this.grantedBy = lockGrantorId;
  }
  
  public LockGrantorId getLockGrantorId() {
    return this.grantedBy;
  }
 
  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result = mult * result + 
        (this.memberId == null ? 0 : this.memberId.hashCode());
    result = mult * result + this.id;

    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (other == null) return false;
    if (!(other instanceof TXLockIdImpl)) return false;
    final TXLockIdImpl that = (TXLockIdImpl)other;

    if (this.memberId != that.memberId && 
        !(this.memberId != null && this.memberId.equals(that.memberId))) {
      return false;
    }
    if (this.id != that.id) return false;

    return true;
  }

  @Override
  public String toString() {
    return "TXLockId: " + this.memberId + "-" + this.id;
  }

  // -------------------------------------------------------------------------
  //   DataSerializable support
  // -------------------------------------------------------------------------
  
  public TXLockIdImpl() {}
  
  public int getDSFID() {
    return TRANSACTION_LOCK_ID;
  }

  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.memberId = (InternalDistributedMember) DataSerializer.readObject(in);
    this.id = in.readInt();
  }
  
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.memberId, out);
    out.writeInt(this.id);
  }
  
  public static final TXLockIdImpl createFromData(DataInput in) 
  throws IOException, ClassNotFoundException {
    TXLockIdImpl result = new TXLockIdImpl();
    result.fromData(in);
    return result;
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}

