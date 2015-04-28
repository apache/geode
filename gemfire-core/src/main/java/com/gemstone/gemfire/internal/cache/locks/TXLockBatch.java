/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.locks;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
//import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.TXRegionLockRequestImpl;
import com.gemstone.gemfire.internal.cache.IdentityArrayList;
import com.gemstone.gemfire.distributed.internal.locks.DLockBatch;
import com.gemstone.gemfire.distributed.internal.locks.DLockBatchId;
import com.gemstone.gemfire.distributed.internal.locks.LockGrantorId;

import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/** 
 * Adapts multiple TXRegionLockRequests to one DLockBatch for DLock to use.
 *
 * @author Kirk Lund 
 */
public final class TXLockBatch implements DLockBatch, DataSerializableFixedID {
  
  /** Identifies the batch as a single entity */
  private TXLockIdImpl txLockId;
  
  /** List of <code>TXRegionLockRequests</code> */
  private List reqs;
  
  /** Identifies the members participating in the transaction */
  private Set participants;
  
  /** 
   * Constructs a <code>TXLockBatch</code> for the list of 
   * <code>TXRegionLockRequests</code> 
   */
  public TXLockBatch(TXLockId txLockId, List reqs, Set participants) {
    this.txLockId = (TXLockIdImpl)txLockId;
    this.reqs = reqs;
    this.participants = participants;
  }
  
  public InternalDistributedMember getOwner() {
    return this.txLockId.getMemberId();
  }
  
  public TXLockId getTXLockId() {
    return this.txLockId;
  }
  
  public DLockBatchId getBatchId() {
    return this.txLockId;
  }

  public void setParticipants(Set participants) {
    this.participants = participants;
  }
  
  public void grantedBy(LockGrantorId lockGrantorId) {
    this.txLockId.setLockGrantorId(lockGrantorId);
  }

  public List getReqs() {
    if (this.reqs != null && !(this.reqs instanceof IdentityArrayList)) {
      this.reqs = new IdentityArrayList(this.reqs);
    }
    return this.reqs;
  }
  
  @Override
  public String toString() {
    return "[TXLockBatch: txLockId=" + txLockId + 
           "; reqs=" + reqs + "; participants=" + participants + "]";
  }

  /**
   *  Each lock batch contains a set of distributed system member ids
   *  that are participating in the transaction.  Public access for testing purposes.
   *  @return participants in the transaction 
   */
  public Set getParticipants() {
    return this.participants;
  }
  
  // -------------------------------------------------------------------------
  //   DataSerializable support
  // -------------------------------------------------------------------------
  
  public TXLockBatch() {}
  
  public int getDSFID() {
    return TX_LOCK_BATCH;
  }

  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.txLockId = TXLockIdImpl.createFromData(in);
    this.participants = InternalDataSerializer.readSet(in);
    {
      int reqsSize = in.readInt();
      if (reqsSize >= 0) {
        this.reqs = new IdentityArrayList(reqsSize);
        for (int i = 0; i < reqsSize; i++) {
          this.reqs.add(TXRegionLockRequestImpl.createFromData(in));
        }
      }
    }
  }
  
  public void toData(DataOutput out) throws IOException {
    InternalDataSerializer.invokeToData(this.txLockId, out);
    InternalDataSerializer.writeSet(this.participants, out);
    if (this.reqs == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(this.reqs.size());
      for (Iterator iter = this.reqs.iterator(); iter.hasNext(); ) {
        TXRegionLockRequestImpl elem = (TXRegionLockRequestImpl)iter.next();
        InternalDataSerializer.invokeToData(elem, out);
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
  
}

