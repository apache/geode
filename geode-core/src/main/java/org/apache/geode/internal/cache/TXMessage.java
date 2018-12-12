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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.logging.LogService;

public abstract class TXMessage extends SerialDistributionMessage
    implements MessageWithReply, TransactionMessage {

  private static final Logger logger = LogService.getLogger();

  private int processorId;

  private int txUniqId;

  private InternalDistributedMember txMemberId = null;

  public TXMessage() {
    // nothing
  }

  public TXMessage(int txUniqueId, InternalDistributedMember onBehalfOfMember,
      ReplyProcessor21 processor) {
    this.txUniqId = txUniqueId;
    this.txMemberId = onBehalfOfMember;
    this.processorId = processor == null ? 0 : processor.getProcessorId();
  }

  public boolean canStartRemoteTransaction() {
    return false;
  }

  @Override
  protected void process(final ClusterDistributionManager dm) {
    Throwable thr = null;
    boolean sendReply = true;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("processing {}", this);
      }
      InternalCache cache = dm.getCache();
      if (checkCacheClosing(cache) || checkDSClosing(cache.getInternalDistributedSystem())) {
        if (cache == null) {
          thr = new CacheClosedException(String.format("Remote cache is closed: %s",
              dm.getId()));
        } else {
          thr = cache
              .getCacheClosedException(String.format("Remote cache is closed: %s",
                  dm.getId()));
        }
        return;
      }
      TXManagerImpl txMgr = cache.getTXMgr();
      TXStateProxy tx = null;
      try {
        assert this.txUniqId != TXManagerImpl.NOTX;
        TXId txId = new TXId(getMemberToMasqueradeAs(), this.txUniqId);
        tx = txMgr.masqueradeAs(this);
        sendReply = operateOnTx(txId, dm);
      } finally {
        txMgr.unmasquerade(tx);
      }
    } catch (CommitConflictException cce) {
      thr = cce;
    } catch (DistributedSystemDisconnectedException se) {
      sendReply = false;
      if (logger.isDebugEnabled()) {
        logger.debug("shutdown caught, abandoning message: " + se);
      }
    } catch (RegionDestroyedException rde) {
      thr = new ForceReattemptException(String.format("Region is destroyed in %s",
          dm.getDistributionManagerId()), rde);
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (sendReply) {
        thr = t;
      }
    } finally {
      ReplySender rs = getReplySender(dm);
      if (sendReply && (this.processorId != 0 || (rs != dm))) {
        ReplyException rex = null;
        if (thr != null) {
          rex = new ReplyException(thr);
        }
        sendReply(getSender(), this.processorId, dm, rex);
      }
    }
  }

  private boolean checkDSClosing(InternalDistributedSystem distributedSystem) {
    return distributedSystem == null || distributedSystem.isDisconnecting();
  }

  private boolean checkCacheClosing(InternalCache cache) {
    return cache == null || cache.isClosed();
  }

  private void sendReply(InternalDistributedMember recipient, int processorId2,
      ClusterDistributionManager dm, ReplyException rex) {
    ReplyMessage.send(recipient, processorId2, rex, getReplySender(dm));
  }

  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    String className = getClass().getName();
    // className.substring(className.lastIndexOf('.', className.lastIndexOf('.') - 1) + 1); //
    // partition.<foo> more generic version
    buff.append(className.substring(
        className.indexOf(PartitionMessage.PN_TOKEN) + PartitionMessage.PN_TOKEN.length())); // partition.<foo>
    buff.append("(txId=").append(this.txUniqId).append("; txMbr=").append(this.txMemberId)
        .append("; sender=").append(getSender()).append("; processorId=").append(this.processorId);
    appendFields(buff);
    buff.append(")");
    return buff.toString();
  }

  public void appendFields(StringBuffer buff) {}

  /**
   * Transaction operations override this method to do actual work
   *
   * @param txId The transaction Id to operate on
   * @return true if TXMessage should send a reply false otherwise
   */
  protected abstract boolean operateOnTx(TXId txId, ClusterDistributionManager dm)
      throws RemoteOperationException;

  public int getTXUniqId() {
    return this.txUniqId;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.processorId);
    out.writeInt(this.txUniqId);
    DataSerializer.writeObject(this.txMemberId, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.processorId = in.readInt();
    this.txUniqId = in.readInt();
    this.txMemberId = DataSerializer.readObject(in);
  }

  public InternalDistributedMember getMemberToMasqueradeAs() {
    if (txMemberId == null) {
      return getSender();
    }
    return txMemberId;
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  public InternalDistributedMember getTXOriginatorClient() {
    return this.txMemberId;
  }

  @Override
  public boolean canParticipateInTransaction() {
    return true;
  }

  @Override
  public boolean isTransactionDistributed() {
    return false;
  }

}
