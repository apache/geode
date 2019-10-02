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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a given collection of managers and then awaits replies. It is used by
 * some tests to flush the unordered communication channels after no-ack tests.
 * <p>
 * On the receiving end, the message will wait for the high priority queue to drain before sending a
 * reply. This guarantees that all high priority messages have been received and applied to the
 * cache. Their reply messages may not necessarily have been sent back or processed (if they have
 * any).
 *
 * @since GemFire 5.1
 */
public class HighPriorityAckedMessage extends HighPriorityDistributionMessage
    implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();

  /** The is of the distribution manager that sent the message */
  private InternalDistributedMember id;
  private int processorId;
  private operationType op;

  static enum operationType {
    DRAIN_POOL, DUMP_STACK
  };

  transient ClusterDistributionManager originDm;
  private transient ReplyProcessor21 rp;
  private boolean useNative;

  public HighPriorityAckedMessage() {
    super();
    InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds != null) {
      this.originDm = (ClusterDistributionManager) ds.getDistributionManager();
    }
    if (this.originDm != null) {
      this.id = this.originDm.getDistributionManagerId();
    }
  }

  /**
   * Request stack dumps. This does not wait for responses. If useNative is true we attempt to use
   * OSProcess native code and null is returned. Otherwise we use a thread mx bean to generate the
   * traces. If returnStacks is true the stacks are not logged but are returned in a map in gzipped
   * form.
   */
  public Map<InternalDistributedMember, byte[]> dumpStacks(Set recipients,
      @SuppressWarnings("hiding") boolean useNative, boolean returnStacks) {
    this.op = operationType.DUMP_STACK;
    this.useNative = useNative;
    Set recips = new HashSet(recipients);
    DistributedMember me = originDm.getDistributionManagerId();
    if (recips.contains(me)) {
      recips.remove(me);
    }
    CollectingReplyProcessor<byte[]> cp = null;
    if (returnStacks) {
      cp = new CollectingReplyProcessor<byte[]>(originDm, recips);
      this.processorId = cp.getProcessorId();
    }
    originDm.putOutgoing(this);
    if (cp != null) {
      try {
        cp.waitForReplies();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return cp.getResults();
    }
    return null;
  }

  /**
   * send the message and wait for replies
   *
   * @param recipients the destination manager ids
   * @param multicast whether to use multicast or unicast
   * @throws InterruptedException if the operation is interrupted (as by shutdown)
   * @throws ReplyException if an exception was sent back by another manager
   */
  public void send(Set recipients, boolean multicast) throws InterruptedException, ReplyException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    this.op = operationType.DRAIN_POOL;
    Set recips = new HashSet(recipients);
    DistributedMember me = originDm.getDistributionManagerId();
    if (recips.contains(me)) {
      recips.remove(me);
    }
    rp = new ReplyProcessor21(originDm, recips);
    processorId = rp.getProcessorId();
    setRecipients(recips);
    setMulticast(multicast);
    originDm.putOutgoing(this);

    rp.waitForReplies();
  }


  /**
   * Sets the id of the distribution manager that is shutting down
   */
  void setDistributionManagerId(InternalDistributedMember id) {
    this.id = id;
  }

  /** set the reply processor id that's used to wait for acknowledgements */
  public void setProcessorId(int pid) {
    processorId = pid;
  }

  /** return the reply processor id that's used to wait for acknowledgements */
  @Override
  public int getProcessorId() {
    return processorId;
  }

  /**
   * Adds the distribution manager that is started up to the current DM's list of members.
   *
   * This method is invoked on the receiver side
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    switch (this.op) {
      case DRAIN_POOL:
        Assert.assertTrue(this.id != null);
        // wait 10 seconds for the high priority queue to drain
        long endTime = System.currentTimeMillis() + 10000;
        ThreadPoolExecutor pool =
            (ThreadPoolExecutor) dm.getExecutors().getHighPriorityThreadPool();
        while (pool.getActiveCount() > 1 && System.currentTimeMillis() < endTime) {
          boolean interrupted = Thread.interrupted();
          try {
            Thread.sleep(500);
          } catch (InterruptedException ie) {
            interrupted = true;
            dm.getCancelCriterion().checkCancelInProgress(ie);
            // if interrupted, we must be shutting down
            return;
          } finally {
            if (interrupted)
              Thread.currentThread().interrupt();
          }
        }
        if (pool.getActiveCount() > 1) {

          logger.warn(
              "{}: There are still {} other threads active in the high priority thread pool.",
              new Object[] {this, Integer.valueOf(pool.getActiveCount() - 1)});
        }
        ReplyMessage.send(getSender(), processorId, null, dm);
        break;
      case DUMP_STACK:
        if (this.processorId > 0) {
          try {
            byte[] zippedStacks = OSProcess.zipStacks();
            ReplyMessage.send(getSender(), processorId, zippedStacks, dm);
          } catch (IOException e) {
            ReplyMessage.send(getSender(), processorId, new ReplyException(e), dm);
          }
        } else {
          OSProcess.printStacks(0, this.useNative);
        }
    }
  }

  @Override
  public int getDSFID() {
    return HIGH_PRIORITY_ACKED_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(processorId);
    out.writeInt(this.op.ordinal());
    out.writeBoolean(this.useNative);
    DataSerializer.writeObject(this.id, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {

    super.fromData(in, context);
    processorId = in.readInt();
    this.op = operationType.values()[in.readInt()];
    this.useNative = in.readBoolean();
    this.id = (InternalDistributedMember) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "<HighPriorityAckedMessage from=" + this.id + ";processorId=" + this.processorId + ">";
  }

}
