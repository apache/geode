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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ProcessorKeeper21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This class keeps track of how many InitialImageMessages are in flight between the initial image
 * provider and the image target.
 *
 * The provider is responsible for calling acquirePermit before sending an initial image chunk. The
 * acquire will block if two many messages are in flight.
 *
 * The initial image target sends FlowControlPermitMessage to the image provider after each
 * processed chunk. Upon receiving the FlowControlPermit message, the provider will increase the
 * number of permits available.
 *
 */
public class InitialImageFlowControl implements MembershipListener {
  private static final Logger logger = LogService.getLogger();

  @MakeNotStatic
  private static final ProcessorKeeper21 keeper = new ProcessorKeeper21(false);
  private int id;
  private int maxPermits = InitialImageOperation.CHUNK_PERMITS;
  private final Semaphore permits = new Semaphore(maxPermits);
  private final DistributionManager dm;
  private final InternalDistributedMember target;
  private final AtomicBoolean aborted = new AtomicBoolean();

  public static InitialImageFlowControl register(DistributionManager dm,
      InternalDistributedMember target) {
    InitialImageFlowControl control = new InitialImageFlowControl(dm, target);
    int id = keeper.put(control);
    control.id = id;

    List availableIds = dm.addMembershipListenerAndGetDistributionManagerIds(control);
    if (!availableIds.contains(target)) {
      control.abort();
    }
    return control;
  }

  private InitialImageFlowControl(DistributionManager dm, InternalDistributedMember target) {
    this.dm = dm;
    this.target = target;
  }

  private void releasePermit() {
    permits.release();
    incMessagesInFlight(-1);
  }

  private void incMessagesInFlight(int val) {
    dm.getStats().incInitialImageMessagesInFlight(val);
  }


  /**
   * Acquire a permit to send another message
   */
  public void acquirePermit() {
    long startWaitTime = System.currentTimeMillis();
    while (!aborted.get()) {
      checkCancellation();

      boolean interrupted = false;
      try {
        basicWait(startWaitTime);
        break;
      } catch (InterruptedException e) {
        interrupted = true; // keep looping
        checkCancellation();
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // while
    if (!aborted.get()) {
      incMessagesInFlight(1);
    }
  }

  private void basicWait(long startWaitTime) throws InterruptedException {
    long timeout = getAckWaitThreshold() * 1000L;
    long timeSoFar = System.currentTimeMillis() - startWaitTime;
    if (timeout <= 0) {
      timeout = Long.MAX_VALUE;
    }
    if (!aborted.get() && !permits.tryAcquire(timeout - timeSoFar - 1, TimeUnit.MILLISECONDS)) {
      checkCancellation();

      List activeMembers = dm.getDistributionManagerIds();
      logger.warn(
          "{} seconds have elapsed while waiting for replies: {} on {} whose current membership list is: [{}]",
          getAckWaitThreshold(), this, dm.getId(), activeMembers);

      permits.acquire();

      // Give an info message since timeout gave a warning.
      logger.info("{} wait for replies completed",
          "InitialImageFlowControl");
    }
  }

  /**
   * Return the time in sec to wait before sending an alert while waiting for ack replies. Note that
   * the ack wait threshold may change at runtime, so we have to consult the system every time.
   */
  private int getAckWaitThreshold() {
    return dm.getConfig().getAckWaitThreshold();
  }

  private void checkCancellation() {
    dm.getCancelCriterion().checkCancelInProgress(null);
  }

  public void unregister() {
    dm.removeMembershipListener(this);
    keeper.remove(id);
    abort();
  }

  public int getId() {
    return id;
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {
    if (id.equals(target)) {
      abort();
    }
  }

  private void abort() {
    if (!aborted.getAndSet(true)) {
      incMessagesInFlight(-(maxPermits - permits.availablePermits()));
      // Just in case java has issues with semaphores rolling over, set this
      // to half Integer.MAX_VALUE rather to release all of the waiters
      permits.release(Integer.MAX_VALUE / 2);
    }
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    // Do nothing
  }

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {
    // Do nothing
  }

  @Override
  public String toString() {
    return "<InitialImageFlowControl for GII to " + target + " with " + permits.availablePermits()
        + " available permits>";
  }


  public static class FlowControlPermitMessage extends DistributionMessage
      implements DataSerializableFixedID {
    private int keeperId;

    private FlowControlPermitMessage(int keeperId2) {
      this.keeperId = keeperId2;
    }

    public FlowControlPermitMessage() {}

    public static void send(DistributionManager dm, InternalDistributedMember recipient,
        int keeperId) {
      FlowControlPermitMessage message = new FlowControlPermitMessage(keeperId);
      message.setRecipient(recipient);
      dm.putOutgoing(message);
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.STANDARD_EXECUTOR;
    }

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      InitialImageFlowControl control = (InitialImageFlowControl) keeper.retrieve(keeperId);
      if (control != null) {
        control.releasePermit();
      }
    }

    @Override
    public int getDSFID() {
      return FLOW_CONTROL_PERMIT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      keeperId = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(keeperId);
    }



  }
}
