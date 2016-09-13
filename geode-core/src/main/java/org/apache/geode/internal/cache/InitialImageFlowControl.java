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

import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.ProcessorKeeper21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.i18n.StringId;

/**
 * This class keeps track of how many InitialImageMessages are in flight between the
 * initial image provider and the image target.
 * 
 * The provider is responsible for calling acquirePermit before sending an initial image
 * chunk. The acquire will block if two many messages are in flight.
 * 
 * The initial image target sends FlowControlPermitMessage to the image provider after
 * each processed chunk. Upon receiving the FlowControlPermit message, the provider
 * will increase the number of permits available.
 *
 */
public class InitialImageFlowControl implements MembershipListener {
  private static final Logger logger = LogService.getLogger();
  
  private static final ProcessorKeeper21 keeper = new ProcessorKeeper21(false);
  private int id;
  private int maxPermits = InitialImageOperation.CHUNK_PERMITS;
  private final Semaphore permits = new Semaphore(maxPermits);
  private final DM dm;
  private final InternalDistributedMember target;
  private final AtomicBoolean aborted = new AtomicBoolean();
  
  public static InitialImageFlowControl register(DM dm, InternalDistributedMember target) {
    InitialImageFlowControl control =new InitialImageFlowControl(dm, target);
    int id = keeper.put(control);
    control.id = id;
    
    Set availableIds = dm.addMembershipListenerAndGetDistributionManagerIds(control);
    if(!availableIds.contains(target)) {
      control.abort();
    }
    return control;
  }

  private InitialImageFlowControl(DM dm, InternalDistributedMember target) {
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
      }
      catch (InterruptedException e) {
        interrupted = true; // keep looping
        checkCancellation();
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // while
    if(!aborted.get()) {
      incMessagesInFlight(1);
    }
  }
  
  private void basicWait(long startWaitTime) throws InterruptedException {
    long timeout = getAckWaitThreshold() * 1000L;
    long timeSoFar = System.currentTimeMillis() - startWaitTime;
    if (timeout <= 0) {
      timeout = Long.MAX_VALUE;
    }
    if (!aborted.get() && !permits.tryAcquire(timeout-timeSoFar-1, TimeUnit.MILLISECONDS)) {
      checkCancellation();

      Set activeMembers = dm.getDistributionManagerIds();
      final Object[] msgArgs = new Object[] {getAckWaitThreshold(), this, dm.getId(), activeMembers};
      final StringId msg = LocalizedStrings.ReplyProcessor21_0_SEC_HAVE_ELAPSED_WHILE_WAITING_FOR_REPLIES_1_ON_2_WHOSE_CURRENT_MEMBERSHIP_LIST_IS_3;
      logger.warn(LocalizedMessage.create(msg, msgArgs));

      permits.acquire();
      
      // Give an info message since timeout gave a warning.
      logger.info(LocalizedMessage.create(LocalizedStrings.ReplyProcessor21_WAIT_FOR_REPLIES_COMPLETED_1, "InitialImageFlowControl"));
    }
  }
      
  /**
   * Return the time in sec to wait before sending an alert while
   * waiting for ack replies.  Note that the ack wait threshold may
   * change at runtime, so we have to consult the system every time.
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
  
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    if(id.equals(target)) {
      abort();
    }
    
  }

  private void abort() {
    if(!aborted.getAndSet(true)) {
      incMessagesInFlight(- (maxPermits - permits.availablePermits()));
      // Just in case java has issues with semaphores rolling over, set this
      // to half Integer.MAX_VALUE rather to release all of the waiters
      permits.release(Integer.MAX_VALUE / 2);
    }
  }

  public void memberJoined(InternalDistributedMember id) {
    //Do nothing
  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }

  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {
    //Do nothing
  }

  @Override
  public String toString() {
    return "<InitialImageFlowControl for GII to " + target + " with " + permits.availablePermits() + " available permits>";
  }


  public static class FlowControlPermitMessage extends DistributionMessage implements DataSerializableFixedID {
    private int keeperId;
    
    private FlowControlPermitMessage(int keeperId2) {
      this.keeperId = keeperId2;
    }

    public FlowControlPermitMessage() {
    }

    public static void send(DM dm, InternalDistributedMember recipient, int keeperId) {
      FlowControlPermitMessage message = new FlowControlPermitMessage(keeperId);
      message.setRecipient(recipient);
      dm.putOutgoing(message);
    }

    @Override
    public int getProcessorType() {
      return DistributionManager.STANDARD_EXECUTOR;
    }
    
    @Override
    public boolean getInlineProcess() {
      return true;
    }

    @Override
    protected void process(DistributionManager dm) {
      InitialImageFlowControl control = (InitialImageFlowControl) keeper.retrieve(keeperId);
      if(control != null) {
        control.releasePermit();
      }
    }

    public int getDSFID() {
      return FLOW_CONTROL_PERMIT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      keeperId = in.readInt();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(keeperId);
    }
    
    
    
  }
}
