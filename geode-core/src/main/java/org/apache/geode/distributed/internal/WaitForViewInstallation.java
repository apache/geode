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

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class WaitForViewInstallation extends HighPriorityDistributionMessage
    implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  public static void send(ClusterDistributionManager dm) throws InterruptedException {
    long viewId = dm.getMembershipManager().getView().getViewId();
    ReplyProcessor21 rp = new ReplyProcessor21(dm, dm.getOtherDistributionManagerIds());
    rp.enableSevereAlertProcessing();
    dm.putOutgoing(new WaitForViewInstallation(viewId, rp.getProcessorId()));
    try {
      rp.waitForReplies();
    } catch (ReplyException e) {
      if (e.getCause() != null && !(e.getCause() instanceof CancelException)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Reply to WaitForViewInstallation received odd exception", e.getCause());
        }
      }
    }
    // this isn't necessary for TXFailoverCommand, which is the only use of this
    // message right now. TXFailoverCommand performs messaging to all servers,
    // which will force us to wait for the view containing the crash of another
    // server to be processed.
    // dm.waitForViewInstallation(viewId);
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  private long viewId;
  private int processorId;

  /** for deserialization */
  public WaitForViewInstallation() {}

  private WaitForViewInstallation(long viewId, int processorId) {
    this.viewId = viewId;
    this.processorId = processorId;
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return WAIT_FOR_VIEW_INSTALLATION;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeLong(this.viewId);
    out.writeInt(this.processorId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws ClassNotFoundException, IOException {
    super.fromData(in, context);
    this.viewId = in.readLong();
    this.processorId = in.readInt();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.distributed.internal.DistributionMessage#process(org.apache.geode.distributed.
   * internal.DistributionManager)
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    boolean interrupted = false;
    try {
      dm.waitForViewInstallation(this.viewId);
    } catch (InterruptedException e) {
      interrupted = true;
    } finally {
      if (!interrupted) {
        ReplyMessage.send(getSender(), this.processorId, null, getReplySender(dm));
      }
    }
  }
}
