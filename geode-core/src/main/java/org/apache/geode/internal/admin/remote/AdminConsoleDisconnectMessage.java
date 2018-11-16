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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.statistics.GemFireStatSampler;

/**
 * A message that is sent to a particular distribution manager to let it know that the sender is an
 * administation console that just disconnected.
 */
public class AdminConsoleDisconnectMessage extends PooledDistributionMessage {
  private static final Logger logger = LogService.getLogger();

  // instance variables
  private boolean alertListenerExpected;
  private transient boolean ignoreAlertListenerRemovalFailure;
  private boolean crashed;
  /** The reason for getting disconnected */
  private String reason;

  public static AdminConsoleDisconnectMessage create() {
    AdminConsoleDisconnectMessage m = new AdminConsoleDisconnectMessage();
    return m;
  }

  /**
   * This is called by a dm when it sends this message to itself as a result of the console dropping
   * out of the view (ie. crashing)
   */
  public void setCrashed(boolean crashed) {
    this.crashed = crashed;
  }

  public void setAlertListenerExpected(boolean alertListenerExpected) {
    this.alertListenerExpected = alertListenerExpected;
  }

  public void setIgnoreAlertListenerRemovalFailure(boolean ignore) {
    this.ignoreAlertListenerRemovalFailure = ignore;
  }

  /**
   * @param reason the reason for getting disconnected
   *
   * @since GemFire 6.5
   */
  public void setReason(String reason) {
    this.reason = reason;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    InternalDistributedSystem sys = dm.getSystem();
    // DistributionConfig config = sys.getConfig();
    if (alertListenerExpected) {
      if (!AlertAppender.getInstance().removeAlertListener(this.getSender())
          && !this.ignoreAlertListenerRemovalFailure) {
        logger.warn("Unable to remove console with id {} from alert listeners.",
            this.getSender());
      }
    }
    GemFireStatSampler sampler = sys.getStatSampler();
    if (sampler != null) {
      sampler.removeListenersByRecipient(this.getSender());
    }
    dm.handleConsoleShutdown(this.getSender(), crashed,
        String.format("Reason for automatic admin disconnect : %s",
            reason));
    // AppCacheSnapshotMessage.flushSnapshots(this.getSender());
  }

  public int getDSFID() {
    return ADMIN_CONSOLE_DISCONNECT_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeBoolean(alertListenerExpected);
    out.writeBoolean(crashed);
    DataSerializer.writeString(reason, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.alertListenerExpected = in.readBoolean();
    this.crashed = in.readBoolean();
    this.reason = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "AdminConsoleDisconnectMessage from " + this.getSender();
  }
}
