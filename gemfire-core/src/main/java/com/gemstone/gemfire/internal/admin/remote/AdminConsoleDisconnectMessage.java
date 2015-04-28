/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.GemFireStatSampler;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * A message that is sent to a particular distribution manager to let
 * it know that the sender is an administation console that just disconnected.
 */
public final class AdminConsoleDisconnectMessage extends PooledDistributionMessage {
  private static final Logger logger = LogService.getLogger();
  
  //instance variables
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
   * This is called by a dm when it sends this message to itself as a result
   * of the console dropping out of the view (ie. crashing)
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
   * @since 6.5
   */
  public void setReason(String reason) {
    this.reason = reason;
  }

  @Override
  public void process(DistributionManager dm) {
    InternalDistributedSystem sys = dm.getSystem();
//    DistributionConfig config = sys.getConfig();
    if (alertListenerExpected) {  
      if (!AlertAppender.getInstance().removeAlertListener(this.getSender()) && !this.ignoreAlertListenerRemovalFailure) {
        logger.warn(LocalizedMessage.create(
          LocalizedStrings.ManagerLogWriter_UNABLE_TO_REMOVE_CONSOLE_WITH_ID_0_FROM_ALERT_LISTENERS,
          this.getSender()));
      }
    } 
    GemFireStatSampler sampler = sys.getStatSampler();
    if (sampler != null) {
      sampler.removeListenersByRecipient(this.getSender());
    }
    dm.handleConsoleShutdown(this.getSender(), crashed, LocalizedStrings.AdminConsoleDisconnectMessage_AUTOMATIC_ADMIN_DISCONNECT_0.toLocalizedString(reason));
//     AppCacheSnapshotMessage.flushSnapshots(this.getSender());
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
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.alertListenerExpected = in.readBoolean();
    this.crashed = in.readBoolean();
    this.reason = DataSerializer.readString(in);
  }

  @Override
  public String toString(){
    return "AdminConsoleDisconnectMessage from " + this.getSender();
  }
}
