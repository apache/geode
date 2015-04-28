/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.admin.AlertLevel;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A message that is sent to make members of the distributed system
 * aware that a manager agent wants alerts at a new level.
 *
 * @see AlertLevel
 *
 * @author David Whitlock
 * @since 3.5
 */
public final class AlertLevelChangeMessage
  extends PooledDistributionMessage  {
  
  private static final Logger logger = LogService.getLogger();

  /** The new alert level */
  private int newLevel;

  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Creates a new <code>AlertLevelChangeMessage</code> 
   */
  public static AlertLevelChangeMessage create(int newLevel) {
    AlertLevelChangeMessage m = new AlertLevelChangeMessage();
    m.newLevel = newLevel;
    return m;
  }

  //////////////////////  Instance Methods  //////////////////////

  @Override
  public void process(DistributionManager dm) {
    AlertAppender.getInstance().removeAlertListener(this.getSender());

    if (this.newLevel != Alert.OFF) {
      AlertAppender.getInstance().addAlertListener(this.getSender(), this.newLevel);
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "Added new AlertListener to application log writer");
      }
    }
  }

  public int getDSFID() {
    return ALERT_LEVEL_CHANGE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.newLevel);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.newLevel = in.readInt();
  }

  @Override
  public String toString() {
    return LocalizedStrings.AlertLevelChangeMessage_CHANGING_ALERT_LEVEL_TO_0.toLocalizedString(AlertLevel.forSeverity(this.newLevel)); 
  }
}
