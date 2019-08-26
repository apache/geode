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

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to make members of the distributed system aware that a manager agent wants
 * alerts at a new level.
 *
 * @since GemFire 3.5
 */
public class AlertLevelChangeMessage extends SerialDistributionMessage {

  private static final Logger logger = LogService.getLogger();

  /** The new alert level */
  private int newLevel;

  /**
   * Creates a new {@code AlertLevelChangeMessage}.
   */
  public static AlertLevelChangeMessage create(int newLevel) {
    AlertLevelChangeMessage alertLevelChangeMessage = new AlertLevelChangeMessage();
    alertLevelChangeMessage.newLevel = newLevel;
    return alertLevelChangeMessage;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    dm.getAlertingService().removeAlertListener(getSender());

    if (newLevel != AlertLevel.NONE.intLevel()) {
      dm.getAlertingService().addAlertListener(getSender(), AlertLevel.find(newLevel));
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "Added new AlertListener");
      }
    }
  }

  @Override
  public int getDSFID() {
    return ALERT_LEVEL_CHANGE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(newLevel);
  }

  @Override
  public void fromData(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    newLevel = in.readInt();
  }

  @Override
  public String toString() {
    return String.format("Changing alert level to %s", AlertLevel.find(newLevel));
  }
}
