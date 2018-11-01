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
package org.apache.geode.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.logging.log4j.AlertAppender;

/**
 * Sent by JMX manager to all other members to notify them that it has started.
 */
public class ManagerStartupMessage extends PooledDistributionMessage {

  private int alertLevel;

  public static ManagerStartupMessage create(int alertLevel) {
    return new ManagerStartupMessage(alertLevel);
  }

  public ManagerStartupMessage() {
    // required for DataSerializableFixedID
  }

  private ManagerStartupMessage(int alertLevel) {
    this.alertLevel = alertLevel;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    if (alertLevel != Alert.OFF) {
      AlertAppender.getInstance().addAlertListener(getSender(), alertLevel);
    }
  }

  @Override
  public int getDSFID() {
    return MANAGER_STARTUP_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(alertLevel);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    alertLevel = in.readInt();
  }

  @Override
  public String toString() {
    return "ManagerStartupMessage from " + getSender() + " alertLevel=" + alertLevel;
  }
}
