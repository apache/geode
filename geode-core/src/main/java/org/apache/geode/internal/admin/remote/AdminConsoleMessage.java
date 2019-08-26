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

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular distribution manager to let it know that the sender is an
 * administration console that just connected.
 */
public class AdminConsoleMessage extends PooledDistributionMessage {
  // instance variables
  private int level;

  public static AdminConsoleMessage create(int level) {
    AdminConsoleMessage m = new AdminConsoleMessage();
    m.setLevel(level);
    return m;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    if (level != Alert.OFF) {
      dm.getAlertingService().addAlertListener(getSender(), AlertLevel.find(level));
    }
    dm.addAdminConsole(getSender());
  }

  @Override
  public int getDSFID() {
    return ADMIN_CONSOLE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(level);
  }

  @Override
  public void fromData(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    level = in.readInt();
  }

  @Override
  public String toString() {
    return "AdminConsoleMessage from " + getSender() + " level=" + level;
  }

}
