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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.ManagerLogWriter;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;

/**
 * A message that is sent to a particular distribution manager to let
 * it know that the sender is an administation console that just connected.
 */
public final class AdminConsoleMessage extends PooledDistributionMessage {
  //instance variables
  int level;

  public static AdminConsoleMessage create(int level) {
    AdminConsoleMessage m = new AdminConsoleMessage();
    m.setLevel(level);
    return m;
  }

  public void setLevel(int level) {
    this.level = level;
  }
  
  @Override
  public void process(DistributionManager dm) {
    if (this.level != Alert.OFF) {
      AlertAppender.getInstance().addAlertListener(this.getSender(), this.level);
    }
    dm.addAdminConsole(this.getSender()); 
  }

  public int getDSFID() {
    return ADMIN_CONSOLE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.level);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.level = in.readInt();
  }

  @Override
  public String toString(){
    return "AdminConsoleMessage from " + this.getSender() + " level=" + level;
  }

}
