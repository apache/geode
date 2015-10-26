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
package com.gemstone.gemfire.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.ManagerLogWriter;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;

public class ManagerStartupMessage extends PooledDistributionMessage {
  //instance variables
  int alertLevel;

  public static ManagerStartupMessage create(int level) {
    ManagerStartupMessage m = new ManagerStartupMessage();
    m.setLevel(level);
    return m;
  }

  public void setLevel(int alertLevel) {
    this.alertLevel = alertLevel;
  }
  
  @Override
  public void process(DistributionManager dm) {
    
    if (this.alertLevel != Alert.OFF) { 
      AlertAppender.getInstance().addAlertListener(this.getSender(), this.alertLevel);
    }
  }

  public int getDSFID() {
    return MANAGER_STARTUP_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.alertLevel);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.alertLevel = in.readInt();
  }

  @Override
  public String toString(){
    return "ManagerStartupMessage from " + this.getSender() + " level=" + alertLevel;
  }


}
