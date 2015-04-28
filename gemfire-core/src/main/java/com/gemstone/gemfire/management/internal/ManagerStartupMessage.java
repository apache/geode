/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
