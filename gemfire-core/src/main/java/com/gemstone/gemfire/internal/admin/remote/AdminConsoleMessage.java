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
