/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A message that is sent to a particular distribution manager to cancel an
 * admin request
 */
public final class CancellationMessage extends PooledDistributionMessage {
  //instance variables
  private int msgToCancel;

  public static CancellationMessage create(InternalDistributedMember recipient, int msgToCancel) {
    CancellationMessage m = new CancellationMessage();
    m.msgToCancel = msgToCancel;
    m.setRecipient(recipient);
    return m;
  }

  @Override
  public void process(DistributionManager dm) {
    CancellationRegistry.getInstance().cancelMessage(this.getSender(), msgToCancel);
  }

  public int getDSFID() {
    return CANCELLATION_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(msgToCancel);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    msgToCancel = in.readInt();
  }

  @Override
  public String toString(){
    return LocalizedStrings.CancellationMessage_CANCELLATIONMESSAGE_FROM_0_FOR_MESSAGE_ID_1.toLocalizedString(new Object[] { this.getSender(), Integer.valueOf(msgToCancel)});
  }
}
