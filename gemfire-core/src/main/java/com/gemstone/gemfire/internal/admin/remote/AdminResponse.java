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
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A message that is sent as a reply to a {@link AdminRequest}.
 */
public abstract class AdminResponse extends HighPriorityDistributionMessage
  implements AdminMessageType {

  // instance variables

  private int msgId; // message id of request this is a response to

  // instance methods

  int getMsgId() {
    return this.msgId;
  }

  void setMsgId(int msgId) {
    this.msgId = msgId;
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }

  /**
   * This method is invoked on the side that sent the original AdminRequest.
   */
  @Override
  protected void process(DistributionManager dm) {
    AdminWaiters.sendResponse(this);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    //System.out.println("BEGIN AdminResponse toData");
    super.toData(out);
    out.writeInt(this.msgId);
    //System.out.println("END AdminResponse toData");
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.msgId = in.readInt();
  }

  public InternalDistributedMember getRecipient() {
    InternalDistributedMember[] recipients = getRecipients();
    int size = recipients.length;
    if (size == 0) {
      return null;
    } else if (size > 1) {
      throw new
        IllegalStateException(
          LocalizedStrings.AdminResponse_COULD_NOT_RETURN_ONE_RECIPIENT_BECAUSE_THIS_MESSAGE_HAS_0_RECIPIENTS.toLocalizedString(Integer.valueOf(size)));
    } else {
      return recipients[0];
    }
  }
}
