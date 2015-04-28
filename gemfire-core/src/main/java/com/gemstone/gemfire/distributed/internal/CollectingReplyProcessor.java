/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This reply processor collects all of the exceptions/results from the
 * ReplyMessages it receives
 * 
 * @author bruces
 *
 */
public class CollectingReplyProcessor<T> extends ReplyProcessor21 {

  private Map<InternalDistributedMember, T> results = new HashMap<InternalDistributedMember, T>();

  public CollectingReplyProcessor(DM dm,
      Collection initMembers) {
    super(dm, initMembers);
  }
  
  @Override
  protected void process(DistributionMessage msg, boolean warn) {
    if (msg instanceof ReplyMessage) {
      InternalDistributedSystem.getLoggerI18n().info(LocalizedStrings.DEBUG,
          "processing message with return value " + ((ReplyMessage)msg).getReturnValue());
      results.put(msg.getSender(), (T)((ReplyMessage)msg).getReturnValue());
    }
    super.process(msg, warn);
  }
  
  public Map<InternalDistributedMember, T> getResults() {
    return this.results;
  }

}
