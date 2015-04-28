/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReliableReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @author sbawaska
 *
 */
public class JtaBeforeCompletionMessage extends TXMessage {
  private static final Logger logger = LogService.getLogger();

  public JtaBeforeCompletionMessage() {
  }
  
  public JtaBeforeCompletionMessage(int txUniqId, InternalDistributedMember onBehalfOfClientMember, ReplyProcessor21 processor) {
    super(txUniqId,onBehalfOfClientMember, processor);
  }

  public static ReliableReplyProcessor21 send(Cache cache, int txUniqId,InternalDistributedMember onBehalfOfClientMember, DistributedMember recipient) {
    final InternalDistributedSystem system =
      (InternalDistributedSystem)cache.getDistributedSystem();
    final Set recipients = Collections.singleton(recipient);
    ReliableReplyProcessor21 response = new ReliableReplyProcessor21(system, recipients);
    JtaBeforeCompletionMessage msg = new JtaBeforeCompletionMessage(txUniqId,onBehalfOfClientMember,
        response);
    msg.setRecipients(recipients);
    system.getDistributionManager().putOutgoing(msg);
    return response;

  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXMessage#operateOnTx(com.gemstone.gemfire.internal.cache.TXId)
   */
  @Override
  protected boolean operateOnTx(TXId txId,DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    TXManagerImpl txMgr = cache.getTXMgr();
    if (logger.isDebugEnabled()) {
      logger.debug("JTA: Calling beforeCompletion for :{}", txId);
    }
    txMgr.getTXState().beforeCompletion();
    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return JTA_BEFORE_COMPLETION_MESSAGE;
  }
}
