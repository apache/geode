/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReliableReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author sbawaska
 *
 */
public class TXRemoteRollbackMessage extends TXMessage {

  private static final Logger logger = LogService.getLogger();
  
  public TXRemoteRollbackMessage() {
  }

  public TXRemoteRollbackMessage(int txUniqId,InternalDistributedMember onBehalfOfClientMember, ReplyProcessor21 processor) {
    super(txUniqId,onBehalfOfClientMember, processor);
  }

  public static ReliableReplyProcessor21 send(Cache cache,
      int txUniqId,InternalDistributedMember onBehalfOfClientMember, DistributedMember recipient) {
    final InternalDistributedSystem system = 
                    (InternalDistributedSystem)cache.getDistributedSystem();
    final Set recipients = Collections.singleton(recipient);
    ReliableReplyProcessor21 response = 
                    new ReliableReplyProcessor21(system, recipients);
    TXRemoteRollbackMessage msg = new TXRemoteRollbackMessage(txUniqId,onBehalfOfClientMember, response);
    msg.setRecipients(recipients);
    system.getDistributionManager().putOutgoing(msg);
    return response;
  }

  @Override
  protected boolean operateOnTx(TXId txId,DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      throw new CacheClosedException(LocalizedStrings.CacheFactory_A_CACHE_HAS_NOT_YET_BEEN_CREATED.toLocalizedString());
    }
    TXManagerImpl txMgr = cache.getTXMgr();
    if (logger.isDebugEnabled()) {
      logger.debug("TX: Rolling back :{}", txId);
    }
    try {
      if (!txMgr.isHostedTxRecentlyCompleted(txId)) {
        txMgr.rollback();
      }
    } finally {
      txMgr.removeHostedTXState(txId);
    }
    return true;
  }

  public int getDSFID() {
    return TX_REMOTE_ROLLBACK_MESSAGE;
  }

}
