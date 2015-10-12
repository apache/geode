/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
/**
 * Distribution message for dropping client from blacklist.
 * 
 * @since 6.0
 *
 */
public class RemoveClientFromBlacklistMessage extends PooledDistributionMessage {
  private static final Logger logger = LogService.getLogger();

  //The proxy id of the client represented by this proxy
  private ClientProxyMembershipID proxyID;
  
  @Override
  protected void process(DistributionManager dm) {
    final Cache cache;
    try {
      // use GemFireCache.getInstance to avoid blocking during cache.xml
      // processing.
      cache = GemFireCacheImpl.getInstance();
    }
    catch (Exception ignore) {
      DistributedSystem ds = dm.getSystem();
      if (ds != null) {
        if (logger.isTraceEnabled()) {
          logger.trace("The node does not contain cache & so QDM Message will return.", ignore);
        }
      }
      return;
    }

    Cache c = GemFireCacheImpl.getInstance();
    if (c != null) {
      List l = c.getCacheServers();
      if (l != null) {
        Iterator i = l.iterator();
        while (i.hasNext()) {
          CacheServerImpl bs = (CacheServerImpl)i.next();
          CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();
          Set s = ccn.getBlacklistedClient();
          if (s != null) {
            if(s.remove(proxyID)){          
            DistributedSystem ds = dm.getSystem();
            if (ds != null) {
              if (logger.isDebugEnabled()) {
                logger.debug("Remove the client from black list as its queue is already destroyed: {}", proxyID);
              }
            }
           }
        }
      }
    }
   }   
  }

  public RemoveClientFromBlacklistMessage() {
    this.setRecipient(ALL_RECIPIENTS);
  }

  public void setProxyID(ClientProxyMembershipID proxyID) {
    this.proxyID = proxyID;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(proxyID, out);
  }

  public int getDSFID() {
    return REMOVE_CLIENT_FROM_BLACKLIST_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    proxyID = ClientProxyMembershipID.readCanonicalized(in);
  }  
  
}  