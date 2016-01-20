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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class CloseCQ extends BaseCQCommand {

  private final static CloseCQ singleton = new CloseCQ();

  public static Command getCommand() {
    return singleton;
  }

  private CloseCQ() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    ClientProxyMembershipID id = servConn.getProxyID();
    CacheServerStats stats = servConn.getCacheServerStats();

    // Based on MessageType.QUERY
    // Added by Rao 2/1/2007
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    String cqName = msg.getPart(0).getString();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received close CQ request from {} cqName: {}", servConn.getName(), servConn.getSocketString(), cqName);
    }

    // Process the query request
    if (cqName == null) {
      String err = LocalizedStrings.CloseCQ_THE_CQNAME_FOR_THE_CQ_CLOSE_REQUEST_IS_NULL.toLocalizedString();
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
          .getTransactionId(), null, servConn);
      return;
    }

    // Process CQ close request
    try {
      // Append Client ID to CQ name
      CqService cqService = crHelper.getCache().getCqService();
      cqService.start();
      
      String serverCqName = cqName;
      if (id != null) {
        serverCqName = cqService.constructServerCqName(cqName, id);
      }
      InternalCqQuery cqQuery = cqService.getCq(serverCqName);
      
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        String queryStr = null;
        Set cqRegionNames = null;        
        
        if (cqQuery != null) {
          queryStr = cqQuery.getQueryString();
          cqRegionNames = new HashSet();
          cqRegionNames.add(((InternalCqQuery)cqQuery).getRegionName());
          authzRequest.closeCQAuthorize(cqName, queryStr, cqRegionNames);
        }
        
      }
      // String cqNameWithClientId = new String(cqName + "__" +
      // getMembershipID());
      cqService.closeCq(cqName, id);
      if(cqQuery != null)
        servConn.removeCq(cqName, cqQuery.isDurable());
    }
    catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      return;
    }
    catch (Exception e) {
      String err = LocalizedStrings.CloseCQ_EXCEPTION_WHILE_CLOSING_CQ_CQNAME_0.toLocalizedString(cqName);
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err,
          msg.getTransactionId(), e, servConn);
      return;
    }

    // Send OK to client
    sendCqResponse(MessageType.REPLY, LocalizedStrings.CloseCQ_CQ_CLOSED_SUCCESSFULLY.toLocalizedString(), msg.getTransactionId(), null, servConn);
    servConn.setAsTrue(RESPONDED);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessCloseCqTime(start - oldStart);
    }
  }

}
