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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqQueryImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.CqServiceImpl;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class StopCQ extends BaseCQCommand {

  private final static StopCQ singleton = new StopCQ();

  public static Command getCommand() {
    return singleton;
  }

  private StopCQ() {
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
      logger.debug("{}: Received stop CQ request from {} cqName: {}", servConn.getName(), servConn.getSocketString(), cqName);
    }

    // Process the query request
    if (cqName == null) {
      String err = LocalizedStrings.StopCQ_THE_CQNAME_FOR_THE_CQ_STOP_REQUEST_IS_NULL.toLocalizedString();
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
          .getTransactionId(), null, servConn);
      return;
    }

    // Process CQ stop request
    try {
      // Append Client ID to CQ name
      CqService cqService = crHelper.getCache().getCqService();
      cqService.start();
      // String cqNameWithClientId = new String(cqName + "__" +
      // getMembershipID());
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
          cqRegionNames.add(((CqQueryImpl)cqQuery).getRegionName());
        }
        authzRequest.stopCQAuthorize(cqName, queryStr, cqRegionNames);
      }
      cqService.stopCq(cqName, id);
      if(cqQuery != null)
        servConn.removeCq(cqName, cqQuery.isDurable());
    }
    catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      return;
    }
    catch (Exception e) {
      String err = LocalizedStrings.StopCQ_EXCEPTION_WHILE_STOPPING_CQ_NAMED_0
        .toLocalizedString(cqName);
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err,
          msg.getTransactionId(), e, servConn);
      return;
    }

    // Send OK to client
    sendCqResponse(MessageType.REPLY, LocalizedStrings.StopCQ_CQ_STOPPED_SUCCESSFULLY.toLocalizedString(), msg
        .getTransactionId(), null, servConn);

    servConn.setAsTrue(RESPONDED);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessStopCqTime(start - oldStart);
    }

  }

}
