/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.operations.ExecuteCQOperationContext;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;

public class ExecuteCQ extends BaseCQCommand {
  protected static final Logger logger = LogService.getLogger();

  private static final ExecuteCQ singleton = new ExecuteCQ();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteCQ() {
    // nothing
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    AcceptorImpl acceptor = serverConnection.getAcceptor();
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    ClientProxyMembershipID id = serverConnection.getProxyID();
    CacheServerStats stats = serverConnection.getCacheServerStats();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // Retrieve the data from the message parts
    String cqName = clientMessage.getPart(0).getString();
    String cqQueryString = clientMessage.getPart(1).getString();
    int cqState = clientMessage.getPart(2).getInt();

    Part isDurablePart = clientMessage.getPart(3);
    byte[] isDurableByte = isDurablePart.getSerializedForm();
    boolean isDurable = !(isDurableByte == null || isDurableByte[0] == 0);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received {} request from {} CqName: {} queryString: {}",
          serverConnection.getName(), MessageType.getString(clientMessage.getMessageType()),
          serverConnection.getSocketString(), cqName, cqQueryString);
    }

    DefaultQueryService qService = null;
    CqService cqServiceForExec = null;
    Query query = null;
    Set cqRegionNames = null;
    ExecuteCQOperationContext executeCQContext = null;
    ServerCQ cqQuery = null;

    try {
      qService = (DefaultQueryService) crHelper.getCache().getLocalQueryService();

      // Authorization check
      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        query = qService.newQuery(cqQueryString);
        cqRegionNames = ((DefaultQuery) query).getRegionsInQuery(null);
        executeCQContext = authzRequest.executeCQAuthorize(cqName, cqQueryString, cqRegionNames);
        String newCqQueryString = executeCQContext.getQuery();

        if (!cqQueryString.equals(newCqQueryString)) {
          query = qService.newQuery(newCqQueryString);
          cqQueryString = newCqQueryString;
          cqRegionNames = executeCQContext.getRegionNames();
          if (cqRegionNames == null) {
            cqRegionNames = ((DefaultQuery) query).getRegionsInQuery(null);
          }
        }
      }

      cqServiceForExec = qService.getCqService();
      cqQuery = cqServiceForExec.executeCq(cqName, cqQueryString, cqState, id,
          acceptor.getCacheClientNotifier(), isDurable, false, 0, null);
    } catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", clientMessage.getTransactionId(), cqe,
          serverConnection);
      return;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      return;
    }

    boolean sendResults = false;

    if (clientMessage.getMessageType() == MessageType.EXECUTECQ_WITH_IR_MSG_TYPE) {
      sendResults = true;
    }

    // Execute the query and send the result-set to client.
    boolean successQuery = false;
    try {
      if (query == null) {
        query = qService.newQuery(cqQueryString);
        cqRegionNames = ((DefaultQuery) query).getRegionsInQuery(null);
      }
      ((DefaultQuery) query).setIsCqQuery(true);
      successQuery = processQuery(clientMessage, query, cqQueryString, cqRegionNames, start,
          cqQuery, executeCQContext, serverConnection, sendResults, securityService);

      // Update the CQ statistics.
      cqQuery.getVsdStats().setCqInitialResultsTime(DistributionStats.getStatTime() - start);
      stats.incProcessExecuteCqWithIRTime(DistributionStats.getStatTime() - start);
      // logger.fine("Time spent in execute with initial results :" +
      // DistributionStats.getStatTime() + ", " + oldstart);
    } finally { // To handle any exception.
      // If failure to execute the query, close the CQ.
      if (!successQuery) {
        try {
          cqServiceForExec.closeCq(cqName, id);
        } catch (Exception ignore) {
          // Ignore.
        }
      }
    }

    if (!sendResults && successQuery) {
      // Send OK to client
      sendCqResponse(MessageType.REPLY,
          "cq created successfully.",
          clientMessage.getTransactionId(), null, serverConnection);

      long start2 = DistributionStats.getStatTime();
      stats.incProcessCreateCqTime(start2 - start);
    }
    serverConnection.setAsTrue(RESPONDED);
  }

}
