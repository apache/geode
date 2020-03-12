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
package org.apache.geode.cache.query.cq.internal.command;

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.operations.ExecuteCQOperationContext;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.cq.internal.CqServiceImpl;
import org.apache.geode.cache.query.cq.internal.ServerCQImpl;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqServiceProvider;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.vmotion.VMotionObserver;
import org.apache.geode.internal.cache.vmotion.VMotionObserverHolder;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * @since GemFire 6.1
 */
public class ExecuteCQ61 extends BaseCQCommand {
  protected static final Logger logger = LogService.getLogger();

  private static final ExecuteCQ61 singleton = new ExecuteCQ61();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteCQ61() {
    // nothing
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Acceptor acceptor = serverConnection.getAcceptor();
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
    // region data policy
    Part regionDataPolicyPart = clientMessage.getPart(clientMessage.getNumberOfParts() - 1);
    byte[] regionDataPolicyPartBytes = regionDataPolicyPart.getSerializedForm();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received {} request from {} CqName: {} queryString: {}",
          serverConnection.getName(), MessageType.getString(clientMessage.getMessageType()),
          serverConnection.getSocketString(), cqName, cqQueryString);
    }

    // Check if the Server is running in NotifyBySubscription=true mode.
    CacheClientNotifier ccn = acceptor.getCacheClientNotifier();
    if (ccn != null) {
      CacheClientProxy proxy = ccn.getClientProxy(id);
      if (proxy != null && !proxy.isNotifyBySubscription()) {
        // This should have been taken care at the client.
        String err =
            "Server notifyBySubscription mode is set to false. CQ execution is not supported in this mode.";
        sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, clientMessage.getTransactionId(),
            null, serverConnection);
        return;
      }
    }

    DefaultQueryService qService;
    CqServiceImpl cqServiceForExec;
    Query query;
    Set cqRegionNames;
    ExecuteCQOperationContext executeCQContext = null;
    ServerCQImpl cqQuery;

    try {
      qService = (DefaultQueryService) crHelper.getCache().getLocalQueryService();

      // Authorization check
      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      query = qService.newQuery(cqQueryString);
      cqRegionNames = ((DefaultQuery) query).getRegionsInQuery(null);
      if (authzRequest != null) {
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

      // auth check to see if user can create CQ or not
      ((DefaultQuery) query).getRegionsInQuery(null).forEach((regionName) -> securityService
          .authorize(Resource.DATA, Operation.READ, regionName));

      // test hook to trigger vMotion during CQ registration
      if (CqServiceProvider.VMOTION_DURING_CQ_REGISTRATION_FLAG) {
        VMotionObserver vmo = VMotionObserverHolder.getInstance();
        vmo.vMotionBeforeCQRegistration();
      }

      cqServiceForExec = (CqServiceImpl) qService.getCqService();
      // registering cq with serverConnection so that when CCP will require auth info it can access
      // that
      // registering cq auth before as possibility that you may get event
      serverConnection.setCq(cqName, isDurable);
      cqQuery = (ServerCQImpl) cqServiceForExec.executeCq(cqName, cqQueryString, cqState, id, ccn,
          isDurable, true, regionDataPolicyPartBytes[0], null);
    } catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", clientMessage.getTransactionId(), cqe,
          serverConnection);
      serverConnection.removeCq(cqName, isDurable);
      return;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.removeCq(cqName, isDurable);
      return;
    }

    boolean sendResults = false;

    if (clientMessage.getMessageType() == MessageType.EXECUTECQ_WITH_IR_MSG_TYPE) {
      sendResults = true;
    }

    // Execute the query only if it is execute with initial results or
    // if it is a non PR query with execute query and maintain keys flags set
    boolean successQuery = false;
    if (sendResults || CqServiceImpl.EXECUTE_QUERY_DURING_INIT && CqServiceProvider.MAINTAIN_KEYS
        && !cqQuery.isPR()) {
      // Execute the query and send the result-set to client.
      try {
        if (query == null) {
          query = qService.newQuery(cqQueryString);
          cqRegionNames = ((DefaultQuery) query).getRegionsInQuery(null);
        }
        ((DefaultQuery) query).setIsCqQuery(true);
        successQuery =
            processQuery(clientMessage, query, cqQueryString, cqRegionNames,
                start, cqQuery, executeCQContext, serverConnection, sendResults, securityService);


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
          } catch (Exception ignored) {
            // Ignore.
          }
        }
      }
    } else {
      // Don't execute query for cq.execute and
      // if it is a PR query with execute query and maintain keys flags not set
      cqQuery.setCqResultsCacheInitialized();
      successQuery = true;
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
