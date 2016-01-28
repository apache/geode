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

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.operations.ExecuteCQOperationContext;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqServiceImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqServiceProvider;
import com.gemstone.gemfire.cache.query.internal.cq.ServerCQImpl;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.vmotion.VMotionObserver;
import com.gemstone.gemfire.internal.cache.vmotion.VMotionObserverHolder;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;


/**
 * @author aingle
 * @since 6.1
 */

public class ExecuteCQ61 extends BaseCQCommand {
  protected static final Logger logger = LogService.getLogger();

  private final static ExecuteCQ61 singleton = new ExecuteCQ61();
  
  public static Command getCommand() {
    return singleton;
  }

  private ExecuteCQ61() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    AcceptorImpl acceptor = servConn.getAcceptor();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    ClientProxyMembershipID id = servConn.getProxyID();
    CacheServerStats stats = servConn.getCacheServerStats();

    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    
    // Retrieve the data from the message parts
    String cqName = msg.getPart(0).getString();
    String cqQueryString = msg.getPart(1).getString();
    int cqState = msg.getPart(2).getInt();

    Part isDurablePart = msg.getPart(3);
    byte[] isDurableByte = isDurablePart.getSerializedForm();
    boolean isDurable = (isDurableByte == null || isDurableByte[0] == 0) ? false
        : true;
//  region data policy
    Part regionDataPolicyPart = msg.getPart(msg.getNumberOfParts()-1);
    byte[] regionDataPolicyPartBytes = regionDataPolicyPart.getSerializedForm();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received {} request from {} CqName: {} queryString: {}", servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getSocketString(), cqName, cqQueryString);
    }

    // Check if the Server is running in NotifyBySubscription=true mode.
    CacheClientNotifier ccn = acceptor.getCacheClientNotifier();
    if (ccn != null) {
      CacheClientProxy proxy = ccn.getClientProxy(id);
      if (proxy != null && !proxy.isNotifyBySubscription()) {
        // This should have been taken care at the client.
        String err = LocalizedStrings.ExecuteCQ_SERVER_NOTIFYBYSUBSCRIPTION_MODE_IS_SET_TO_FALSE_CQ_EXECUTION_IS_NOT_SUPPORTED_IN_THIS_MODE.toLocalizedString();
        sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
          .getTransactionId(), null, servConn);
        return;
      }
    }

    DefaultQueryService qService = null;
    CqServiceImpl cqServiceForExec = null;
    Query query = null;
    Set cqRegionNames = null;
    ExecuteCQOperationContext executeCQContext = null;
    ServerCQImpl cqQuery = null;
    
    try {
      qService = (DefaultQueryService)((GemFireCacheImpl)crHelper.getCache()).getLocalQueryService();

      // Authorization check
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        query = qService.newQuery(cqQueryString);
        cqRegionNames = ((DefaultQuery)query).getRegionsInQuery(null);
        executeCQContext = authzRequest.executeCQAuthorize(cqName,
            cqQueryString, cqRegionNames);
        String newCqQueryString = executeCQContext.getQuery();
        
        if (!cqQueryString.equals(newCqQueryString)) {
          query = qService.newQuery(newCqQueryString);
          cqQueryString = newCqQueryString;
          cqRegionNames = executeCQContext.getRegionNames();
          if (cqRegionNames == null) {
            cqRegionNames = ((DefaultQuery)query).getRegionsInQuery(null);
          }
        }
      }
      
      // test hook to trigger vMotion during CQ registration

      if (CqServiceProvider.VMOTION_DURING_CQ_REGISTRATION_FLAG) {
        VMotionObserver vmo = VMotionObserverHolder.getInstance();
        vmo.vMotionBeforeCQRegistration();
      }

      cqServiceForExec = (CqServiceImpl) qService.getCqService();
      //registering cq with serverConnection so that when CCP will require auth info it can access that
      //registering cq auth before as possibility that you may get event
      servConn.setCq(cqName, isDurable);
      cqQuery = (ServerCQImpl) cqServiceForExec.executeCq(cqName, cqQueryString, cqState, 
        id, ccn, isDurable, true, regionDataPolicyPartBytes[0], null);  
    }
    catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      servConn.removeCq(cqName, isDurable);
      return;
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.removeCq(cqName, isDurable);
      return;
    }

    long oldstart = start;
    boolean sendResults = false;
    boolean successQuery = false;

    if (msg.getMessageType() == MessageType.EXECUTECQ_WITH_IR_MSG_TYPE) {
      sendResults = true;
    }
    
    // Execute the query only if it is execute with initial results or
    // if it is a non PR query with execute query and maintain keys flags set
    if(sendResults || (CqServiceImpl.EXECUTE_QUERY_DURING_INIT && CqServiceProvider.MAINTAIN_KEYS && !cqQuery.isPR())) {
      // Execute the query and send the result-set to client.
      try {
        if (query == null) {
          query = qService.newQuery(cqQueryString);
          cqRegionNames = ((DefaultQuery)query).getRegionsInQuery(null);
        }
        ((DefaultQuery)query).setIsCqQuery(true);
        successQuery = processQuery(msg, query, cqQueryString,
            cqRegionNames, start, cqQuery, executeCQContext, servConn, sendResults);
      

        // Update the CQ statistics.
        cqQuery.getVsdStats().setCqInitialResultsTime((DistributionStats.getStatTime()) - oldstart);
        stats.incProcessExecuteCqWithIRTime((DistributionStats.getStatTime()) - oldstart);
        //logger.fine("Time spent in execute with initial results :" + DistributionStats.getStatTime() + ", " +  oldstart);
      } finally { // To handle any exception.
        // If failure to execute the query, close the CQ.
        if (!successQuery) {
          try {
            cqServiceForExec.closeCq(cqName, id);
          }
          catch (Exception ex) {
            // Ignore.
          }      
        }
      }
    } else {
      // Don't execute query for cq.execute and
      // if it is a PR query with execute query and maintain keys flags not set
      cqQuery.cqResultKeysInitialized = true;
      successQuery = true;
    }
      
    if (!sendResults && successQuery) {
      // Send OK to client
      sendCqResponse(MessageType.REPLY, LocalizedStrings.ExecuteCQ_CQ_CREATED_SUCCESSFULLY.toLocalizedString(), msg
          .getTransactionId(), null, servConn);

      long start2 = DistributionStats.getStatTime();
      stats.incProcessCreateCqTime(start2 - oldstart);
    }
    servConn.setAsTrue(RESPONDED);

  }

}
