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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.GetDurableCQsOperationContext;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;


public class GetDurableCQs extends BaseCQCommand {

  private final static GetDurableCQs singleton = new GetDurableCQs();

  public static Command getCommand() {
    return singleton;
  }

  private GetDurableCQs() {
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

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received {} request from {}", servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getSocketString());
    }

    DefaultQueryService qService = null;
    CqService cqServiceForExec = null;
    Query query = null;
    Set cqRegionNames = null;
    GetDurableCQsOperationContext getDurableCqsOperationContext = null;
    InternalCqQuery cqQuery = null;

    try {
      qService = (DefaultQueryService) ((GemFireCacheImpl) crHelper.getCache())
          .getLocalQueryService();

      // Authorization check
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        authzRequest.getDurableCQsAuthorize();
      }

      cqServiceForExec = qService.getCqService();
      List<String> durableCqs = cqServiceForExec.getAllDurableClientCqs(id);

      ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
      chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
      chunkedResponseMsg.setTransactionId(msg.getTransactionId());
      chunkedResponseMsg.sendHeader();

      List durableCqList = new ArrayList(maximumChunkSize);
      final boolean isTraceEnabled = logger.isTraceEnabled();
      for (Iterator it = durableCqs.iterator(); it.hasNext();) {
        Object durableCqName = it.next();
        durableCqList.add(durableCqName);
        if (isTraceEnabled) {
          logger.trace("{}: getDurableCqsResponse <{}>; list size was {}", servConn.getName(), durableCqName, durableCqList.size());
        }
        if (durableCqList.size() == maximumChunkSize) {
          // Send the chunk and clear the list
          sendDurableCqsResponseChunk(durableCqList, false, servConn);
          durableCqList.clear();
        }
      }
      // Send the last chunk even if the list is of zero size.
      sendDurableCqsResponseChunk(durableCqList, true, servConn);

    } catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      return;
    } catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      return;
    }
  }

  private void sendDurableCqsResponseChunk(List list, boolean lastChunk,
      ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();

    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} durableCQs response chunk{}", servConn.getName(), (lastChunk ? " last " : " "), (logger.isTraceEnabled() ? " keys=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    chunkedResponseMsg.sendChunk(servConn);
  }


}
