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
import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class GetDurableCQs extends BaseCQCommand {

  private static final GetDurableCQs singleton = new GetDurableCQs();

  public static Command getCommand() {
    return singleton;
  }

  private GetDurableCQs() {
    // nothing
  }

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, InterruptedException {
    Acceptor acceptor = serverConnection.getAcceptor();
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    ClientProxyMembershipID id = serverConnection.getProxyID();
    CacheServerStats stats = serverConnection.getCacheServerStats();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received {} request from {}", serverConnection.getName(),
          MessageType.getString(clientMessage.getMessageType()),
          serverConnection.getSocketString());
    }

    try {
      DefaultQueryService qService =
          (DefaultQueryService) crHelper.getCache().getLocalQueryService();

      securityService.authorize(Resource.CLUSTER, Operation.READ);

      // Authorization check
      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        authzRequest.getDurableCQsAuthorize();
      }

      CqService cqServiceForExec = qService.getCqService();
      List<String> durableCqs = cqServiceForExec.getAllDurableClientCqs(id);

      ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
      chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
      chunkedResponseMsg.setTransactionId(clientMessage.getTransactionId());
      chunkedResponseMsg.sendHeader();

      List durableCqList = new ArrayList(MAXIMUM_CHUNK_SIZE);
      final boolean isTraceEnabled = logger.isTraceEnabled();
      for (String durableCqName : durableCqs) {
        durableCqList.add(durableCqName);
        if (isTraceEnabled) {
          logger.trace("{}: getDurableCqsResponse <{}>; list size was {}",
              serverConnection.getName(), durableCqName, durableCqList.size());
        }
        if (durableCqList.size() == MAXIMUM_CHUNK_SIZE) {
          // Send the chunk and clear the list
          sendDurableCqsResponseChunk(durableCqList, false, serverConnection);
          durableCqList.clear();
        }
      }
      // Send the last chunk even if the list is of zero size.
      sendDurableCqsResponseChunk(durableCqList, true, serverConnection);

    } catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", clientMessage.getTransactionId(), cqe,
          serverConnection);
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
    }
  }

  private void sendDurableCqsResponseChunk(List list, boolean lastChunk, ServerConnection servConn)
      throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();

    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, false);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} durableCQs response chunk{}", servConn.getName(),
          lastChunk ? " last " : " ",
          logger.isTraceEnabled() ? " keys=" + list + " chunk=<" + chunkedResponseMsg + ">" : "");
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
