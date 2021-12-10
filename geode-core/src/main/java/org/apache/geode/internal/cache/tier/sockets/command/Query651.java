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
import java.util.List;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommandQuery;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;

public class Query651 extends BaseCommandQuery {

  @Immutable
  private static final Query651 singleton = new Query651();

  public static Command getCommand() {
    return singleton;
  }

  protected Query651() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, InterruptedException {

    // Based on MessageType.DESTROY
    // Added by gregp 10/18/05
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    // Retrieve the data from the message parts
    String queryString = clientMessage.getPart(0).getString();
    long compiledQueryId = 0;
    Object[] queryParams = null;
    try {
      if (clientMessage.getMessageType() == MessageType.QUERY_WITH_PARAMETERS) {
        // Query with parameters supported from 6.6 onwards.
        int params = clientMessage.getPart(1).getInt(); // Number of parameters.
        // In case of native client there will be extra two parameters at 2 and 3 index.
        int paramStartIndex = 2;
        if (clientMessage
            .getNumberOfParts() > (1 /* type */ + 1 /* query string */ + 1 /* params length */
                + params /* number of params */)) {
          int timeout = clientMessage.getPart(3).getInt();
          serverConnection.setRequestSpecificTimeout(timeout);
          paramStartIndex = 4;
        }
        // Get the query execution parameters.
        queryParams = new Object[params];
        for (int i = 0; i < queryParams.length; i++) {
          queryParams[i] = clientMessage.getPart(i + paramStartIndex).getObject();
        }
      } else {
        // this is optional part for message specific timeout, which right now send by native client
        // need to take care while adding new message
        if (clientMessage.getNumberOfParts() == 3) {
          int timeout = clientMessage.getPart(2).getInt();
          serverConnection.setRequestSpecificTimeout(timeout);
        }
      }
    } catch (ClassNotFoundException cne) {
      throw new QueryInvalidException(cne.getMessage() + queryString);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received query request from {} queryString: {}{}",
          serverConnection.getName(), serverConnection.getSocketString(), queryString,
          (queryParams != null ? (" with num query parameters :" + queryParams.length) : ""));
    }
    try {
      // Create query
      QueryService queryService =
          serverConnection.getCachedRegionHelper().getCache().getLocalQueryService();
      org.apache.geode.cache.query.Query query = null;

      if (queryParams != null) {
        // Its a compiled query.
        CacheClientNotifier ccn = serverConnection.getAcceptor().getCacheClientNotifier();
        query = ccn.getCompiledQuery(queryString);
        if (query == null) {
          // This is first time the query is seen by this server.
          query = queryService.newQuery(queryString);
          ccn.addCompiledQuery((DefaultQuery) query);
        }
        ccn.getStats().incCompiledQueryUsedCount(1);
        ((DefaultQuery) query).setLastUsed(true);
      } else {
        query = queryService.newQuery(queryString);
      }

      Set regionNames = ((DefaultQuery) query).getRegionsInQuery(queryParams);

      // Authorization check
      QueryOperationContext queryContext = null;
      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        queryContext = authzRequest.queryAuthorize(queryString, regionNames, queryParams);
        String newQueryString = queryContext.getQuery();
        if (queryString != null && !queryString.equals(newQueryString)) {
          query = queryService.newQuery(newQueryString);
          queryString = newQueryString;
          regionNames = queryContext.getRegionNames();
          if (regionNames == null) {
            regionNames = ((DefaultQuery) query).getRegionsInQuery(null);
          }
        }
      }

      processQueryUsingParams(clientMessage, query, queryString, regionNames, start, null,
          queryContext, serverConnection, true, queryParams, securityService);
    } catch (QueryInvalidException e) {
      throw new QueryInvalidException(e.getMessage() + queryString);
    }
  }

  @Override
  protected CollectionType getCollectionType(SelectResults selectResults) {
    return new CollectionTypeImpl(List.class, selectResults.getCollectionType().getElementType());
  }
}
