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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.cache.operations.QueryOperationContext;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import java.io.IOException;
import java.util.Set;

public class Query651 extends BaseCommandQuery {

  private final static Query651 singleton = new Query651();

  public static Command getCommand() {
    return singleton;
  }

  private Query651() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {

    // Based on MessageType.DESTROY
    // Added by gregp 10/18/05
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    // Retrieve the data from the message parts
    String queryString = msg.getPart(0).getString();
    long compiledQueryId = 0;
    Object[] queryParams = null;
    try {
      if (msg.getMessageType() == MessageType.QUERY_WITH_PARAMETERS) {
        // Query with parameters supported from 6.6 onwards.
        int params = msg.getPart(1).getInt(); // Number of parameters.
        // In case of native client there will be extra two parameters at 2 and 3 index.
        int paramStartIndex = 2;
        if (msg.getNumberOfParts() > (1 /* type */ + 1 /* query string */ + 
            1 /* params length */ + params /* number of params*/)) {
          int timeout = msg.getPart(3).getInt();
          servConn.setRequestSpecificTimeout(timeout);
          paramStartIndex = 4;  
        }
        // Get the query execution parameters.
        queryParams = new Object[params];
        for (int i=0; i < queryParams.length; i++) {
          queryParams[i] = msg.getPart(i + paramStartIndex).getObject();
        }
      } else {
        //this is optional part for message specific timeout, which right now send by native client
        //need to take care while adding new message
        if (msg.getNumberOfParts() == 3) {
          int timeout = msg.getPart(2).getInt();
          servConn.setRequestSpecificTimeout(timeout);
        } 
      }
    } catch (ClassNotFoundException cne) {
      throw new QueryInvalidException(cne.getMessage()
          + queryString);
    }
      
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received query request from {} queryString: {}{}", servConn.getName(), servConn.getSocketString(), queryString, (queryParams != null ? (" with num query parameters :" + queryParams.length):""));
    }
    try {
      // Create query
      QueryService queryService = ((GemFireCacheImpl)servConn.getCachedRegionHelper().getCache())
      .getLocalQueryService();
      com.gemstone.gemfire.cache.query.Query query = null;

      if (queryParams != null){
        // Its a compiled query.
        CacheClientNotifier ccn = servConn.getAcceptor().getCacheClientNotifier();
        query = ccn.getCompiledQuery(queryString);
        if (query == null) {
          // This is first time the query is seen by this server.
          query = queryService.newQuery(queryString);
          ccn.addCompiledQuery((DefaultQuery)query);
        }
        ccn.getStats().incCompiledQueryUsedCount(1);
        ((DefaultQuery)query).setLastUsed(true);
      } else {
        query = queryService.newQuery(queryString);
      }

      Set regionNames = ((DefaultQuery)query).getRegionsInQuery(queryParams);

      // Authorization check
      QueryOperationContext queryContext = null;
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        queryContext = authzRequest.queryAuthorize(queryString, regionNames, queryParams);
        String newQueryString = queryContext.getQuery();
        if (queryString != null && !queryString.equals(newQueryString)) {
          query = queryService.newQuery(newQueryString);
          queryString = newQueryString;
          regionNames = queryContext.getRegionNames();
          if (regionNames == null) {
            regionNames = ((DefaultQuery)query).getRegionsInQuery(null);
          }
        }
      }

      processQueryUsingParams(msg, query, queryString, regionNames, start, null,
          queryContext, servConn, true, queryParams);
    } catch (QueryInvalidException e) {
      throw new QueryInvalidException(e.getMessage()
          + queryString );
    }
  }

}
