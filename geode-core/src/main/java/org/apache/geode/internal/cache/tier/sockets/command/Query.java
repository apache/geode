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
/**
 * 
 */
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommandQuery;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;

public class Query extends BaseCommandQuery {

  private final static Query singleton = new Query();

  public static Command getCommand() {
    return singleton;
  }

  protected Query() {}

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {

    // Based on MessageType.DESTROY
    // Added by gregp 10/18/05
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    // Retrieve the data from the message parts
    String queryString = msg.getPart(0).getString();

    // this is optional part for message specific timeout, which right now send by native client
    // need to take care while adding new message

    if (msg.getNumberOfParts() == 3) {
      int timeout = msg.getPart(2).getInt();
      servConn.setRequestSpecificTimeout(timeout);
    }


    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received query request from {} queryString: {}", servConn.getName(),
          servConn.getSocketString(), queryString);
    }
    try {
      // Create query
      QueryService queryService =
          ((GemFireCacheImpl) servConn.getCachedRegionHelper().getCache()).getLocalQueryService();
      org.apache.geode.cache.query.Query query = queryService.newQuery(queryString);
      Set regionNames = ((DefaultQuery) query).getRegionsInQuery(null);

      // Authorization check
      QueryOperationContext queryContext = null;
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        queryContext = authzRequest.queryAuthorize(queryString, regionNames);
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

      processQuery(msg, query, queryString, regionNames, start, null, queryContext, servConn, true);
    } catch (QueryInvalidException e) {
      throw new QueryInvalidException(e.getMessage() + queryString);
    } catch (QueryExecutionLowMemoryException e) {
      writeQueryResponseException(msg, e, false, servConn);
    }
  }

  protected CollectionType getCollectionType(SelectResults selectResults) {
    return new CollectionTypeImpl(List.class, selectResults.getCollectionType().getElementType());
  }
}
