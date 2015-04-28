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

import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.cache.operations.QueryOperationContext;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommandQuery;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;

public class Query extends BaseCommandQuery {

  private final static Query singleton = new Query();

  public static Command getCommand() {
    return singleton;
  }

  private Query() {
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
    
    //this is optional part for message specific timeout, which right now send by native client
    //need to take care while adding new message
     
    if (msg.getNumberOfParts() == 3) {
      int timeout = msg.getPart(2).getInt();
      servConn.setRequestSpecificTimeout(timeout);
    }


    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received query request from {} queryString: {}", servConn.getName(), servConn.getSocketString(), queryString);
    }
    try {
    // Create query
      QueryService queryService = ((GemFireCacheImpl)servConn.getCachedRegionHelper().getCache())
        .getLocalQueryService();
    com.gemstone.gemfire.cache.query.Query query = queryService
        .newQuery(queryString);
    Set regionNames = ((DefaultQuery)query).getRegionsInQuery(null);

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
          regionNames = ((DefaultQuery)query).getRegionsInQuery(null);
        }
      }
    }

    processQuery(msg, query, queryString, regionNames, start, null,
        queryContext, servConn, true);
    } catch (QueryInvalidException e) {
      throw new QueryInvalidException(e.getMessage()
          + queryString );
    } catch (QueryExecutionLowMemoryException e) {
      writeQueryResponseException(msg, e, false, servConn);
    }
  }

}
