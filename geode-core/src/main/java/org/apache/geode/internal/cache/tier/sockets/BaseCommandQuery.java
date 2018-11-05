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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public abstract class BaseCommandQuery extends BaseCommand {

  /**
   * Process the give query and sends the resulset back to the client.
   *
   * @return true if successful execution false in case of failure.
   */
  protected boolean processQuery(Message msg, Query query, String queryString, Set regionNames,
      long start, ServerCQ cqQuery, QueryOperationContext queryContext, ServerConnection servConn,
      boolean sendResults, final SecurityService securityService)
      throws IOException, InterruptedException {
    return processQueryUsingParams(msg, query, queryString, regionNames, start, cqQuery,
        queryContext, servConn, sendResults, null, securityService);
  }

  /**
   * Process the give query and sends the resulset back to the client.
   *
   * @return true if successful execution false in case of failure.
   */
  protected boolean processQueryUsingParams(Message msg, Query query, String queryString,
      Set regionNames, long start, ServerCQ cqQuery, QueryOperationContext queryContext,
      ServerConnection servConn, boolean sendResults, Object[] params,
      final SecurityService securityService) throws IOException, InterruptedException {
    ChunkedMessage queryResponseMsg = servConn.getQueryResponseMessage();
    CacheServerStats stats = servConn.getCacheServerStats();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadQueryRequestTime(start - oldStart);
    }

    // from 7.0, set flag to indicate a remote query irrespective of the
    // object type
    if (servConn.getClientVersion().compareTo(Version.GFE_70) >= 0) {
      ((DefaultQuery) query).setRemoteQuery(true);
    }
    // Process the query request
    try {
      // integrated security
      for (Object regionName : regionNames) {
        securityService.authorize(Resource.DATA, Operation.READ, regionName.toString());
      }

      // Execute query
      // startTime = GenericStats.getTime();
      // startTime = System.currentTimeMillis();

      // For now we assume the results are a SelectResults
      // which is the only possibility now, but this may change
      // in the future if we support arbitrary queries
      Object result = null;

      if (params != null) {
        result = query.execute(params);
      } else {
        result = query.execute();
      }

      // Asif : Before conditioning the results check if any
      // of the regions involved in the query have been destroyed
      // or not. If yes, throw an Exception.
      // This is a workaround/fix for Bug 36969
      Iterator itr = regionNames.iterator();
      while (itr.hasNext()) {
        String regionName = (String) itr.next();
        if (crHelper.getRegion(regionName) == null) {
          throw new RegionDestroyedException(
              "Region destroyed during the execution of the query",
              regionName);
        }
      }
      AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
      if (postAuthzRequest != null) {
        if (cqQuery == null) {
          queryContext = postAuthzRequest.queryAuthorize(queryString, regionNames, result,
              queryContext, params);
        } else {
          queryContext = postAuthzRequest.executeCQAuthorize(cqQuery.getName(), queryString,
              regionNames, result, queryContext);
        }
        result = queryContext.getQueryResult();
      }

      if (result instanceof SelectResults) {
        SelectResults selectResults = (SelectResults) result;

        if (logger.isDebugEnabled()) {
          logger.debug("Query Result size for : {} is {}", query.getQueryString(),
              selectResults.size());
        }

        CollectionType collectionType = null;
        boolean sendCqResultsWithKey = true;
        boolean isStructs = false;

        // check if resultset has serialized objects, so that they could be sent
        // as ObjectPartList
        boolean hasSerializedObjects = ((DefaultQuery) query).isKeepSerialized();
        if (logger.isDebugEnabled()) {
          logger.debug("Query Result for :{} has serialized objects: {}", query.getQueryString(),
              hasSerializedObjects);
        }
        // Don't convert to a Set, there might be duplicates now
        // The results in a StructSet are stored in Object[]s
        // Get them as Object[]s for the objs[] in order to avoid duplicating
        // the StructTypes

        // Object[] objs = new Object[selectResults.size()];
        // Get the collection type (which includes the element type)
        // (used to generate the appropriate instance on the client)

        // Get the collection type (which includes the element type)
        // (used to generate the appropriate instance on the client)
        collectionType = getCollectionType(selectResults);
        isStructs = collectionType.getElementType().isStructType();

        // Check if the Query is from CQ execution.
        if (cqQuery != null) {
          // Check if the key can be sent to the client based on its version.
          sendCqResultsWithKey = sendCqResultsWithKey(servConn);

          if (sendCqResultsWithKey) {
            // Update the collection type to include key info.
            collectionType = new CollectionTypeImpl(Collection.class,
                new StructTypeImpl(new String[] {"key", "value"}));
            isStructs = collectionType.getElementType().isStructType();
          }
        }

        int numberOfChunks = (int) Math.ceil(selectResults.size() * 1.0 / MAXIMUM_CHUNK_SIZE);

        if (logger.isTraceEnabled()) {
          logger.trace("{}: Query results size: {}: Entries in chunk: {}: Number of chunks: {}",
              servConn.getName(), selectResults.size(), MAXIMUM_CHUNK_SIZE, numberOfChunks);
        }

        long oldStart = start;
        start = DistributionStats.getStatTime();
        stats.incProcessQueryTime(start - oldStart);

        if (sendResults) {
          queryResponseMsg.setMessageType(MessageType.RESPONSE);
          queryResponseMsg.setTransactionId(msg.getTransactionId());
          queryResponseMsg.sendHeader();
        }

        if (sendResults && numberOfChunks == 0) {
          // Send 1 empty chunk
          if (logger.isTraceEnabled()) {
            logger.trace("{}: Creating chunk: 0", servConn.getName());
          }
          writeQueryResponseChunk(new Object[0], collectionType, true, servConn);
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Sent chunk (1 of 1) of query response for query {}",
                servConn.getName(), queryString);
          }
        } else {
          // Send response to client.
          // from 7.0, if the object is in the form of serialized byte array,
          // send it as a part of ObjectPartList
          if (hasSerializedObjects) {
            sendResultsAsObjectPartList(numberOfChunks, servConn, selectResults.asList(), isStructs,
                collectionType, queryString, cqQuery, sendCqResultsWithKey, sendResults,
                securityService);
          } else {
            sendResultsAsObjectArray(selectResults, numberOfChunks, servConn, isStructs,
                collectionType, queryString, cqQuery, sendCqResultsWithKey, sendResults);
          }
        }

        if (cqQuery != null) {
          // Set the CQ query result cache initialized flag.
          cqQuery.setCqResultsCacheInitialized();
        }

      } else if (result instanceof Integer) {
        if (sendResults) {
          queryResponseMsg.setMessageType(MessageType.RESPONSE);
          queryResponseMsg.setTransactionId(msg.getTransactionId());
          queryResponseMsg.sendHeader();
          writeQueryResponseChunk(result, null, true, servConn);
        }
      } else {
        throw new QueryInvalidException(String.format("Unknown result type: %s",
            result.getClass()));
      }
      msg.clearParts();
    } catch (QueryInvalidException e) {
      // Handle this exception differently since it can contain
      // non-serializable objects.
      // java.io.NotSerializableException: antlr.CommonToken
      // Log a warning to show stack trace and create a new
      // QueryInvalidEsception on the original one's message (not cause).
      logger.warn(String.format("Unexpected QueryInvalidException while processing query %s",
          queryString),
          e);
      QueryInvalidException qie =
          new QueryInvalidException(String.format("%s : QueryString is: %s.",
              new Object[] {e.getLocalizedMessage(), queryString}));
      writeQueryResponseException(msg, qie, servConn);
      return false;
    } catch (DistributedSystemDisconnectedException se) {
      if (msg != null && logger.isDebugEnabled()) {
        logger.debug(
            "{}: ignoring message of type {} from client {} because shutdown occurred during message processing.",
            servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getProxyID());
      }
      servConn.setFlagProcessMessagesAsFalse();
      servConn.setClientDisconnectedException(se);
      return false;
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, e);
      // Otherwise, write a query response and continue
      // Check if query got canceled from QueryMonitor.
      DefaultQuery defaultQuery = (DefaultQuery) query;
      if ((defaultQuery).isCanceled()) {
        e = new QueryException(defaultQuery.getQueryCanceledException().getMessage(),
            e.getCause());
      }
      writeQueryResponseException(msg, e, servConn);
      return false;
    } finally {
      // Since the query object is being shared in case of bind queries,
      // resetting the flag may cause inconsistency.
      // Also since this flag is only being set in code path executed by
      // remote query execution, resetting it is not required.

      // ((DefaultQuery)query).setRemoteQuery(false);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent query response for query {}", servConn.getName(), queryString);
    }

    stats.incWriteQueryResponseTime(DistributionStats.getStatTime() - start);
    return true;
  }

  protected CollectionType getCollectionType(SelectResults results) {
    return results.getCollectionType();
  }

  private boolean sendCqResultsWithKey(ServerConnection servConn) {
    Version clientVersion = servConn.getClientVersion();
    if (clientVersion.compareTo(Version.GFE_65) >= 0) {
      return true;
    }
    return false;
  }

  protected void sendCqResponse(int msgType, String msgStr, int txId, Throwable e,
      ServerConnection servConn) throws IOException {
    ChunkedMessage cqMsg = servConn.getChunkedResponseMessage();
    if (logger.isDebugEnabled()) {
      logger.debug("CQ Response message :{}", msgStr);
    }

    switch (msgType) {
      case MessageType.REPLY:
        cqMsg.setNumberOfParts(1);
        break;

      case MessageType.CQDATAERROR_MSG_TYPE:
        logger.warn(msgStr);
        cqMsg.setNumberOfParts(1);
        break;

      case MessageType.CQ_EXCEPTION_TYPE:
        String exMsg = "";
        if (e != null) {
          exMsg = e.getLocalizedMessage();
        }
        logger.info(msgStr + exMsg, e);

        msgStr += exMsg; // fixes bug 42309

        cqMsg.setNumberOfParts(1);
        break;

      default:
        msgType = MessageType.CQ_EXCEPTION_TYPE;
        cqMsg.setNumberOfParts(1);
        msgStr += "Uknown query Exception.";
        break;
    }

    cqMsg.setMessageType(msgType);
    cqMsg.setTransactionId(txId);
    cqMsg.sendHeader();
    cqMsg.addStringPart(msgStr);
    cqMsg.setLastChunk(true);
    cqMsg.sendChunk(servConn);
    cqMsg.setLastChunk(true);

    if (logger.isDebugEnabled()) {
      logger.debug("CQ Response sent successfully");
    }
  }

  private void sendResultsAsObjectArray(SelectResults selectResults, int numberOfChunks,
      ServerConnection servConn, boolean isStructs, CollectionType collectionType,
      String queryString, ServerCQ cqQuery, boolean sendCqResultsWithKey, boolean sendResults)
      throws IOException {
    int resultIndex = 0;
    // For CQ only as we dont want CQEntries which have null values.
    int cqResultIndex = 0;
    Object[] objs = selectResults.toArray();
    for (int j = 0; j < numberOfChunks; j++) {
      boolean incompleteArray = false;
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Creating chunk: {}", servConn.getName(), j);
      }
      Object[] results = new Object[MAXIMUM_CHUNK_SIZE];
      for (int i = 0; i < MAXIMUM_CHUNK_SIZE; i++) {
        if ((resultIndex) == objs.length) {
          incompleteArray = true;
          break;
        }
        if (logger.isTraceEnabled()) {
          logger.trace("{}: Adding entry [{}] to query results: {}", servConn.getName(),
              resultIndex, objs[resultIndex]);
        }
        if (cqQuery != null) {
          CqEntry e = (CqEntry) objs[resultIndex];
          // The value may have become null because of entry invalidation.
          if (e.getValue() == null) {
            resultIndex++;
            // i will get incremented anyway so we need to decrement it back so
            // that results[i] is not null.
            i--;
            continue;
          }
          // Add the key into CQ results cache.
          // For PR the Result caching is not yet supported.
          // cqQuery.cqResultsCacheInitialized is added to take care
          // of CQ execute requests that are re-sent. In that case no
          // need to update the Results cache.
          if (!cqQuery.isPR()) {
            cqQuery.addToCqResultKeys(e.getKey());
          }

          // Add to the Results object array.
          if (sendCqResultsWithKey) {
            results[i] = e.getKeyValuePair();
          } else {
            results[i] = e.getValue();
          }
        } else {
          // instance check added to fix bug 40516.
          if (isStructs && (objs[resultIndex] instanceof Struct)) {
            results[i] = ((Struct) objs[resultIndex]).getFieldValues();
          } else {
            results[i] = objs[resultIndex];
          }
        }
        resultIndex++;
        cqResultIndex++;
      }
      // Shrink array if necessary. This will occur if the number
      // of entries in the chunk does not divide evenly into the
      // number of entries in the result set.
      if (incompleteArray) {
        Object[] newResults;
        if (cqQuery != null) {
          newResults = new Object[cqResultIndex % MAXIMUM_CHUNK_SIZE];
        } else {
          newResults = new Object[resultIndex % MAXIMUM_CHUNK_SIZE];
        }
        for (int i = 0; i < newResults.length; i++) {
          newResults[i] = results[i];
        }
        results = newResults;
      }

      if (sendResults) {
        writeQueryResponseChunk(results, collectionType, (resultIndex == objs.length),
            servConn);

        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sent chunk ({} of {}) of query response for query: {}",
              servConn.getName(), (j + 1), numberOfChunks, queryString);
        }
      }
      // If we have reached the last element of SelectResults then we should
      // break out of loop here only.
      if (resultIndex == objs.length) {
        break;
      }
    }
  }

  private void sendResultsAsObjectPartList(int numberOfChunks, ServerConnection servConn, List objs,
      boolean isStructs, CollectionType collectionType, String queryString, ServerCQ cqQuery,
      boolean sendCqResultsWithKey, boolean sendResults, final SecurityService securityService)
      throws IOException {
    int resultIndex = 0;
    Object result = null;
    for (int j = 0; j < numberOfChunks; j++) {
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Creating chunk: {}", servConn.getName(), j);
      }
      ObjectPartList serializedObjs = new ObjectPartList(MAXIMUM_CHUNK_SIZE, false);
      for (int i = 0; i < MAXIMUM_CHUNK_SIZE; i++) {
        if ((resultIndex) == objs.size()) {
          break;
        }
        if (logger.isTraceEnabled()) {
          logger.trace("{}: Adding entry [{}] to query results: {}", servConn.getName(),
              resultIndex, objs.get(resultIndex));
        }
        if (cqQuery != null) {
          CqEntry e = (CqEntry) objs.get(resultIndex);
          // The value may have become null because of entry invalidation.
          if (e.getValue() == null) {
            resultIndex++;
            continue;
          }

          // Add the key into CQ results cache.
          // For PR the Result caching is not yet supported.
          // cqQuery.cqResultsCacheInitialized is added to take care
          // of CQ execute requests that are re-sent. In that case no
          // need to update the Results cache.
          if (!cqQuery.isPR()) {
            cqQuery.addToCqResultKeys(e.getKey());
          }

          // Add to the Results object array.
          if (sendCqResultsWithKey) {
            result = e.getKeyValuePair();
          } else {
            result = e.getValue();
          }
        } else {
          result = objs.get(resultIndex);
        }
        if (sendResults) {
          addToObjectPartList(serializedObjs, result, collectionType, false, servConn, isStructs,
              securityService);
        }
        resultIndex++;
      }

      if (sendResults) {
        writeQueryResponseChunk(serializedObjs, collectionType, ((j + 1) == numberOfChunks),
            servConn);

        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sent chunk ({} of {}) of query response for query: {}",
              servConn.getName(), (j + 1), numberOfChunks, queryString);
        }
      }
    }
  }

  private void addToObjectPartList(ObjectPartList serializedObjs, Object res,
      CollectionType collectionType, boolean lastChunk, ServerConnection servConn,
      boolean isStructs, final SecurityService securityService) throws IOException {
    if (isStructs && (res instanceof Struct)) {
      Object[] values = ((Struct) res).getFieldValues();
      // create another ObjectPartList for the struct
      ObjectPartList serializedValueObjs = new ObjectPartList(values.length, false);
      for (Object value : values) {
        addObjectToPartList(serializedValueObjs, null, value, securityService);
      }
      serializedObjs.addPart(null, serializedValueObjs, ObjectPartList.OBJECT, null);
    } else if (res instanceof Object[]) {// for CQ key-value pairs
      Object[] values = ((Object[]) res);
      // create another ObjectPartList for the Object[]
      ObjectPartList serializedValueObjs = new ObjectPartList(values.length, false);
      for (int i = 0; i < values.length; i += 2) {
        Object key = values[i];
        Object value = values[i + 1];
        addObjectToPartList(serializedValueObjs, key, value, securityService);
      }
      serializedObjs.addPart(null, serializedValueObjs, ObjectPartList.OBJECT, null);
    } else { // for deserialized objects
      addObjectToPartList(serializedObjs, null, res, securityService);
    }
  }

  private void addObjectToPartList(ObjectPartList objPartList, Object key, Object value,
      final SecurityService securityService) {
    Object object = value;
    boolean isObject = true;
    if (value instanceof CachedDeserializable) {
      object = ((CachedDeserializable) value).getSerializedValue();
    } else if (value instanceof byte[]) {
      isObject = false;
    }

    object = securityService.postProcess(null, key, object, isObject);
    if (key != null) {
      objPartList.addPart(null, key, ObjectPartList.OBJECT, null);
    }
    objPartList.addPart(null, object, isObject ? ObjectPartList.OBJECT : ObjectPartList.BYTES,
        null);
  }

}
