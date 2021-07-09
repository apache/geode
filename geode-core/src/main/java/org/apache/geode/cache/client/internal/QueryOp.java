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
package org.apache.geode.cache.client.internal;

import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Does a region query on a server
 *
 * @since GemFire 5.7
 */
public class QueryOp {
  /**
   * Does a region query on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param queryPredicate A query language boolean query predicate
   * @return A <code>SelectResults</code> containing the values that match the
   *         <code>queryPredicate</code>.
   */
  public static SelectResults execute(ExecutablePool pool, String queryPredicate,
      Object[] queryParams) {
    AbstractOp op;

    if (queryParams != null && queryParams.length > 0) {
      op = new QueryOpImpl(queryPredicate, queryParams);
    } else {
      op = new QueryOpImpl(queryPredicate);
    }
    return (SelectResults) pool.execute(op);
  }

  private QueryOp() {
    // no instances allowed
  }

  /**
   * Note: this class is extended by CreateCQWithIROpImpl.
   */
  public static class QueryOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    QueryOpImpl(String queryPredicate) {
      super(MessageType.QUERY, 1);
      getMessage().addStringPart(queryPredicate);
    }

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    QueryOpImpl(String queryPredicate, Object[] queryParams) {
      super(MessageType.QUERY_WITH_PARAMETERS, 2 + queryParams.length);
      getMessage().addStringPart(queryPredicate);
      getMessage().addIntPart(queryParams.length);
      for (Object param : queryParams) {
        getMessage().addObjPart(param);
      }
    }

    /**
     * This constructor is used by our subclass CreateCQWithIROpImpl
     *
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    protected QueryOpImpl(int msgType, int numParts) {
      super(msgType, numParts);
    }

    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(2, Version.CURRENT);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      final SelectResults[] resultRef = new SelectResults[1];
      final Exception[] exceptionRef = new Exception[1];
      ChunkHandler ch = cm -> {
        Part collectionTypePart = cm.getPart(0);
        Object o = collectionTypePart.getObject();
        if (o instanceof Throwable) {
          String s = "While performing a remote " + getOpName();
          exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
          return;
        }
        CollectionType collectionType = (CollectionType) o;
        Part resultPart = cm.getPart(1);
        Object queryResult;
        try {
          queryResult = resultPart.getObject();
        } catch (Exception e) {
          String s = "While deserializing " + getOpName() + " result";

          // Enable the workaround to convert PdxSerializationException into IOException to retry.
          // It only worked when the client is configured to connect to more than one cache server
          // AND the pool's "retry-attempts" is -1 (the default which means try each server) or > 0.
          // It is possible that if application closed the current connection and got a new
          // connection to the same server and retried the query to it, that it would also
          // workaround this issue and it would not have the limitations of needing multiple servers
          // and would not depend on the retry-attempts configuration.
          boolean enableQueryRetryOnPdxSerializationException = Boolean.getBoolean(
              GeodeGlossary.GEMFIRE_PREFIX + "enableQueryRetryOnPdxSerializationException");
          if (e instanceof PdxSerializationException
              && enableQueryRetryOnPdxSerializationException) {
            // IOException will allow the client to retry next server in the connection pool until
            // exhausted all the servers (so it will not retry forever). Why retry:
            // The byte array of the pdxInstance is always the same at the server. Other clients can
            // get a correct one from query response message. Even this client can get it correctly
            // before and after the PdxSerializationException.
            exceptionRef[0] = new IOException(s, e);
            LogService.getLogger().warn(
                "Encountered unexpected PdxSerializationException, retrying on another server");
          } else {
            exceptionRef[0] = new SerializationException(s, e);
          }
          return;
        }
        if (queryResult instanceof Throwable) {
          String s = "While performing a remote " + getOpName();
          exceptionRef[0] = new ServerOperationException(s, (Throwable) queryResult);
        } else if (queryResult instanceof Integer) {
          // Create the appropriate SelectResults instance if necessary
          if (resultRef[0] == null) {
            resultRef[0] = QueryUtils.getEmptySelectResults(TypeUtils.OBJECT_TYPE, null);
          }
          resultRef[0].add(queryResult);
        } else { // typical query result
          // Create the appropriate SelectResults instance if necessary
          if (resultRef[0] == null) {
            resultRef[0] = QueryUtils.getEmptySelectResults(collectionType, null);
          }
          SelectResults selectResults = resultRef[0];
          ObjectType objectType = collectionType.getElementType();
          Object[] resultArray;
          // for select * queries, the serialized object byte arrays are
          // returned as part of ObjectPartList
          boolean isObjectPartList = false;
          if (queryResult instanceof ObjectPartList) {
            isObjectPartList = true;
            resultArray = ((ObjectPartList) queryResult).getObjects().toArray();
          } else {
            // Add the results to the SelectResults
            resultArray = (Object[]) queryResult;
          }
          if (objectType.isStructType()) {
            for (Object value : resultArray) {
              if (isObjectPartList) {
                selectResults.add(new StructImpl((StructTypeImpl) objectType,
                    ((ObjectPartList) value).getObjects().toArray()));
              } else {
                selectResults
                    .add(new StructImpl((StructTypeImpl) objectType, (Object[]) value));
              }
            }
          } else {
            selectResults.addAll(Arrays.asList(resultArray));
          }
        }
      };
      processChunkedResponse((ChunkedMessage) msg, getOpName(), ch);
      if (exceptionRef[0] != null) {
        throw exceptionRef[0];
      } else {
        return resultRef[0];
      }
    }

    protected String getOpName() {
      return "query";
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.QUERY_DATA_ERROR || msgType == MessageType.CQDATAERROR_MSG_TYPE
          || msgType == MessageType.CQ_EXCEPTION_TYPE;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startQuery();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endQuerySend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endQuery(start, hasTimedOut(), hasFailed());
    }
  }
}
