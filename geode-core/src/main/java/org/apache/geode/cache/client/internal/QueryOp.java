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

import java.util.Arrays;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.client.ServerOperationException;

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
    AbstractOp op = null;

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
  protected static class QueryOpImpl extends AbstractOp {
    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public QueryOpImpl(String queryPredicate) {
      super(MessageType.QUERY, 1);
      getMessage().addStringPart(queryPredicate);
    }

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public QueryOpImpl(String queryPredicate, Object[] queryParams) {
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
      ChunkHandler ch = new ChunkHandler() {
        public void handle(ChunkedMessage cm) throws Exception {
          Part collectionTypePart = cm.getPart(0);
          Object o = collectionTypePart.getObject();
          if (o instanceof Throwable) {
            String s = "While performing a remote " + getOpName();
            exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
            return;
          }
          CollectionType collectionType = (CollectionType) o;
          Part resultPart = cm.getPart(1);
          Object queryResult = null;
          try {
            queryResult = resultPart.getObject();
          } catch (Exception e) {
            String s = "While deserializing " + getOpName() + " result";
            exceptionRef[0] = new SerializationException(s, e);
            return;
          }
          if (queryResult instanceof Throwable) {
            String s = "While performing a remote " + getOpName();
            exceptionRef[0] = new ServerOperationException(s, (Throwable) queryResult);
            return;
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
              for (int i = 0; i < resultArray.length; i++) {
                if (isObjectPartList) {
                  selectResults.add(new StructImpl((StructTypeImpl) objectType,
                      ((ObjectPartList) resultArray[i]).getObjects().toArray()));
                } else {
                  selectResults
                      .add(new StructImpl((StructTypeImpl) objectType, (Object[]) resultArray[i]));
                }
              }
            } else {
              selectResults.addAll(Arrays.asList(resultArray));
            }
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
