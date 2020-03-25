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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Does a region getAll on a server
 *
 * @since GemFire 5.7
 */
public class GetAllOp {

  private static final Logger logger = LogService.getLogger();

  /**
   * Does a region getAll on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the getAll on
   * @param keys list of keys to get
   * @return the map of values found by the getAll if any
   */
  public static VersionedObjectList execute(ExecutablePool pool, String region, List keys,
      Object callback) {
    AbstractOp op = new GetAllOpImpl(region, keys, callback);
    op.initMessagePart();
    return ((VersionedObjectList) pool.execute(op)).setKeys(keys);
  }

  public static VersionedObjectList execute(ExecutablePool pool, Region region, List keys,
      int retryAttempts, Object callback) {
    AbstractOp op = new GetAllOpImpl(region.getFullPath(), keys, callback);
    ClientMetadataService cms = ((InternalRegion) region).getCache().getClientMetadataService();

    Map<ServerLocation, Set> serverToFilterMap = cms.getServerToFilterMap(keys, region, true);

    if (serverToFilterMap == null || serverToFilterMap.isEmpty()) {
      op.initMessagePart();
      return ((VersionedObjectList) pool.execute(op)).setKeys(keys);
    } else {
      VersionedObjectList result = null;
      ServerConnectivityException se = null;
      List retryList = new ArrayList();
      List callableTasks =
          constructGetAllTasks(region.getFullPath(), serverToFilterMap, (PoolImpl) pool, callback);
      Map<ServerLocation, Object> results =
          SingleHopClientExecutor.submitGetAll(serverToFilterMap,
              callableTasks, cms, (LocalRegion) region);
      for (ServerLocation server : results.keySet()) {
        Object serverResult = results.get(server);
        if (serverResult instanceof ServerConnectivityException) {
          se = (ServerConnectivityException) serverResult;
          retryList.addAll(serverToFilterMap.get(server));
        } else {
          if (result == null) {
            result = (VersionedObjectList) serverResult;
          } else {
            result.addAll((VersionedObjectList) serverResult);
          }
        }
      }

      if (se != null) {
        if (retryAttempts == 0) {
          throw se;
        } else {
          VersionedObjectList retryResult =
              GetAllOp.execute(pool, region.getFullPath(), retryList, callback);
          if (result == null) {
            result = retryResult;
          } else {
            result.addAll(retryResult);
          }
        }
      }

      return result;
    }
  }

  private GetAllOp() {
    // no instances allowed
  }

  static List constructGetAllTasks(String region,
      final Map<ServerLocation, Set> serverToFilterMap, final PoolImpl pool,
      final Object callback) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<>();
    ArrayList<ServerLocation> servers = new ArrayList<>(serverToFilterMap.keySet());

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing tasks for the servers {}", servers);
    }
    for (ServerLocation server : servers) {
      Set filterSet = serverToFilterMap.get(server);
      AbstractOp getAllOp = new GetAllOpImpl(region, new ArrayList(filterSet), callback);

      SingleHopOperationCallable task =
          new SingleHopOperationCallable(new ServerLocation(server.getHostName(), server.getPort()),
              pool, getAllOp, UserAttributes.userAttributes.get());
      tasks.add(task);
    }
    return tasks;
  }

  static class GetAllOpImpl extends AbstractOp {

    private final List keyList;
    private final Object callback;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public GetAllOpImpl(String region, List keys, Object callback) {
      super(callback != null ? MessageType.GET_ALL_WITH_CALLBACK : MessageType.GET_ALL_70, 3);
      this.keyList = keys;
      this.callback = callback;
      getMessage().addStringPart(region, true);
    }

    @Override
    protected void initMessagePart() {
      Object[] keysArray = new Object[this.keyList.size()];
      this.keyList.toArray(keysArray);
      getMessage().addObjPart(keysArray);
      if (this.callback != null) {
        getMessage().addObjPart(this.callback);
      } else {
        // using the old GET_ALL_70 command that expects an int saying we are not register interest
        getMessage().addIntPart(0);
      }
    }

    public List getKeyList() {
      return this.keyList;
    }


    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Object processResponse(Message msg, final Connection con) throws Exception {
      final VersionedObjectList result = new VersionedObjectList(false);
      final Exception[] exceptionRef = new Exception[1];
      processChunkedResponse((ChunkedMessage) msg, "getAll", new ChunkHandler() {
        @Override
        public void handle(ChunkedMessage cm) throws Exception {
          Part part = cm.getPart(0);
          try {
            Object o = part.getObject();
            if (o instanceof Throwable) {
              String s = "While performing a remote getAll";
              exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
            } else {
              VersionedObjectList chunk = (VersionedObjectList) o;
              chunk.replaceNullIDs(con.getEndpoint().getMemberId());
              result.addAll(chunk);
            }
          } catch (Exception e) {
            exceptionRef[0] = new ServerOperationException("Unable to deserialize value", e);
          }
        }
      });
      if (exceptionRef[0] != null) {
        throw exceptionRef[0];
      } else {
        return result;
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.GET_ALL_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetAll();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetAllSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetAll(start, hasTimedOut(), hasFailed());
    }
  }
}
