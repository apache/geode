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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.PutAllPartialResultException.PutAllPartialResult;
import org.apache.geode.internal.cache.RegionMapOwner;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Does a region putAll on a server
 *
 * @since GemFire 5.7
 */
public class PutAllOp {

  private static final Logger logger = LogService.getLogger();

  public static final int FLAG_EMPTY = 0x01;
  public static final int FLAG_CONCURRENCY_CHECKS = 0x02;

  /**
   * Does a region put on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the putAll on
   * @param map the Map of keys and values to put
   * @param eventId the event id for this putAll
   * @param skipCallbacks true if no callbacks will be invoked
   */
  public static VersionedObjectList execute(ExecutablePool pool, Region region, Map map,
      EventID eventId, boolean skipCallbacks, boolean isRetry, Object callbackArg) {
    PutAllOpImpl op = new PutAllOpImpl(region, map, eventId,
        ((Pool) pool).getPRSingleHopEnabled(), skipCallbacks, callbackArg);
    op.initMessagePart();
    if (isRetry) {
      op.getMessage().setIsRetry();
    }
    return (VersionedObjectList) pool.execute(op);
  }

  /**
   * Does a region put on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the putAll on
   * @param map the Map of keys and values to put
   * @param eventId the event id for this putAll
   */
  public static VersionedObjectList execute(ExecutablePool pool, Region<Object, Object> region,
      Map<Object, Object> map, EventID eventId, boolean skipCallbacks, int retryAttempts,
      Object callbackArg) {
    ClientMetadataService clientMetadataService =
        ((RegionMapOwner) region).getCache().getClientMetadataService();

    Map<ServerLocation, Set> serverToFilterMap =
        clientMetadataService.getServerToFilterMap(map.keySet(), region, true);

    if (serverToFilterMap == null || serverToFilterMap.isEmpty()) {
      AbstractOp op = new PutAllOpImpl(region, map, eventId,
          ((Pool) pool).getPRSingleHopEnabled(), skipCallbacks, callbackArg);
      op.initMessagePart();
      return (VersionedObjectList) pool.execute(op);
    }

    List callableTasks = constructAndGetPutAllTasks(region, map, eventId, skipCallbacks,
        serverToFilterMap, (InternalPool) pool, callbackArg);

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("PutAllOp#execute : Number of putAll tasks is : {}", callableTasks.size());
    }
    Map<ServerLocation, RuntimeException> failedServers = new HashMap<>();
    PutAllPartialResult result = new PutAllPartialResult(map.size());
    try {
      Map<ServerLocation, Object> results = SingleHopClientExecutor
          .submitBulkOp(callableTasks, clientMetadataService,
              (LocalRegion) region, failedServers);
      for (Map.Entry<ServerLocation, Object> entry : results.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof PutAllPartialResultException) {
          PutAllPartialResultException pap = (PutAllPartialResultException) value;
          if (isDebugEnabled) {
            logger.debug(
                "PutAll SingleHop encountered PutAllPartialResultException exception: {}, failedServers are {}",
                pap, failedServers.keySet());
          }
          result.consolidate(pap.getResult());
        } else {
          if (value != null) {
            VersionedObjectList list = (VersionedObjectList) value;
            result.addKeysAndVersions(list);
          }
        }
      }
    } catch (RuntimeException ex) {
      if (isDebugEnabled) {
        logger.debug("single-hop putAll encountered unexpected exception: ", ex);
      }
      throw ex;
    }

    if (!failedServers.isEmpty()) {
      if (retryAttempts == 0) {
        throw failedServers.values().iterator().next();
      }

      // if the partial result set doesn't already have keys (for tracking version tags)
      // then we need to gather up the keys that we know have succeeded so far and
      // add them to the partial result set
      if (result.getSucceededKeysAndVersions().size() == 0) {
        // if there're failed servers, we need to save the succeed keys in submitPutAll
        // if retry succeeded, everything is ok, otherwise, the saved "succeeded
        // keys" should be consolidated into PutAllPartialResultException
        // succeedKeySet is used to send back to client in PartialResult case
        // so it's not a must to use LinkedHashSet
        Set succeedKeySet = new LinkedHashSet();
        Set<ServerLocation> serverSet = serverToFilterMap.keySet();
        for (ServerLocation server : serverSet) {
          if (!failedServers.containsKey(server)) {
            succeedKeySet.addAll(serverToFilterMap.get(server));
          }
        }

        // save succeedKeys, but if retries all succeeded, discard the PutAllPartialResult
        result.addKeys(succeedKeySet);
      }

      // send maps for the failed servers one by one instead of merging
      // them into one big map. The reason is, we have to keep the same event
      // ids for each sub map. There is a unit test in PutAllCSDUnitTest for
      // the otherwise case.
      boolean oneSubMapRetryFailed = false;
      Set<ServerLocation> failedServerSet = failedServers.keySet();
      for (ServerLocation failedServer : failedServerSet) {
        RuntimeException savedRTE = failedServers.get(failedServer);
        if (savedRTE instanceof PutAllPartialResultException) {
          // will not retry for PutAllPartialResultException
          // but it means at least one sub map ever failed
          oneSubMapRetryFailed = true;
          continue;
        }
        Map newMap = new LinkedHashMap();
        Set keySet = serverToFilterMap.get(failedServer);
        for (Object key : keySet) {
          newMap.put(key, map.get(key));
        }

        try {
          VersionedObjectList v =
              PutAllOp.execute(pool, region, newMap, eventId, skipCallbacks, true, callbackArg);
          if (v == null) {
            result.addKeys(keySet);
          } else {
            result.addKeysAndVersions(v);
          }
        } catch (PutAllPartialResultException pre) {
          oneSubMapRetryFailed = true;
          if (logger.isDebugEnabled()) {
            logger.debug("Retry failed with PutAllPartialResultException: {} Before retry: {}", pre,
                result.getKeyListString());
          }
          result.consolidate(pre.getResult());
        } catch (Exception rte) {
          oneSubMapRetryFailed = true;
          Object firstKey = newMap.keySet().iterator().next();
          result.saveFailedKey(firstKey, rte);
        }
      } // for failedServer

      // If all retries succeeded, the PRE in first tries can be ignored
      if (oneSubMapRetryFailed && result.hasFailure()) {
        throw new PutAllPartialResultException(result);
      }
    } // failedServers!=null

    return result.getSucceededKeysAndVersions();
  }

  private PutAllOp() {
    // no instances allowed
  }

  private static List constructAndGetPutAllTasks(Region region, final Map map,
      final EventID eventId,
      boolean skipCallbacks, final Map<ServerLocation, Set> serverToFilterMap,
      final InternalPool pool, Object callbackArg) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<>();
    List<ServerLocation> servers = new ArrayList<>(serverToFilterMap.keySet());

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing tasks for the servers {}", servers);
    }
    for (ServerLocation server : servers) {
      Set filterSet = serverToFilterMap.get(server);
      Map newKeysValuesMap = new LinkedHashMap();
      // iterator 1: for single hop, both iterator filterSet and newKeysValuesMap
      for (Object key : filterSet) {
        newKeysValuesMap.put(key, map.get(key));
      }
      AbstractOp putAllOp =
          new PutAllOpImpl(region, newKeysValuesMap, eventId, true, skipCallbacks, callbackArg);

      SingleHopOperationCallable task =
          new SingleHopOperationCallable(new ServerLocation(server.getHostName(), server.getPort()),
              pool, putAllOp, UserAttributes.userAttributes.get());
      tasks.add(task);
    }
    return tasks;
  }

  private static class PutAllOpImpl extends AbstractOp {

    private boolean prSingleHopEnabled = false;

    private LocalRegion region = null;

    private Map map = null;
    private final Object callbackArg;
    private ArrayList keys = null;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public PutAllOpImpl(Region region, Map map, EventID eventId, boolean prSingleHopEnabled,
        boolean skipCallbacks, Object callbackArg) {
      super(callbackArg != null ? MessageType.PUT_ALL_WITH_CALLBACK : MessageType.PUTALL,
          (callbackArg != null ? 6 : 5) + (map.size() * 2));
      this.prSingleHopEnabled = prSingleHopEnabled;
      this.region = (LocalRegion) region;
      getMessage().addStringPart(region.getFullPath(), true);
      getMessage().addBytesPart(eventId.calcBytes());
      getMessage().addIntPart(skipCallbacks ? 1 : 0);
      this.map = map;
      this.callbackArg = callbackArg;
    }

    @Override
    protected void initMessagePart() {
      int size = map.size();
      int flags = 0;
      if (region.getDataPolicy() == DataPolicy.EMPTY) {
        flags |= FLAG_EMPTY;
      }
      if (region.getConcurrencyChecksEnabled()) {
        flags |= FLAG_CONCURRENCY_CHECKS;
      }
      getMessage().addIntPart(flags);
      getMessage().addIntPart(size);
      if (this.callbackArg != null) {
        getMessage().addObjPart(this.callbackArg);
      }
      this.keys = new ArrayList(size);
      Iterator iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry mapEntry = (Map.Entry) iterator.next();
        Object key = mapEntry.getKey();
        this.keys.add(key);
        getMessage().addStringOrObjPart(key);
        Object value = mapEntry.getValue();
        if (value instanceof CachedDeserializable) {
          CachedDeserializable cd = (CachedDeserializable) value;
          if (!cd.isSerialized()) {
            // it is a byte[]
            getMessage().addObjPart(cd.getDeserializedForReading());
          } else {
            Object cdValue = cd.getValue();
            if (cdValue instanceof byte[]) {
              getMessage().addRawPart((byte[]) cdValue, true);
            } else {
              getMessage().addObjPart(cdValue);
            }
          }
        } else {
          getMessage().addObjPart(value);
        }
      }
    }

    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(2, Version.CURRENT);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Object processResponse(final Message msg, final Connection con) throws Exception {
      final VersionedObjectList result = new VersionedObjectList();
      final Exception[] exceptionRef = new Exception[1];
      try {
        processChunkedResponse((ChunkedMessage) msg, "putAll", new ChunkHandler() {
          @Override
          public void handle(ChunkedMessage cm) throws Exception {
            int numParts = msg.getNumberOfParts();
            final boolean isDebugEnabled = logger.isDebugEnabled();
            if (isDebugEnabled) {
              logger.debug("putAllOp.processChunkedResponse processing message with {} parts",
                  numParts);
            }
            for (int partNo = 0; partNo < numParts; partNo++) {
              Part part = cm.getPart(partNo);
              try {
                Object o = part.getObject();
                if (isDebugEnabled) {
                  logger.debug("part({}) contained {}", partNo, o);
                }
                if (o == null) {
                  // no response is an okay response
                } else if (o instanceof byte[]) {
                  if (prSingleHopEnabled) {
                    byte[] bytesReceived = part.getSerializedForm();
                    if (bytesReceived[0] != ClientMetadataService.INITIAL_VERSION) { // nw hop
                      if (region != null) {
                        try {
                          ClientMetadataService cms = region.getCache().getClientMetadataService();
                          cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
                        } catch (CacheClosedException e) {
                        }
                      }
                    }
                  }
                } else if (o instanceof Throwable) {
                  String s = "While performing a remote putAll";
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
          }
        });
      } catch (ServerOperationException e) {
        if (e.getCause() instanceof PutAllPartialResultException) {
          PutAllPartialResultException cause = (PutAllPartialResultException) e.getCause();
          cause.getSucceededKeysAndVersions().replaceNullIDs(con.getEndpoint().getMemberId());
          throw cause;
        } else {
          throw e;
        }
      }
      if (exceptionRef[0] != null) {
        throw exceptionRef[0];
      } else {
        // v7.0.1: fill in the keys
        if (result.hasVersions() && result.getKeys().isEmpty()) {
          if (logger.isTraceEnabled()) {
            logger.trace("setting keys of response to {}", this.keys);
          }
          result.setKeys(this.keys);
        }
      }
      return result;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.PUT_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPutAll();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPutAllSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPutAll(start, hasTimedOut(), hasFailed());
    }
  }

}
