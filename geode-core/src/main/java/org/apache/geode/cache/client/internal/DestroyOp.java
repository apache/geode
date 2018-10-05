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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;

/**
 * Does a region destroy on a server
 *
 * @since GemFire 5.7
 */
public class DestroyOp {

  private static final Logger logger = LogService.getLogger();

  public static final int HAS_VERSION_TAG = 0x01;

  public static final int HAS_ENTRY_NOT_FOUND_PART = 0x02;

  /**
   * Does a region entry destroy on a server using connections from the given pool to communicate
   * with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the region to do the entry destroy on
   * @param key the entry key to do the destroy on
   * @param event the event for this destroy operation
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static Object execute(ExecutablePool pool, LocalRegion region, Object key,
      Object expectedOldValue, Operation operation, EntryEventImpl event, Object callbackArg,
      boolean prSingleHopEnabled) {
    if (logger.isDebugEnabled()) {
      logger.debug("Preparing DestroyOp for {} operation={}", key, operation);
    }
    DestroyOpImpl op = new DestroyOpImpl(region, key, expectedOldValue, operation, event,
        callbackArg, prSingleHopEnabled);
    if (prSingleHopEnabled) {
      ClientMetadataService cms = region.getCache().getClientMetadataService();
      ServerLocation server =
          cms.getBucketServerLocation(region, Operation.DESTROY, key, null, callbackArg);
      if (server != null) {
        try {
          PoolImpl poolImpl = (PoolImpl) pool;
          boolean onlyUseExistingCnx = ((poolImpl.getMaxConnections() != -1
              && poolImpl.getConnectionCount() >= poolImpl.getMaxConnections()) ? true : false);
          op.setAllowDuplicateMetadataRefresh(!onlyUseExistingCnx);
          return pool.executeOn(server, op, true, onlyUseExistingCnx);
        } catch (AllConnectionsInUseException e) {
        } catch (ServerConnectivityException e) {
          if (e instanceof ServerOperationException) {
            throw e; // fixed 44656
          }
          cms.removeBucketServerLocation(server);
        }
      }
    }
    return pool.execute(op);
  }

  /**
   * Does a region entry destroy on a server using the given connection to communicate with the
   * server.
   *
   * @param con the connection to use to send to the server
   * @param pool the pool to use to communicate with the server.
   * @param region the region to do the entry destroy on
   * @param key the entry key to do the destroy on
   * @param event the event for this destroy operation
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(Connection con, ExecutablePool pool, String region, Object key,
      Object expectedOldValue, Operation operation, EntryEventImpl event, Object callbackArg) {
    AbstractOp op = new DestroyOpImpl(region, key, expectedOldValue, operation, event, callbackArg);
    pool.executeOn(con, op);
  }

  /** this is set if a response is received indicating that the entry was not found on the server */
  public static boolean TEST_HOOK_ENTRY_NOT_FOUND;

  private DestroyOp() {
    // no instances allowed
  }

  private static class DestroyOpImpl extends AbstractOp {

    Object key = null;

    private LocalRegion region;

    private Operation operation;

    private boolean prSingleHopEnabled = false;

    private EntryEventImpl event;

    private Object callbackArg;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public DestroyOpImpl(LocalRegion region, Object key, Object expectedOldValue,
        Operation operation, EntryEventImpl event, Object callbackArg, boolean prSingleHopEnabled) {
      super(MessageType.DESTROY, callbackArg != null ? 6 : 5);
      this.key = key;
      this.region = region;
      this.operation = operation;
      this.prSingleHopEnabled = prSingleHopEnabled;
      this.callbackArg = callbackArg;
      this.event = event;
      getMessage().addStringPart(region.getFullPath());
      getMessage().addStringOrObjPart(key);
      getMessage().addObjPart(expectedOldValue);
      getMessage().addObjPart(operation == Operation.DESTROY ? null : operation); // server
                                                                                  // interprets null
                                                                                  // as DESTROY
      getMessage().addBytesPart(event.getEventId().calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }


    public DestroyOpImpl(String region, Object key, Object expectedOldValue, Operation operation,
        EntryEventImpl event, Object callbackArg) {
      super(MessageType.DESTROY, callbackArg != null ? 6 : 5);
      this.key = key;
      this.event = event;
      this.callbackArg = callbackArg;
      getMessage().addStringPart(region);
      getMessage().addStringOrObjPart(key);
      getMessage().addObjPart(expectedOldValue);
      getMessage().addObjPart(operation == Operation.DESTROY ? null : operation); // server
                                                                                  // interprets null
                                                                                  // as DESTROY
      getMessage().addBytesPart(event.getEventId().calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }


    @Override
    protected Object processResponse(Message msg, Connection con) throws Exception {
      processAck(msg, "destroy");
      boolean isReply = (msg.getMessageType() == MessageType.REPLY);
      int partIdx = 0;
      int flags = 0;
      if (isReply) {
        flags = msg.getPart(partIdx++).getInt();
        if ((flags & HAS_VERSION_TAG) != 0) {
          VersionTag tag = (VersionTag) msg.getPart(partIdx++).getObject();
          // we use the client's ID since we apparently don't track the server's ID in connections
          tag.replaceNullIDs((InternalDistributedMember) con.getEndpoint().getMemberId());
          this.event.setVersionTag(tag);
          if (logger.isDebugEnabled()) {
            logger.debug("received Destroy response with {}", tag);
          }
        } else if (logger.isDebugEnabled()) {
          logger.debug("received Destroy response with no version tag");
        }
      }
      if (prSingleHopEnabled) {
        Part part = msg.getPart(partIdx++);
        byte[] bytesReceived = part.getSerializedForm();
        if (bytesReceived[0] != ClientMetadataService.INITIAL_VERSION
            && bytesReceived.length == ClientMetadataService.SIZE_BYTES_ARRAY_RECEIVED) {
          if (this.region != null) {
            try {
              ClientMetadataService cms = region.getCache().getClientMetadataService();
              int myVersion =
                  cms.getMetaDataVersion(region, Operation.UPDATE, key, null, callbackArg);
              if (myVersion != bytesReceived[0] || isAllowDuplicateMetadataRefresh()) {
                cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
              }
            } catch (CacheClosedException e) {
              return null;
            }
          }
        }
      } else {
        partIdx++; // skip OK byte
      }
      boolean entryNotFound = false;
      if (msg.getMessageType() == MessageType.REPLY && (flags & HAS_ENTRY_NOT_FOUND_PART) != 0) {
        entryNotFound = (msg.getPart(partIdx++).getInt() == 1);
        if (logger.isDebugEnabled() && (flags & HAS_ENTRY_NOT_FOUND_PART) != 0) {
          logger.debug("destroy response has entryNotFound={}", entryNotFound);
        }
        if (entryNotFound) {
          TEST_HOOK_ENTRY_NOT_FOUND = true;
        }
      }
      if (this.operation == Operation.REMOVE && entryNotFound) {
        if (logger.isDebugEnabled()) {
          logger.debug("received REMOVE response from server with entryNotFound={}", entryNotFound);
        }
        return new EntryNotFoundException(
            "entry not found with expected value");
      }
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.DESTROY_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startDestroy();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endDestroySend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endDestroy(start, hasTimedOut(), hasFailed());
    }

    @Override
    public String toString() {
      return "DestroyOp:" + key;
    }
  }
}
