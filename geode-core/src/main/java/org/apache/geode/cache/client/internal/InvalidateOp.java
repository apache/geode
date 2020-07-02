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
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Does a region invalidate on a server
 *
 * @since GemFire 6.6
 */
public class InvalidateOp {

  private static final Logger logger = LogService.getLogger();

  public static final int HAS_VERSION_TAG = 0x01;

  /**
   * Does a region invalidate on a server using connections from the given pool to communicate with
   * the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param regionName the name of the region to do the entry invalidate on.
   * @param region the region to do the entry invalidate on.
   * @param event the event for this invalidate operation
   */
  public static Object execute(ExecutablePool pool, String regionName, EntryEventImpl event,
      boolean prSingleHopEnabled,
      LocalRegion region) {
    AbstractOp op = new InvalidateOpImpl(regionName, event, prSingleHopEnabled, region);

    if (prSingleHopEnabled) {
      ClientMetadataService cms = region.getCache().getClientMetadataService();
      ServerLocation server =
          cms.getBucketServerLocation(region, Operation.INVALIDATE, event.getKey(), null,
              event.getCallbackArgument());
      if (server != null) {
        try {
          PoolImpl poolImpl = (PoolImpl) pool;
          boolean onlyUseExistingCnx = (poolImpl.getMaxConnections() != -1
              && poolImpl.getConnectionCount() >= poolImpl.getMaxConnections());
          op.setAllowDuplicateMetadataRefresh(!onlyUseExistingCnx);
          return pool.executeOn(new ServerLocation(server.getHostName(), server.getPort()), op,
              true, onlyUseExistingCnx);
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

  private InvalidateOp() {
    // no instances allowed
  }

  private static class InvalidateOpImpl extends AbstractOp {
    private EntryEventImpl event;

    private boolean prSingleHopEnabled = false;

    private LocalRegion region = null;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public InvalidateOpImpl(String regionName, EntryEventImpl event, boolean prSingleHopEnabled,
        LocalRegion region) {
      super(MessageType.INVALIDATE, event.getCallbackArgument() != null ? 4 : 3);
      Object callbackArg = event.getCallbackArgument();
      this.event = event;
      this.prSingleHopEnabled = prSingleHopEnabled;
      this.region = region;

      getMessage().addStringPart(regionName, true);
      getMessage().addStringOrObjPart(event.getKeyInfo().getKey());
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
      processAck(msg, "invalidate");
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
            logger.debug("received Invalidate response with {}", tag);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("received Invalidate response");
          }
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
                  cms.getMetaDataVersion(region, Operation.UPDATE, event.getKey(), null,
                      event.getCallbackArgument());
              if (myVersion != bytesReceived[0] || isAllowDuplicateMetadataRefresh()) {
                cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
              }
            } catch (CacheClosedException e) {
              return null;
            }
          }
        }
      }
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.INVALIDATE_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startInvalidate();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endInvalidateSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endInvalidate(start, hasTimedOut(), hasFailed());
    }
  }
}
