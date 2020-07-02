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
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Does a region get on a server
 *
 * @since GemFire 5.7
 */
public class GetOp {
  private static final Logger logger = LogService.getLogger();

  public static final int HAS_CALLBACK_ARG = 0x01;
  public static final int HAS_VERSION_TAG = 0x02;
  public static final int KEY_NOT_PRESENT = 0x04;
  public static final int VALUE_IS_INVALID = 0x08; // Token.INVALID

  /**
   * Does a region get on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the region to do the get on
   * @param key the entry key to do the get on
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   * @param clientEvent holder for returning version information
   * @return the entry value found by the get if any
   */
  public static Object execute(ExecutablePool pool, LocalRegion region, Object key,
      Object callbackArg, boolean prSingleHopEnabled, EntryEventImpl clientEvent) {
    ClientMetadataService cms = region.getCache().getClientMetadataService();
    GetOpImpl op = new GetOpImpl(region, key, callbackArg, prSingleHopEnabled, clientEvent);

    if (logger.isDebugEnabled()) {
      logger.debug("GetOp invoked for key {}", key);
    }
    if (prSingleHopEnabled) {
      ServerLocation server =
          cms.getBucketServerLocation(region, Operation.GET, key, null, callbackArg);
      if (server != null) {
        try {
          PoolImpl poolImpl = (PoolImpl) pool;
          boolean onlyUseExistingCnx = ((poolImpl.getMaxConnections() != -1
              && poolImpl.getConnectionCount() >= poolImpl.getMaxConnections()) ? true : false);
          op.setAllowDuplicateMetadataRefresh(!onlyUseExistingCnx);
          return pool.executeOn(new ServerLocation(server.getHostName(), server.getPort()), op,
              true, onlyUseExistingCnx);
        } catch (AllConnectionsInUseException e) {
        } catch (ServerConnectivityException e) {
          if (e instanceof ServerOperationException) {
            throw e; // fixed 44656
          }
          cms.removeBucketServerLocation(server);
        } catch (CacheLoaderException e) {
          if (e.getCause() instanceof ServerConnectivityException)
            cms.removeBucketServerLocation(server);
        }
      }
    }
    return pool.execute(op);
  }


  private GetOp() {
    // no instances allowed
  }

  static class GetOpImpl extends AbstractOp {

    private LocalRegion region = null;

    private boolean prSingleHopEnabled = false;

    private Object key;

    private Object callbackArg;

    private EntryEventImpl clientEvent;

    public String toString() {
      return "GetOpImpl(key=" + key + ")";
    }

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public GetOpImpl(LocalRegion region, Object key, Object callbackArg, boolean prSingleHopEnabled,
        EntryEventImpl clientEvent) {
      super(MessageType.REQUEST, callbackArg != null ? 3 : 2);
      if (logger.isDebugEnabled()) {
        logger.debug("constructing a GetOp for key {}", key/* , new Exception("stack trace") */);
      }
      this.region = region;
      this.prSingleHopEnabled = prSingleHopEnabled;
      this.key = key;
      this.callbackArg = callbackArg;
      this.clientEvent = clientEvent;
      getMessage().addStringPart(region.getFullPath(), true);
      getMessage().addStringOrObjPart(key);
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException(); // version tag processing requires the connection
    }

    @Override
    protected Object processResponse(Message msg, ClientCacheConnection con) throws Exception {
      Object object = processObjResponse(msg, "get");
      if (msg.getNumberOfParts() > 1) {
        int partIdx = 1;
        int flags = msg.getPart(partIdx++).getInt();
        if ((flags & HAS_CALLBACK_ARG) != 0) {
          msg.getPart(partIdx++).getObject(); // callbackArg
        }
        // if there's a version tag
        if ((object == null) && ((flags & VALUE_IS_INVALID) != 0)) {
          object = Token.INVALID;
        }
        if ((flags & HAS_VERSION_TAG) != 0) {
          VersionTag tag = (VersionTag) msg.getPart(partIdx++).getObject();
          assert con != null; // for debugging
          assert con.getEndpoint() != null; // for debugging
          assert tag != null; // for debugging
          tag.replaceNullIDs((InternalDistributedMember) con.getEndpoint().getMemberId());
          if (this.clientEvent != null) {
            this.clientEvent.setVersionTag(tag);
          }
          if ((flags & KEY_NOT_PRESENT) != 0) {
            object = Token.TOMBSTONE;
          }
        }
        if (prSingleHopEnabled && msg.getNumberOfParts() > partIdx) {
          byte version = 0;
          int noOfMsgParts = msg.getNumberOfParts();
          if (noOfMsgParts == partIdx + 1) {
            Part part = msg.getPart(partIdx++);
            if (part.isBytes()) {
              byte[] bytesReceived = part.getSerializedForm();
              if (bytesReceived[0] != ClientMetadataService.INITIAL_VERSION
                  && bytesReceived.length == ClientMetadataService.SIZE_BYTES_ARRAY_RECEIVED) {
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
          } else if (noOfMsgParts == partIdx + 2) {
            msg.getPart(partIdx++).getObject(); // callbackArg
            Part part = msg.getPart(partIdx++);
            if (part.isBytes()) {
              byte[] bytesReceived = part.getSerializedForm();
              if (this.region != null
                  && bytesReceived.length == ClientMetadataService.SIZE_BYTES_ARRAY_RECEIVED) {
                ClientMetadataService cms;
                try {
                  cms = region.getCache().getClientMetadataService();
                  version =
                      cms.getMetaDataVersion(region, Operation.UPDATE, key, null, callbackArg);
                } catch (CacheClosedException e) {
                  return null;
                }
                if (bytesReceived[0] != version) {
                  cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
                }
              }
            }
          }
        }
      }
      return object;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGet();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGet(start, hasTimedOut(), hasFailed());
    }
  }
}
