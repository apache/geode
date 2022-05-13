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

import static org.apache.geode.internal.cache.tier.MessageType.GET;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_INVALID;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_INVALID_WITH_METADATA_REFRESH;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_NOT_FOUND;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_NOT_FOUND_WITH_METADATA_REFRESH;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_TOMBSTONE;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_TOMBSTONE_WITH_METADATA_REFRESH;
import static org.apache.geode.internal.cache.tier.MessageType.GET_RESPONSE_WITH_METADATA_REFRESH;
import static org.apache.geode.internal.cache.tier.MessageType.GET_WITH_CALLBACK;

import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
  public static Object execute(final @NotNull ExecutablePool pool,
      final @NotNull LocalRegion region, @NotNull Object key,
      final @Nullable Object callbackArg, final boolean prSingleHopEnabled,
      final @NotNull EntryEventImpl clientEvent) {
    final AbstractOp op;
    if (null == callbackArg) {
      op = new GetWithoutCallbackOp(region, key, prSingleHopEnabled, clientEvent);
    } else {
      op = new GetWithCallbackOp(region, key, callbackArg, prSingleHopEnabled, clientEvent);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("GetOp invoked for key {}", key);
    }

    if (prSingleHopEnabled) {
      final ClientMetadataService cms = region.getCache().getClientMetadataService();
      ServerLocation server =
          cms.getBucketServerLocation(region, Operation.GET, key, null, callbackArg);
      if (server != null) {
        try {
          PoolImpl poolImpl = (PoolImpl) pool;
          boolean onlyUseExistingCnx = (poolImpl.getMaxConnections() != -1
              && poolImpl.getConnectionCount() >= poolImpl.getMaxConnections());
          op.setAllowDuplicateMetadataRefresh(!onlyUseExistingCnx);
          return pool.executeOn(new ServerLocation(server.getHostName(), server.getPort()), op,
              true, onlyUseExistingCnx);
        } catch (AllConnectionsInUseException ignored) {
        } catch (ServerConnectivityException e) {
          if (e instanceof ServerOperationException) {
            throw e;
          }
          cms.removeBucketServerLocation(server);
        } catch (CacheLoaderException e) {
          if (e.getCause() instanceof ServerConnectivityException) {
            cms.removeBucketServerLocation(server);
          }
        }
      }
    }
    return pool.execute(op);
  }


  private GetOp() {
    // no instances allowed
  }

  abstract static class AbstractGetOp extends AbstractOp {

    private final LocalRegion region;
    private final boolean singleHopEnabled;
    protected final Object key;
    protected final EntryEventImpl clientEvent;

    protected AbstractGetOp(final int msgType, final int msgParts,
        final @NotNull LocalRegion region, final @NotNull Object key,
        final boolean singleHopEnabled, final @NotNull EntryEventImpl clientEvent) {
      super(msgType, msgParts);
      this.region = region;
      this.key = key;
      this.singleHopEnabled = singleHopEnabled;
      this.clientEvent = clientEvent;

      final Message message = getMessage();
      message.addStringPart(region.getFullPath(), true);
      message.addObjPart(key);
    }

    protected @Nullable Object getCallbackArg() {
      return null;
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      throw new UnsupportedOperationException("version tag processing requires the connection");
    }

    @Override
    protected Object processResponse(final @NotNull Message message,
        final @NotNull Connection connection)
        throws IOException, ClassNotFoundException {
      final int messageType = message.getMessageType();
      switch (messageType) {
        case GET_RESPONSE:
          return processGetResponse(message, connection);
        case GET_RESPONSE_NOT_FOUND:
          return processGetResponseNotFound(message);
        case GET_RESPONSE_INVALID:
          return processGetResponseInvalid(message, connection);
        case GET_RESPONSE_TOMBSTONE:
          return processGetResponseTombstone(message, connection);
        case GET_RESPONSE_WITH_METADATA_REFRESH:
          return processGetResponseWithMetadataRefresh(message, connection);
        case GET_RESPONSE_NOT_FOUND_WITH_METADATA_REFRESH:
          return processGetResponseNotFoundWithMetadataRefresh(message);
        case GET_RESPONSE_INVALID_WITH_METADATA_REFRESH:
          return processGetResponseInvalidWithMetadataRefresh(message, connection);
        case GET_RESPONSE_TOMBSTONE_WITH_METADATA_REFRESH:
          return processGetResponseTombstoneWithMetadataRefresh(message, connection);
      }

      processErrorResponse(message, MessageType.getString(GET));
      return null;
    }

    private Object processGetResponse(final @NotNull Message message,
        final @NotNull Connection connection)
        throws IOException, ClassNotFoundException {
      final Object value = message.getPart(0).getObject();
      updateClientEventWithVersionTag(() -> message.getPart(1).getObject(), connection);
      return value;
    }

    private Object processGetResponseWithMetadataRefresh(final @NotNull Message message,
        final @NotNull Connection connection)
        throws IOException, ClassNotFoundException {
      final Object value = message.getPart(0).getObject();
      updateClientEventWithVersionTag(() -> message.getPart(1).getObject(), connection);
      processMetadataRefresh(() -> message.getPart(2));
      return value;
    }

    private Object processGetResponseNotFound(final @NotNull Message message) {
      return null;
    }

    private Object processGetResponseNotFoundWithMetadataRefresh(final @NotNull Message message) {
      processMetadataRefresh(() -> message.getPart(0));
      return null;
    }

    private Object processGetResponseInvalid(final @NotNull Message message,
        final @NotNull Connection connection) throws IOException, ClassNotFoundException {
      updateClientEventWithVersionTag(() -> message.getPart(0).getObject(), connection);
      return Token.INVALID;
    }

    private Object processGetResponseInvalidWithMetadataRefresh(final @NotNull Message message,
        final @NotNull Connection connection) throws IOException, ClassNotFoundException {
      updateClientEventWithVersionTag(() -> message.getPart(0).getObject(), connection);
      processMetadataRefresh(() -> message.getPart(1));
      return Token.INVALID;
    }

    private Object processGetResponseTombstone(final @NotNull Message message,
        final @NotNull Connection connection)
        throws IOException, ClassNotFoundException {
      updateClientEventWithVersionTag(() -> message.getPart(0).getObject(), connection);
      return Token.TOMBSTONE;
    }

    private Object processGetResponseTombstoneWithMetadataRefresh(final @NotNull Message message,
        final @NotNull Connection connection)
        throws IOException, ClassNotFoundException {
      updateClientEventWithVersionTag(() -> message.getPart(0).getObject(), connection);
      processMetadataRefresh(() -> message.getPart(1));
      return Token.TOMBSTONE;
    }

    private void processMetadataRefresh(final @NotNull PartSupplier partSupplier) {
      if (singleHopEnabled) {
        final byte[] bytesReceived = partSupplier.get().getSerializedForm();
        final byte remoteVersion = bytesReceived[0];
        final byte networkHopType = bytesReceived[1];

        final ClientMetadataService cms = region.getCache().getClientMetadataService();
        final byte localVersion =
            cms.getMetaDataVersion(region, Operation.UPDATE, key, null, getCallbackArg());
        if (remoteVersion != localVersion) {
          cms.scheduleGetPRMetaData(region, false, networkHopType);
        }
      }
    }

    @Override
    protected boolean isErrorResponse(final int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override
    protected long startAttempt(final @NotNull ConnectionStats stats) {
      return stats.startGet();
    }

    @Override
    protected void endSendAttempt(final @NotNull ConnectionStats stats, final long start) {
      stats.endGetSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(final @NotNull ConnectionStats stats, final long start) {
      stats.endGet(start, hasTimedOut(), hasFailed());
    }

    @FunctionalInterface
    interface PartSupplier {
      @NotNull
      Part get();
    }

    @FunctionalInterface
    interface PartObjectSupplier<T> {
      Object getObject() throws IOException, ClassNotFoundException;

      @SuppressWarnings("unchecked")
      default T get() throws IOException, ClassNotFoundException {
        return (T) getObject();
      }
    }

    protected void updateClientEventWithVersionTag(
        final @NotNull PartObjectSupplier<VersionTag<?>> versionTagSupplier,
        final @NotNull Connection connection)
        throws IOException, ClassNotFoundException {
      if (clientEvent != null) {
        final VersionTag<?> versionTag = versionTagSupplier.get();
        if (null != versionTag) {
          versionTag.replaceNullIDs(
              (InternalDistributedMember) connection.getEndpoint().getMemberId());
          clientEvent.setVersionTag(versionTag);
        }
      }
    }

  }

  static class GetWithoutCallbackOp extends AbstractGetOp {

    public GetWithoutCallbackOp(final @NotNull LocalRegion region, final @NotNull Object key,
        final boolean prSingleHopEnabled, final @NotNull EntryEventImpl clientEvent) {
      super(GET, 2, region, key, prSingleHopEnabled, clientEvent);
    }

    @Override
    public String toString() {
      return "GetWithoutCallbackOp{" +
          "key=" + key +
          '}';
    }
  }

  static class GetWithCallbackOp extends AbstractGetOp {

    private final Object callbackArg;

    public GetWithCallbackOp(final @NotNull LocalRegion region, final @NotNull Object key,
        final @NotNull Object callbackArg, final boolean prSingleHopEnabled,
        final @NotNull EntryEventImpl clientEvent) {
      super(GET_WITH_CALLBACK, 3, region, key, prSingleHopEnabled, clientEvent);
      this.callbackArg = callbackArg;

      final Message message = getMessage();
      message.addObjPart(callbackArg);
    }

    @Override
    protected @NotNull Object getCallbackArg() {
      return callbackArg;
    }

    @Override
    public String toString() {
      return "GetWithCallbackOp{" +
          "key=" + key +
          ", callbackArg=" + callbackArg +
          '}';
    }
  }

  static class GetOpImpl extends AbstractOp {

    private LocalRegion region = null;

    private boolean prSingleHopEnabled = false;

    private final Object key;

    private final Object callbackArg;

    private final EntryEventImpl clientEvent;

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
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      throw new UnsupportedOperationException(); // version tag processing requires the connection
    }

    @Override
    protected Object processResponse(final @NotNull Message msg, final @NotNull Connection con)
        throws Exception {
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
          if (clientEvent != null) {
            clientEvent.setVersionTag(tag);
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
              if (region != null
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

    private boolean processMetadataRefresh(final Part part) {
      byte version;
      byte[] bytesReceived = part.getSerializedForm();
      if (region != null
          && bytesReceived.length == ClientMetadataService.SIZE_BYTES_ARRAY_RECEIVED) {
        ClientMetadataService cms;
        try {
          cms = region.getCache().getClientMetadataService();
          version =
              cms.getMetaDataVersion(region, Operation.UPDATE, key, null, callbackArg);
        } catch (CacheClosedException e) {
          return true;
        }
        if (bytesReceived[0] != version) {
          cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
        }
      }
      return false;
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
