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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;

/**
 * Does a region put (or create) on a server
 *
 * @since GemFire 5.7
 */
public class PutOp {

  private static final Logger logger = LogService.getLogger();

  /**
   * Does a region put on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the region to do the put on
   * @param key the entry key to do the put on
   * @param value the entry value to put
   * @param event the event for this put
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static Object execute(ExecutablePool pool, LocalRegion region, Object key, Object value,
      byte[] deltaBytes, EntryEventImpl event, Operation operation, boolean requireOldValue,
      Object expectedOldValue, Object callbackArg, boolean prSingleHopEnabled) {
    PutOpImpl op = new PutOpImpl(region, key, value, deltaBytes, event, operation, requireOldValue,
        expectedOldValue, callbackArg, false/* donot send full obj; send delta */,
        prSingleHopEnabled);

    if (prSingleHopEnabled) {
      ClientMetadataService cms = region.getCache().getClientMetadataService();
      ServerLocation server =
          cms.getBucketServerLocation(region, Operation.UPDATE, key, value, callbackArg);
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
        }
      }
    }
    return pool.execute(op);
  }

  public static Object execute(ExecutablePool pool, String regionName, Object key, Object value,
      byte[] deltaBytes, EntryEventImpl event, Operation operation,
      boolean requireOldValue,
      Object expectedOldValue, Object callbackArg,
      boolean prSingleHopEnabled) {

    AbstractOp op = new PutOpImpl(regionName, key, value, deltaBytes, event, operation,
        requireOldValue, expectedOldValue, callbackArg, false/* donot send full obj; send delta */,
        prSingleHopEnabled);
    return pool.execute(op);
  }


  /**
   * This is a unit test method. It does a region put on a server using the given connection from
   * the given pool to communicate with the server. Do not call this method if the value is Delta
   * instance.
   *
   * @param con the connection to use
   * @param pool the pool to use to communicate with the server.
   * @param regionName the name of the region to do the put on
   * @param key the entry key to do the put on
   * @param value the entry value to put
   * @param event the event for this put
   * @param callbackArg an optional callback arg to pass to any cache callbacks
   */
  public static void execute(Connection con, ExecutablePool pool, String regionName, Object key,
      Object value, EntryEventImpl event, Object callbackArg, boolean prSingleHopEnabled) {
    AbstractOp op = new PutOpImpl(regionName, key, value, null, event, Operation.CREATE, false,
        null, callbackArg, false /* donot send full Obj; send delta */, prSingleHopEnabled);
    pool.executeOn(con, op);
  }

  public static final byte HAS_OLD_VALUE_FLAG = 0x01;
  public static final byte OLD_VALUE_IS_OBJECT_FLAG = 0x02;
  public static final byte HAS_VERSION_TAG = 0x04;

  private PutOp() {
    // no instances allowed
  }

  protected static class PutOpImpl extends AbstractOp {

    private Object key;


    private LocalRegion region;

    /**
     * the operation will have either a region or a regionName. Names seem to be used by unit tests
     * to exercise operations without creating a real region
     */
    private String regionName;

    private Object value;

    private boolean deltaSent = false;

    private EntryEventImpl event;

    private Object callbackArg;

    private boolean prSingleHopEnabled;

    private boolean requireOldValue;

    private Object expectedOldValue;


    public PutOpImpl(String regionName, Object key, Object value, byte[] deltaBytes,
        EntryEventImpl event, Operation op, boolean requireOldValue, Object expectedOldValue,
        Object callbackArg, boolean respondingToInvalidDelta, boolean prSingleHopEnabled) {
      super(MessageType.PUT,
          7 + (callbackArg != null ? 1 : 0) + (expectedOldValue != null ? 1 : 0));
      final boolean isDebugEnabled = logger.isDebugEnabled();
      if (isDebugEnabled) {
        logger.debug("PutOpImpl constructing(1) message for {}; operation={}", event.getEventId(),
            op);
      }
      this.key = key;
      this.callbackArg = callbackArg;
      this.event = event;
      this.value = value;
      this.regionName = regionName;
      this.prSingleHopEnabled = prSingleHopEnabled;
      this.requireOldValue = requireOldValue;
      this.expectedOldValue = expectedOldValue;
      getMessage().addStringPart(regionName);
      getMessage().addObjPart(op);
      int flags = 0;
      if (requireOldValue)
        flags |= 0x01;
      if (expectedOldValue != null)
        flags |= 0x02;
      getMessage().addIntPart(flags);
      if (expectedOldValue != null) {
        getMessage().addObjPart(expectedOldValue);
      }
      getMessage().addStringOrObjPart(key);
      if (respondingToInvalidDelta) {
        getMessage().setIsRetry();
      }
      // Add message part for sending either delta or full value
      if (!respondingToInvalidDelta && deltaBytes != null && op == Operation.UPDATE) {
        getMessage().addObjPart(Boolean.TRUE);
        getMessage().addBytesPart(deltaBytes);
        this.deltaSent = true;
        if (isDebugEnabled) {
          logger.debug("PutOp: Sending delta for key {}", this.key);
        }
      } else if (value instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable) value;
        if (!cd.isSerialized()) {
          // it is a byte[]
          getMessage().addObjPart(Boolean.FALSE);
          getMessage().addObjPart(cd.getDeserializedForReading());
        } else {
          getMessage().addObjPart(Boolean.FALSE);
          Object cdValue = cd.getValue();
          if (cdValue instanceof byte[]) {
            getMessage().addRawPart((byte[]) cdValue, true);
          } else {
            getMessage().addObjPart(cdValue);
          }
        }
      } else {
        getMessage().addObjPart(Boolean.FALSE);
        getMessage().addObjPart(value);
      }
      getMessage().addBytesPart(event.getEventId().calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    public PutOpImpl(Region region, Object key, Object value, byte[] deltaBytes,
        EntryEventImpl event, Operation op, boolean requireOldValue, Object expectedOldValue,
        Object callbackArg, boolean sendFullObj, boolean prSingleHopEnabled) {
      super(MessageType.PUT,
          7 + (callbackArg != null ? 1 : 0) + (expectedOldValue != null ? 1 : 0));
      this.key = key;
      this.callbackArg = callbackArg;
      this.event = event;
      this.value = value;
      this.region = (LocalRegion) region;
      this.regionName = region.getFullPath();
      this.prSingleHopEnabled = prSingleHopEnabled;
      final boolean isDebugEnabled = logger.isDebugEnabled();
      if (isDebugEnabled) {
        logger.debug("PutOpImpl constructing message with operation={}", op);
      }
      getMessage().addStringPart(region.getFullPath());
      getMessage().addObjPart(op);
      int flags = 0;
      if (requireOldValue)
        flags |= 0x01;
      if (expectedOldValue != null)
        flags |= 0x02;
      getMessage().addIntPart(flags);
      if (expectedOldValue != null) {
        getMessage().addObjPart(expectedOldValue);
      }
      getMessage().addStringOrObjPart(key);
      // Add message part for sending either delta or full value
      if (!sendFullObj && deltaBytes != null && op == Operation.UPDATE) {
        getMessage().addObjPart(Boolean.TRUE);
        getMessage().addBytesPart(deltaBytes);
        this.deltaSent = true;
        if (isDebugEnabled) {
          logger.debug("PutOp: Sending delta for key {}", this.key);
        }
      } else if (value instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable) value;
        if (!cd.isSerialized()) {
          // it is a byte[]
          getMessage().addObjPart(Boolean.FALSE);
          getMessage().addObjPart(cd.getDeserializedForReading());
        } else {
          getMessage().addObjPart(Boolean.FALSE);
          Object cdValue = cd.getValue();
          if (cdValue instanceof byte[]) {
            getMessage().addRawPart((byte[]) cdValue, true);
          } else {
            getMessage().addObjPart(cdValue);
          }
        }
      } else {
        getMessage().addObjPart(Boolean.FALSE);
        getMessage().addObjPart(value);
      }
      getMessage().addBytesPart(event.getEventId().calcBytes());
      if (callbackArg != null) {
        getMessage().addObjPart(callbackArg);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      throw new UnsupportedOperationException(
          "processResponse should not be invoked in PutOp.  Use processResponse(Message, Connection)");
    }

    /*
     * Process a response that contains an ack.
     *
     * @param msg the message containing the response
     *
     * @param con Connection on which this op is executing
     *
     * @throws Exception if response could not be processed or we received a response with a server
     * exception.
     *
     * @since GemFire 6.1
     */
    @Override
    protected Object processResponse(Message msg, Connection con) throws Exception {
      processAck(msg, "put", con);

      if (prSingleHopEnabled) {
        Part part = msg.getPart(0);
        byte[] bytesReceived = part.getSerializedForm();
        if (bytesReceived[0] != ClientMetadataService.INITIAL_VERSION
            && bytesReceived.length == ClientMetadataService.SIZE_BYTES_ARRAY_RECEIVED) {
          if (this.region != null) {
            ClientMetadataService cms = region.getCache().getClientMetadataService();
            byte myVersion =
                cms.getMetaDataVersion(region, Operation.UPDATE, key, value, callbackArg);
            if (myVersion != bytesReceived[0] || isAllowDuplicateMetadataRefresh()) {
              cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
            }
          }
        }
      }
      if (msg.getMessageType() == MessageType.REPLY && msg.getNumberOfParts() > 1) {
        int flags = msg.getPart(1).getInt();
        int partIdx = 2;
        Object oldValue = null;
        if ((flags & HAS_OLD_VALUE_FLAG) != 0) {
          oldValue = msg.getPart(partIdx++).getObject();
          if ((flags & OLD_VALUE_IS_OBJECT_FLAG) != 0 && oldValue instanceof byte[]) {
            ByteArrayInputStream in = new ByteArrayInputStream((byte[]) oldValue);
            DataInputStream din = new DataInputStream(in);
            oldValue = DataSerializer.readObject(din);
          }
        }
        // if the server has versioning we will attach it to the client's event
        // here so it can be applied to the cache
        if ((flags & HAS_VERSION_TAG) != 0) {
          VersionTag tag = (VersionTag) msg.getPart(partIdx++).getObject();
          // we use the client's ID since we apparently don't track the server's ID in connections
          tag.replaceNullIDs((InternalDistributedMember) con.getEndpoint().getMemberId());
          this.event.setVersionTag(tag);
        }
        return oldValue;
      }
      return null;
    }

    /**
     * Process a response that contains an ack.
     *
     * @param msg the message containing the response
     * @param opName text describing this op
     * @param con Connection on which this op is executing
     * @throws Exception if response could not be processed or we received a response with a server
     *         exception.
     * @since GemFire 6.1
     */
    private void processAck(Message msg, String opName, Connection con) throws Exception {
      final int msgType = msg.getMessageType();
      // Update delta stats
      if (this.deltaSent && this.region != null) {
        this.region.getCachePerfStats().incDeltasSent();
      }
      if (msgType == MessageType.REPLY) {
        return;
      } else {
        Part part = msg.getPart(0);
        if (msgType == MessageType.PUT_DELTA_ERROR) {
          if (logger.isDebugEnabled()) {
            logger.debug("PutOp: Sending full value as delta failed on server...");
          }
          AbstractOp op = new PutOpImpl(this.regionName, this.key, this.value, null, this.event,
              Operation.CREATE, this.requireOldValue, this.expectedOldValue, this.callbackArg,
              true /* send full obj */, this.prSingleHopEnabled);

          op.attempt(con);
          if (this.region != null) {
            this.region.getCachePerfStats().incDeltaFullValuesSent();
          }
        } else if (msgType == MessageType.EXCEPTION) {
          String s = ": While performing a remote " + opName;
          throw new ServerOperationException(s, (Throwable) part.getObject());
          // Get the exception toString part.
          // This was added for c++ thin client and not used in java
        } else if (isErrorResponse(msgType)) {
          throw new ServerOperationException(part.getString());
        } else {
          throw new InternalGemFireError(
              "Unexpected message type " + MessageType.getString(msgType));
        }
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.PUT_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startPut();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endPutSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endPut(start, hasTimedOut(), hasFailed());
    }

    @Override
    public String toString() {
      return "PutOp:" + key;
    }

    /**
     * Attempts to read a response to this operation by reading it from the given connection, and
     * returning it.
     *
     * @param cnx the connection to read the response from
     * @return the result of the operation or <code>null</code if the operation has no result.
     * @throws Exception if the execute failed
     */
    @Override
    protected Object attemptReadResponse(Connection cnx) throws Exception {
      Message msg = createResponseMessage();
      if (msg != null) {
        msg.setComms(cnx.getSocket(), cnx.getInputStream(), cnx.getOutputStream(),
            cnx.getCommBuffer(), cnx.getStats());
        if (msg instanceof ChunkedMessage) {
          try {
            return processResponse(msg, cnx);
          } finally {
            msg.unsetComms();
            processSecureBytes(cnx, msg);
          }
        } else {
          try {
            msg.receive();
          } finally {
            msg.unsetComms();
            processSecureBytes(cnx, msg);
          }
          return processResponse(msg, cnx);
        }
      } else {
        return null;
      }
    }
  }

}
