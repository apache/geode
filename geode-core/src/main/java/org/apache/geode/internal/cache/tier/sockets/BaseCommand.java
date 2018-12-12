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

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CopyException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SerializationException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.FindVersionTagOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.LocalRegion.NonTXEntry;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VersionTagHolder;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.security.GemFireSecurityException;

public abstract class BaseCommand implements Command {
  protected static final Logger logger = LogService.getLogger();

  private static final byte[] OK_BYTES = new byte[] {0};

  public static final int MAXIMUM_CHUNK_SIZE =
      Integer.getInteger("BridgeServer.MAXIMUM_CHUNK_SIZE", 100);

  /** Whether to suppress logging of IOExceptions */
  private static final boolean SUPPRESS_IO_EXCEPTION_LOGGING =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "bridge.suppressIOExceptionLogging");

  /**
   * Maximum number of concurrent incoming client message bytes that a cache server will allow.
   * Once a server is working on this number additional incoming client messages will wait until one
   * of them completes or fails. The bytes are computed based in the size sent in the incoming msg
   * header.
   */
  private static final int MAX_INCOMING_DATA =
      Integer.getInteger("BridgeServer.MAX_INCOMING_DATA", -1);

  /**
   * Maximum number of concurrent incoming client messages that a cache server will allow. Once a
   * server is working on this number additional incoming client messages will wait until one of
   * them completes or fails.
   */
  private static final int MAX_INCOMING_MESSAGES =
      Integer.getInteger("BridgeServer.MAX_INCOMING_MSGS", -1);

  private static final Semaphore INCOMING_DATA_LIMITER;

  private static final Semaphore INCOMING_MSG_LIMITER;

  static {
    Semaphore semaphore;
    if (MAX_INCOMING_DATA > 0) {
      // backport requires that this is fair since we inc by values > 1
      semaphore = new Semaphore(MAX_INCOMING_DATA, true);
    } else {
      semaphore = null;
    }
    INCOMING_DATA_LIMITER = semaphore;
    if (MAX_INCOMING_MESSAGES > 0) {
      // unfair for best performance
      semaphore = new Semaphore(MAX_INCOMING_MESSAGES, false);
    } else {
      semaphore = null;
    }
    INCOMING_MSG_LIMITER = semaphore;
  }

  protected static byte[] okBytes() {
    return OK_BYTES;
  }

  protected boolean setLastResultReceived(
      ServerToClientFunctionResultSender resultSender) {

    if (resultSender != null) {
      synchronized (resultSender) {
        if (resultSender.isLastResultReceived()) {
          return false;
        } else {
          resultSender.setLastResultReceived(true);
        }
      }
    }
    return true;
  }

  @Override
  public void execute(Message clientMessage, ServerConnection serverConnection,
      SecurityService securityService) {
    // Read the request and update the statistics
    long start = DistributionStats.getStatTime();
    if (EntryLogger.isEnabled() && serverConnection != null) {
      EntryLogger.setSource(serverConnection.getMembershipID(), "c2s");
    }
    boolean shouldMasquerade = shouldMasqueradeForTx(clientMessage, serverConnection);
    try {
      if (shouldMasquerade) {
        InternalCache cache = serverConnection.getCache();
        InternalDistributedMember member =
            (InternalDistributedMember) serverConnection.getProxyID().getDistributedMember();
        TXManagerImpl txMgr = cache.getTxManager();
        TXStateProxy tx = null;
        try {
          tx = txMgr.masqueradeAs(clientMessage, member, false);
          cmdExecute(clientMessage, serverConnection, securityService, start);
          tx.updateProxyServer(txMgr.getMemberId());
        } finally {
          txMgr.unmasquerade(tx);
        }
      } else {
        cmdExecute(clientMessage, serverConnection, securityService, start);
      }

    } catch (TransactionException | CopyException | SerializationException | CacheWriterException
        | CacheLoaderException | GemFireSecurityException | PartitionOfflineException
        | MessageTooLargeException e) {
      handleExceptionNoDisconnect(clientMessage, serverConnection, e);
    } catch (EOFException eof) {
      BaseCommand.handleEOFException(clientMessage, serverConnection, eof);
    } catch (InterruptedIOException e) { // Solaris only
      BaseCommand.handleInterruptedIOException(serverConnection, e);
    } catch (IOException e) {
      BaseCommand.handleIOException(clientMessage, serverConnection, e);
    } catch (DistributedSystemDisconnectedException e) {
      BaseCommand.handleShutdownException(clientMessage, serverConnection, e);
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable e) {
      BaseCommand.handleThrowable(clientMessage, serverConnection, e);
    } finally {
      EntryLogger.clearSource();
    }
  }

  /**
   * checks to see if this thread needs to masquerade as a transactional thread. clients after
   * GFE_66 should be able to start a transaction.
   *
   * @return true if thread should masquerade as a transactional thread.
   */
  protected boolean shouldMasqueradeForTx(Message clientMessage,
      ServerConnection serverConnection) {
    return serverConnection.getClientVersion().compareTo(Version.GFE_66) >= 0
        && clientMessage.getTransactionId() > TXManagerImpl.NOTX;
  }

  /**
   * If an operation is retried then some server may have seen it already. We cannot apply this
   * operation to the cache without knowing whether a version tag has already been created for it.
   * Otherwise caches that have seen the event already will reject it but others will not, but will
   * have no version tag with which to perform concurrency checks.
   * <p>
   * The client event should have the event identifier from the client and the region affected by
   * the operation.
   */
  public boolean recoverVersionTagForRetriedOperation(EntryEventImpl clientEvent) {
    InternalRegion r = clientEvent.getRegion();
    VersionTag tag = r.findVersionTagForEvent(clientEvent.getEventId());
    if (tag == null) {
      if (r instanceof DistributedRegion || r instanceof PartitionedRegion) {
        // TODO this could be optimized for partitioned regions by sending the key
        // so that the PR could look at an individual bucket for the event
        tag = FindVersionTagOperation.findVersionTag(r, clientEvent.getEventId(), false);
      }
    }
    if (tag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("recovered version tag {} for replayed operation {}", tag,
            clientEvent.getEventId());
      }
      clientEvent.setVersionTag(tag);
    }
    return tag != null;
  }

  /**
   * If an operation is retried then some server may have seen it already. We cannot apply this
   * operation to the cache without knowing whether a version tag has already been created for it.
   * Otherwise caches that have seen the event already will reject it but others will not, but will
   * have no version tag with which to perform concurrency checks.
   * <p>
   * The client event should have the event identifier from the client and the region affected by
   * the operation.
   */
  protected VersionTag findVersionTagsForRetriedBulkOp(LocalRegion region, EventID eventID) {
    VersionTag tag = region.findVersionTagForClientBulkOp(eventID);
    if (tag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("recovered version tag {} for replayed bulk operation {}", tag, eventID);
      }
      return tag;
    }
    if (region instanceof DistributedRegion || region instanceof PartitionedRegion) {
      // TODO this could be optimized for partitioned regions by sending the key
      // so that the PR could look at an individual bucket for the event
      tag = FindVersionTagOperation.findVersionTag(region, eventID, true);
    }
    if (tag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("recovered version tag {} for replayed bulk operation {}", tag, eventID);
      }
    }
    return tag;
  }

  public abstract void cmdExecute(final Message clientMessage,
      final ServerConnection serverConnection, final SecurityService securityService,
      final long start) throws IOException, ClassNotFoundException, InterruptedException;

  protected void writeReply(Message origMsg, ServerConnection serverConnection) throws IOException {
    Message replyMsg = serverConnection.getReplyMessage();
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(okBytes());
    replyMsg.send(serverConnection);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", serverConnection.getName(), origMsg.getTransactionId());
    }
  }

  protected void writeReplyWithRefreshMetadata(Message origMsg, ServerConnection serverConnection,
      PartitionedRegion pr, byte nwHop) throws IOException {
    Message replyMsg = serverConnection.getReplyMessage();
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    replyMsg.send(serverConnection);
    pr.getPrStats().incPRMetaDataSentCount();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADATA tx: {}", serverConnection.getName(),
          origMsg.getTransactionId());
    }
  }

  private static void handleEOFException(Message msg, ServerConnection serverConnection,
      Exception eof) {
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    boolean potentialModification = serverConnection.getPotentialModification();
    if (!crHelper.isShutdown()) {
      if (potentialModification) {
        stats.incAbandonedWriteRequests();
      } else {
        stats.incAbandonedReadRequests();
      }
      if (!SUPPRESS_IO_EXCEPTION_LOGGING) {
        if (potentialModification) {
          int transId = msg != null ? msg.getTransactionId() : Integer.MIN_VALUE;
          logger.warn(
              "{}: EOFException during a write operation on region : {} key: {} messageId: {}",
              new Object[] {serverConnection.getName(), serverConnection.getModRegion(),
                  serverConnection.getModKey(), transId});
        } else {
          logger.debug("EOF exception", eof);
          logger.info("{}: connection disconnect detected by EOF.",
              serverConnection.getName());
        }
      }
    }
    serverConnection.setFlagProcessMessagesAsFalse();
    serverConnection.setClientDisconnectedException(eof);
  }

  private static void handleInterruptedIOException(ServerConnection serverConnection, Exception e) {
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    if (!crHelper.isShutdown() && serverConnection.isOpen()) {
      if (!SUPPRESS_IO_EXCEPTION_LOGGING) {
        if (logger.isDebugEnabled())
          logger.debug("Aborted message due to interrupt: {}", e.getMessage(), e);
      }
    }
    serverConnection.setFlagProcessMessagesAsFalse();
    serverConnection.setClientDisconnectedException(e);
  }

  private static void handleIOException(Message msg, ServerConnection serverConnection,
      Exception e) {
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    boolean potentialModification = serverConnection.getPotentialModification();

    if (!crHelper.isShutdown() && serverConnection.isOpen()) {
      if (!SUPPRESS_IO_EXCEPTION_LOGGING) {
        if (potentialModification) {
          int transId = msg != null ? msg.getTransactionId() : Integer.MIN_VALUE;
          logger.warn(String.format(
              "%s: Unexpected IOException during operation for region: %s key: %s messId: %s",
              new Object[] {serverConnection.getName(), serverConnection.getModRegion(),
                  serverConnection.getModKey(), transId}),
              e);
        } else {
          logger.warn(String.format("%s: Unexpected IOException: ",
              serverConnection.getName()), e);
        }
      }
    }
    serverConnection.setFlagProcessMessagesAsFalse();
    serverConnection.setClientDisconnectedException(e);
  }

  private static void handleShutdownException(Message msg, ServerConnection serverConnection,
      Exception e) {
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    boolean potentialModification = serverConnection.getPotentialModification();

    if (!crHelper.isShutdown()) {
      if (potentialModification) {
        int transId = msg != null ? msg.getTransactionId() : Integer.MIN_VALUE;
        logger.warn(String.format(
            "%s: Unexpected ShutdownException during operation on region: %s key: %s messageId: %s",
            new Object[] {serverConnection.getName(), serverConnection.getModRegion(),
                serverConnection.getModKey(), transId}),
            e);
      } else {
        logger.warn(String.format("%s: Unexpected ShutdownException: ",
            serverConnection.getName()),
            e);
      }
    }
    serverConnection.setFlagProcessMessagesAsFalse();
    serverConnection.setClientDisconnectedException(e);
  }

  private static void handleExceptionNoDisconnect(Message msg, ServerConnection serverConnection,
      Exception e) {
    boolean requiresResponse = serverConnection.getTransientFlag(REQUIRES_RESPONSE);
    boolean responded = serverConnection.getTransientFlag(RESPONDED);
    boolean requiresChunkedResponse = serverConnection.getTransientFlag(REQUIRES_CHUNKED_RESPONSE);
    boolean potentialModification = serverConnection.getPotentialModification();

    try {
      boolean wroteExceptionResponse = false;
      try {
        if (requiresResponse && !responded) {
          if (requiresChunkedResponse) {
            writeChunkedException(msg, e, serverConnection);
          } else {
            writeException(msg, e, false, serverConnection);
          }
          wroteExceptionResponse = true;
          serverConnection.setAsTrue(RESPONDED);
        }
      } finally { // inner try-finally to ensure proper ordering of logging
        if (potentialModification) {
          int transId = msg != null ? msg.getTransactionId() : Integer.MIN_VALUE;
          if (!wroteExceptionResponse) {
            logger.warn(String.format(
                "%s: Unexpected Exception during operation on region: %s key: %s messageId: %s",
                new Object[] {serverConnection.getName(), serverConnection.getModRegion(),
                    serverConnection.getModKey(), transId}),
                e);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Exception during operation on region: {} key: {} messageId: {}",
                  serverConnection.getName(), serverConnection.getModRegion(),
                  serverConnection.getModKey(), transId, e);
            }
          }
        } else {
          if (!wroteExceptionResponse) {
            logger.warn(String.format("%s: Unexpected Exception",
                serverConnection.getName()), e);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Exception: {}", serverConnection.getName(), e.getMessage(), e);
            }
          }
        }
      }
    } catch (IOException ioe) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Unexpected IOException writing exception: {}", serverConnection.getName(),
            ioe.getMessage(), ioe);
      }
    }
  }

  private static void handleThrowable(Message msg, ServerConnection serverConnection,
      Throwable th) {
    boolean requiresResponse = serverConnection.getTransientFlag(REQUIRES_RESPONSE);
    boolean responded = serverConnection.getTransientFlag(RESPONDED);
    boolean requiresChunkedResponse = serverConnection.getTransientFlag(REQUIRES_CHUNKED_RESPONSE);
    boolean potentialModification = serverConnection.getPotentialModification();

    try {
      try {
        if (th instanceof Error) {
          logger.fatal(String.format("%s : Unexpected Error on server",
              serverConnection.getName()),
              th);
        }
        if (requiresResponse && !responded) {
          if (requiresChunkedResponse) {
            writeChunkedException(msg, th, serverConnection);
          } else {
            writeException(msg, th, false, serverConnection);
          }
          serverConnection.setAsTrue(RESPONDED);
        }
      } finally { // inner try-finally to ensure proper ordering of logging
        if (!(th instanceof Error || th instanceof CacheLoaderException)) {
          if (potentialModification) {
            int transId = msg != null ? msg.getTransactionId() : Integer.MIN_VALUE;
            logger.warn(String.format(
                "%s: Unexpected Exception during operation on region: %s key: %s messageId: %s",
                new Object[] {serverConnection.getName(), serverConnection.getModRegion(),
                    serverConnection.getModKey(), transId}),
                th);
          } else {
            logger.warn(String.format("%s: Unexpected Exception",
                serverConnection.getName()), th);
          }
        }
      }
    } catch (IOException ioe) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Unexpected IOException writing exception: {}", serverConnection.getName(),
            ioe.getMessage(), ioe);
      }
    } finally {
      serverConnection.setFlagProcessMessagesAsFalse();
      serverConnection.setClientDisconnectedException(th);
    }
  }

  protected static void writeChunkedException(Message origMsg, Throwable e,
      ServerConnection serverConnection) throws IOException {
    writeChunkedException(origMsg, e, serverConnection,
        serverConnection.getChunkedResponseMessage());
  }

  protected static void writeChunkedException(Message origMsg, Throwable e,
      ServerConnection serverConnection, ChunkedMessage originalResponse) throws IOException {
    writeChunkedException(origMsg, e, serverConnection, originalResponse, 2);
  }

  private static void writeChunkedException(Message origMsg, Throwable exception,
      ServerConnection serverConnection, ChunkedMessage originalResponse, int numOfParts)
      throws IOException {
    Throwable e = getClientException(serverConnection, exception);
    ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
    chunkedResponseMsg.setServerConnection(serverConnection);
    if (originalResponse.headerHasBeenSent()) {
      chunkedResponseMsg.setNumberOfParts(numOfParts);
      chunkedResponseMsg.setLastChunkAndNumParts(true, numOfParts);
      chunkedResponseMsg.addObjPart(e);
      if (numOfParts == 2) {
        chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: {}",
            serverConnection.getName(), e.getMessage(), e);
      }
    } else {
      chunkedResponseMsg.setMessageType(MessageType.EXCEPTION);
      chunkedResponseMsg.setNumberOfParts(numOfParts);
      chunkedResponseMsg.setLastChunkAndNumParts(true, numOfParts);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.addObjPart(e);
      if (numOfParts == 2) {
        chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk: {}", serverConnection.getName(), e.getMessage(),
            e);
      }
    }
    chunkedResponseMsg.sendChunk(serverConnection);
  }

  // Get the exception stacktrace for native clients
  public static String getExceptionTrace(Throwable ex) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    pw.close();
    return sw.toString();
  }

  protected static void writeException(Message origMsg, Throwable e, boolean isSevere,
      ServerConnection serverConnection) throws IOException {
    writeException(origMsg, MessageType.EXCEPTION, e, isSevere, serverConnection);
  }

  private static Throwable getClientException(ServerConnection serverConnection, Throwable e) {
    InternalCache cache = serverConnection.getCache();
    if (cache != null) {
      OldClientSupportService svc = cache.getService(OldClientSupportService.class);
      if (svc != null) {
        return svc.getThrowable(e, serverConnection.getClientVersion());
      }
    }
    return e;
  }

  protected static void writeException(Message origMsg, int msgType, Throwable e, boolean isSevere,
      ServerConnection serverConnection) throws IOException {
    Throwable theException = getClientException(serverConnection, e);
    Message errorMsg = serverConnection.getErrorResponseMessage();
    errorMsg.setMessageType(msgType);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    if (isSevere) {
      String msg = theException.getMessage();
      if (msg == null) {
        msg = theException.toString();
      }
      logger.fatal("Severe cache exception : {}", msg);
    }
    errorMsg.addObjPart(theException);
    errorMsg.addStringPart(getExceptionTrace(theException));
    errorMsg.send(serverConnection);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Wrote exception: {}", serverConnection.getName(), e.getMessage(), e);
    }
    if (e instanceof MessageTooLargeException) {
      throw (IOException) e;
    }
  }

  protected static void writeErrorResponse(Message origMsg, int messageType,
      ServerConnection serverConnection) throws IOException {
    Message errorMsg = serverConnection.getErrorResponseMessage();
    errorMsg.setMessageType(messageType);
    errorMsg.setNumberOfParts(1);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    errorMsg.addStringPart(
        "Invalid data received. Please see the cache server log file for additional details.");
    errorMsg.send(serverConnection);
  }

  protected static void writeErrorResponse(Message origMsg, int messageType, String msg,
      ServerConnection serverConnection) throws IOException {
    Message errorMsg = serverConnection.getErrorResponseMessage();
    errorMsg.setMessageType(messageType);
    errorMsg.setNumberOfParts(1);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    errorMsg.addStringPart(msg);
    errorMsg.send(serverConnection);
  }

  protected static void writeRegionDestroyedEx(Message msg, String regionName, String title,
      ServerConnection serverConnection) throws IOException {
    String reason = serverConnection.getName() + ": Region named " + regionName + title;
    RegionDestroyedException ex = new RegionDestroyedException(reason, regionName);
    if (serverConnection.getTransientFlag(REQUIRES_CHUNKED_RESPONSE)) {
      writeChunkedException(msg, ex, serverConnection);
    } else {
      writeException(msg, ex, false, serverConnection);
    }
  }

  protected static void writeResponse(Object data, Object callbackArg, Message origMsg,
      boolean isObject, ServerConnection serverConnection) throws IOException {
    Message responseMsg = serverConnection.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());


    if (callbackArg == null) {
      responseMsg.setNumberOfParts(1);
    } else {
      responseMsg.setNumberOfParts(2);
    }
    if (data instanceof byte[]) {
      responseMsg.addRawPart((byte[]) data, isObject);
    } else {
      Assert.assertTrue(isObject, "isObject should be true when value is not a byte[]");
      responseMsg.addObjPart(data, false);
    }
    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(serverConnection);
    origMsg.clearParts();
  }

  protected static void writeResponseWithRefreshMetadata(Object data, Object callbackArg,
      Message origMsg, boolean isObject, ServerConnection serverConnection, PartitionedRegion pr,
      byte nwHop) throws IOException {
    Message responseMsg = serverConnection.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    if (callbackArg == null) {
      responseMsg.setNumberOfParts(2);
    } else {
      responseMsg.setNumberOfParts(3);
    }

    if (data instanceof byte[]) {
      responseMsg.addRawPart((byte[]) data, isObject);
    } else {
      Assert.assertTrue(isObject, "isObject should be true when value is not a byte[]");
      responseMsg.addObjPart(data, false);
    }
    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    responseMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(serverConnection);
    origMsg.clearParts();
  }

  protected static void writeResponseWithFunctionAttribute(byte[] data, Message origMsg,
      ServerConnection serverConnection) throws IOException {
    Message responseMsg = serverConnection.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.setNumberOfParts(1);
    responseMsg.addBytesPart(data);
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(serverConnection);
    origMsg.clearParts();
  }

  protected static void checkForInterrupt(ServerConnection serverConnection, Exception e)
      throws InterruptedException, InterruptedIOException {
    serverConnection.getCachedRegionHelper().checkCancelInProgress(e);
    if (e instanceof InterruptedException) {
      throw (InterruptedException) e;
    }
    if (e instanceof InterruptedIOException) {
      throw (InterruptedIOException) e;
    }
  }

  static void writeQueryResponseChunk(Object queryResponseChunk, CollectionType collectionType,
      boolean lastChunk, ServerConnection serverConnection) throws IOException {
    ChunkedMessage queryResponseMsg = serverConnection.getQueryResponseMessage();
    queryResponseMsg.setNumberOfParts(2);
    queryResponseMsg.setLastChunk(lastChunk);
    queryResponseMsg.addObjPart(collectionType, false);
    queryResponseMsg.addObjPart(queryResponseChunk, false);
    queryResponseMsg.sendChunk(serverConnection);
  }

  protected static void writeQueryResponseException(Message origMsg, Throwable exception,
      ServerConnection serverConnection) throws IOException {
    Throwable e = getClientException(serverConnection, exception);
    ChunkedMessage queryResponseMsg = serverConnection.getQueryResponseMessage();
    ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
    if (queryResponseMsg.headerHasBeenSent()) {
      // fix for bug 35442
      // This client is expecting 2 parts in this message so send 2 parts
      queryResponseMsg.setServerConnection(serverConnection);
      queryResponseMsg.setNumberOfParts(2);
      queryResponseMsg.setLastChunkAndNumParts(true, 2);
      queryResponseMsg.addObjPart(e);
      queryResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: {}",
            serverConnection.getName(), e.getMessage(), e);
      }
      queryResponseMsg.sendChunk(serverConnection);
    } else {
      chunkedResponseMsg.setServerConnection(serverConnection);
      chunkedResponseMsg.setMessageType(MessageType.EXCEPTION);
      chunkedResponseMsg.setNumberOfParts(2);
      chunkedResponseMsg.setLastChunkAndNumParts(true, 2);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.addObjPart(e);
      chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk: {}", serverConnection.getName(), e.getMessage(),
            e);
      }
      chunkedResponseMsg.sendChunk(serverConnection);
    }
  }

  protected static void writeChunkedErrorResponse(Message origMsg, int messageType, String message,
      ServerConnection serverConnection) throws IOException {
    // Send chunked response header identifying error message
    ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending error message header type: {} transaction: {}",
          serverConnection.getName(), messageType, origMsg.getTransactionId());
    }
    chunkedResponseMsg.setMessageType(messageType);
    chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
    chunkedResponseMsg.sendHeader();

    // Send actual error
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending error message chunk: {}", serverConnection.getName(), message);
    }
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(true);
    chunkedResponseMsg.addStringPart(message);
    chunkedResponseMsg.sendChunk(serverConnection);
  }

  protected static void writeFunctionResponseException(Message origMsg, int messageType,
      ServerConnection serverConnection, Throwable exception) throws IOException {
    Throwable e = getClientException(serverConnection, exception);
    ChunkedMessage functionResponseMsg = serverConnection.getFunctionResponseMessage();
    ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
    if (functionResponseMsg.headerHasBeenSent()) {
      functionResponseMsg.setServerConnection(serverConnection);
      functionResponseMsg.setNumberOfParts(2);
      functionResponseMsg.setLastChunkAndNumParts(true, 2);
      functionResponseMsg.addObjPart(e);
      functionResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: {}",
            serverConnection.getName(), e.getMessage(), e);
      }
      functionResponseMsg.sendChunk(serverConnection);
    } else {
      chunkedResponseMsg.setServerConnection(serverConnection);
      chunkedResponseMsg.setMessageType(messageType);
      chunkedResponseMsg.setNumberOfParts(2);
      chunkedResponseMsg.setLastChunkAndNumParts(true, 2);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.addObjPart(e);
      chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk: {}", serverConnection.getName(), e.getMessage(),
            e);
      }
      chunkedResponseMsg.sendChunk(serverConnection);
    }
  }

  protected static void writeFunctionResponseError(Message origMsg, int messageType, String message,
      ServerConnection servConn) throws IOException {
    ChunkedMessage functionResponseMsg = servConn.getFunctionResponseMessage();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    if (functionResponseMsg.headerHasBeenSent()) {
      functionResponseMsg.setNumberOfParts(1);
      functionResponseMsg.setLastChunk(true);
      functionResponseMsg.addStringPart(message);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending Error chunk while reply in progress: {}", servConn.getName(),
            message);
      }
      functionResponseMsg.sendChunk(servConn);
    } else {
      chunkedResponseMsg.setMessageType(messageType);
      chunkedResponseMsg.setNumberOfParts(1);
      chunkedResponseMsg.setLastChunk(true);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.addStringPart(message);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending Error chunk: {}", servConn.getName(), message);
      }
      chunkedResponseMsg.sendChunk(servConn);
    }
  }

  protected static void writeKeySetErrorResponse(Message origMsg, int messageType, String message,
      ServerConnection servConn) throws IOException {
    // Send chunked response header identifying error message
    ChunkedMessage chunkedResponseMsg = servConn.getKeySetResponseMessage();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending error message header type: {} transaction: {}", servConn.getName(),
          messageType, origMsg.getTransactionId());
    }
    chunkedResponseMsg.setMessageType(messageType);
    chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
    chunkedResponseMsg.sendHeader();
    // Send actual error
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending error message chunk: {}", servConn.getName(), message);
    }
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(true);
    chunkedResponseMsg.addStringPart(message);
    chunkedResponseMsg.sendChunk(servConn);
  }

  static Message readRequest(ServerConnection servConn) {
    Message requestMsg = null;
    try {
      requestMsg = servConn.getRequestMessage();
      requestMsg.receive(servConn, MAX_INCOMING_DATA, INCOMING_DATA_LIMITER, INCOMING_MSG_LIMITER);
      return requestMsg;
    } catch (EOFException eof) {
      handleEOFException(null, servConn, eof);
      // TODO: Check if there is any need for explicitly returning

    } catch (InterruptedIOException e) { // Solaris only
      handleInterruptedIOException(servConn, e);

    } catch (IOException e) {
      handleIOException(null, servConn, e);

    } catch (DistributedSystemDisconnectedException e) {
      handleShutdownException(null, servConn, e);

    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable e) {
      SystemFailure.checkFailure();
      handleThrowable(null, servConn, e);
    }
    return requestMsg;
  }

  protected static void fillAndSendRegisterInterestResponseChunks(LocalRegion region, Object riKey,
      int interestType, InterestResultPolicy policy, ServerConnection servConn) throws IOException {
    fillAndSendRegisterInterestResponseChunks(region, riKey, interestType, false, policy, servConn);
  }

  /**
   * serializeValues is unused for clients < GFE_80
   */
  protected static void fillAndSendRegisterInterestResponseChunks(LocalRegion region, Object riKey,
      int interestType, boolean serializeValues, InterestResultPolicy policy,
      ServerConnection servConn) throws IOException {
    // Client is not interested.
    if (policy.isNone()) {
      sendRegisterInterestResponseChunk(region, riKey, new ArrayList(), true, servConn);
      return;
    }
    if (policy.isKeysValues() && servConn.getClientVersion().compareTo(Version.GFE_80) >= 0) {
      handleKeysValuesPolicy(region, riKey, interestType, serializeValues, servConn);
      return;
    }
    if (riKey instanceof List) {
      handleList(region, (List) riKey, policy, servConn);
      return;
    }
    if (!(riKey instanceof String)) {
      handleSingleton(region, riKey, policy, servConn);
      return;
    }

    switch (interestType) {
      case InterestType.OQL_QUERY:
        // Not supported yet
        throw new InternalGemFireError(
            "not yet supported");

      case InterestType.FILTER_CLASS:
        throw new InternalGemFireError(
            "not yet supported");

      case InterestType.REGULAR_EXPRESSION:
        String regEx = (String) riKey;
        if (regEx.equals(".*")) {
          handleAllKeys(region, policy, servConn);
        } else {
          handleRegEx(region, regEx, policy, servConn);
        }
        break;

      case InterestType.KEY:
        if (riKey.equals("ALL_KEYS")) {
          handleAllKeys(region, policy, servConn);
        } else {
          handleSingleton(region, riKey, policy, servConn);
        }
        break;

      default:
        throw new InternalGemFireError(
            "unknown interest type");
    }
  }

  private static void handleKeysValuesPolicy(LocalRegion region, Object riKey, int interestType,
      boolean serializeValues, ServerConnection servConn) throws IOException {
    if (riKey instanceof List) {
      handleKVList(region, (List) riKey, serializeValues, servConn);
      return;
    }
    if (!(riKey instanceof String)) {
      handleKVSingleton(region, riKey, serializeValues, servConn);
      return;
    }

    switch (interestType) {
      case InterestType.OQL_QUERY:
        throw new InternalGemFireError(
            "not yet supported");

      case InterestType.FILTER_CLASS:
        throw new InternalGemFireError(
            "not yet supported");

      case InterestType.REGULAR_EXPRESSION:
        String regEx = (String) riKey;
        if (regEx.equals(".*")) {
          handleKVAllKeys(region, null, serializeValues, servConn);
        } else {
          handleKVAllKeys(region, regEx, serializeValues, servConn);
        }
        break;

      case InterestType.KEY:
        if (riKey.equals("ALL_KEYS")) {
          handleKVAllKeys(region, null, serializeValues, servConn);
        } else {
          handleKVSingleton(region, riKey, serializeValues, servConn);
        }
        break;

      default:
        throw new InternalGemFireError(
            "unknown interest type");
    }
  }

  /**
   * @param list is a List of entry keys
   */
  private static void sendRegisterInterestResponseChunk(Region region, Object riKey, List list,
      boolean lastChunk, ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getRegisterInterestResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, false);
    String regionName = region == null ? " null " : region.getFullPath();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Sending{}register interest response chunk for region: {} for keys: {} chunk=<{}>",
          servConn.getName(), lastChunk ? " last " : " ", regionName, riKey, chunkedResponseMsg);
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

  /**
   * Determines whether keys for destroyed entries (tombstones) should be sent to clients in
   * register-interest results.
   *
   * @return true if tombstones should be sent to the client
   */
  private static boolean sendTombstonesInRIResults(ServerConnection servConn,
      InterestResultPolicy policy) {
    return policy == InterestResultPolicy.KEYS_VALUES
        && servConn.getClientVersion().compareTo(Version.GFE_80) >= 0;
  }

  /**
   * Process an interest request involving a list of keys
   *
   * @param region the region
   * @param keyList the list of keys
   * @param policy the policy
   */
  private static void handleList(LocalRegion region, List keyList, InterestResultPolicy policy,
      ServerConnection servConn) throws IOException {
    if (region instanceof PartitionedRegion) {
      // too bad java doesn't provide another way to do this...
      handleListPR((PartitionedRegion) region, keyList, policy, servConn);
      return;
    }
    List newKeyList = new ArrayList(MAXIMUM_CHUNK_SIZE);
    // Handle list of keys
    if (region != null) {
      for (Object entryKey : keyList) {
        if (region.containsKey(entryKey)
            || sendTombstonesInRIResults(servConn, policy) && region.containsTombstone(entryKey)) {

          appendInterestResponseKey(region, keyList, entryKey, newKeyList, servConn);
        }
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, keyList, newKeyList, true, servConn);
  }

  /**
   * Handles both RR and PR cases
   */
  @SuppressWarnings(value = "NP_NULL_PARAM_DEREF",
      justification = "Null value handled in sendNewRegisterInterestResponseChunk()")
  private static void handleKVSingleton(LocalRegion region, Object entryKey,
      boolean serializeValues, ServerConnection servConn) throws IOException {
    VersionedObjectList values = new VersionedObjectList(MAXIMUM_CHUNK_SIZE, true,
        region == null || region.getAttributes().getConcurrencyChecksEnabled(), serializeValues);

    if (region != null) {
      if (region.containsKey(entryKey) || region.containsTombstone(entryKey)) {
        VersionTagHolder versionHolder = createVersionTagHolder();
        ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
        // From Get70.getValueAndIsObject()
        Object data = region.get(entryKey, null, true, true, true, id, versionHolder, true);
        VersionTag vt = versionHolder.getVersionTag();

        updateValues(values, entryKey, data, vt);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendNewRegisterInterestResponseChunk(region, entryKey, values, true, servConn);
  }

  /**
   * Process an interest request consisting of a single key
   *
   * @param region the region
   * @param entryKey the key
   * @param policy the policy
   */
  private static void handleSingleton(LocalRegion region, Object entryKey,
      InterestResultPolicy policy, ServerConnection servConn) throws IOException {
    List keyList = new ArrayList(1);
    if (region != null) {
      if (region.containsKey(entryKey)
          || sendTombstonesInRIResults(servConn, policy) && region.containsTombstone(entryKey)) {
        appendInterestResponseKey(region, entryKey, entryKey, keyList, servConn);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, entryKey, keyList, true, servConn);
  }

  /**
   * Process an interest request of type ALL_KEYS
   *
   * @param region the region
   * @param policy the policy
   */
  private static void handleAllKeys(LocalRegion region, InterestResultPolicy policy,
      ServerConnection servConn) throws IOException {
    List keyList = new ArrayList(MAXIMUM_CHUNK_SIZE);
    if (region != null) {
      for (Object entryKey : region.keySet(sendTombstonesInRIResults(servConn, policy))) {
        appendInterestResponseKey(region, "ALL_KEYS", entryKey, keyList, servConn);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, "ALL_KEYS", keyList, true, servConn);
  }

  private static void handleKVAllKeys(LocalRegion region, String regex, boolean serializeValues,
      ServerConnection servConn) throws IOException {

    if (region instanceof PartitionedRegion) {
      handleKVKeysPR((PartitionedRegion) region, regex, serializeValues, servConn);
      return;
    }

    VersionedObjectList values = new VersionedObjectList(MAXIMUM_CHUNK_SIZE, true,
        region == null || region.getAttributes().getConcurrencyChecksEnabled(), serializeValues);

    if (region != null) {

      Pattern keyPattern = null;
      if (regex != null) {
        keyPattern = Pattern.compile(regex);
      }

      for (Object key : region.keySet(true)) {
        VersionTagHolder versionHolder = createVersionTagHolder();
        if (keyPattern != null) {
          if (!(key instanceof String)) {
            // key is not a String, cannot apply regex to this entry
            continue;
          }
          if (!keyPattern.matcher((String) key).matches()) {
            // key does not match the regex, this entry should not be
            // returned.
            continue;
          }
        }

        ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
        Object data = region.get(key, null, true, true, true, id, versionHolder, true);
        VersionTag versionTag = versionHolder.getVersionTag();
        updateValues(values, key, data, versionTag);

        if (values.size() == MAXIMUM_CHUNK_SIZE) {
          sendNewRegisterInterestResponseChunk(region, regex != null ? regex : "ALL_KEYS", values,
              false, servConn);
          values.clear();
        }
      } // for
    } // if

    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendNewRegisterInterestResponseChunk(region, regex != null ? regex : "ALL_KEYS", values, true,
        servConn);
  }

  private static void handleKVKeysPR(PartitionedRegion region, Object keyInfo,
      boolean serializeValues, ServerConnection servConn) throws IOException {

    VersionedObjectList values = new VersionedObjectList(MAXIMUM_CHUNK_SIZE, true,
        region.getConcurrencyChecksEnabled(), serializeValues);

    if (keyInfo instanceof List) {
      HashMap<Integer, HashSet> bucketKeys = new HashMap<>();
      for (Object key : (List) keyInfo) {
        int id = PartitionedRegionHelper.getHashKey(region, null, key, null, null);
        if (bucketKeys.containsKey(id)) {
          bucketKeys.get(id).add(key);
        } else {
          HashSet<Object> keys = new HashSet<>();
          keys.add(key);
          bucketKeys.put(id, keys);
        }
      }
      region.fetchEntries(bucketKeys, values, servConn);
    } else { // keyInfo is a String
      region.fetchEntries((String) keyInfo, values, servConn);
    }

    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendNewRegisterInterestResponseChunk(region, keyInfo != null ? keyInfo : "ALL_KEYS", values,
        true, servConn);
  }

  /**
   * Copied from Get70.getValueAndIsObject(), except a minor change. (Make the method static instead
   * of copying it here?)
   */
  private static void updateValues(VersionedObjectList values, Object key, Object value,
      VersionTag versionTag) {
    boolean isObject = true;

    // If the value in the VM is a CachedDeserializable,
    // get its value. If it is Token.REMOVED, Token.DESTROYED,
    // Token.INVALID, or Token.LOCAL_INVALID
    // set it to null. If it is NOT_AVAILABLE, get the value from
    // disk. If it is already a byte[], set isObject to false.
    boolean wasInvalid = false;
    if (value instanceof CachedDeserializable) {
      value = ((CachedDeserializable) value).getValue();
    } else if (isRemovalToken(value)) {
      value = null;
    } else if (value == Token.INVALID || value == Token.LOCAL_INVALID) {
      value = null; // fix for bug 35884
      wasInvalid = true;
    } else if (value instanceof byte[]) {
      isObject = false;
    }
    boolean keyNotPresent = !wasInvalid && (value == null || value == Token.TOMBSTONE);

    if (keyNotPresent) {
      values.addObjectPartForAbsentKey(key, value, versionTag);
    } else {
      values.addObjectPart(key, value, isObject, versionTag);
    }
  }

  private static boolean isRemovalToken(final Object value) {
    return value == Token.REMOVED_PHASE1 || value == Token.REMOVED_PHASE2
        || value == Token.DESTROYED || value == Token.TOMBSTONE;
  }

  public static void appendNewRegisterInterestResponseChunkFromLocal(LocalRegion region,
      VersionedObjectList values, Object riKeys, Set keySet, ServerConnection servConn)
      throws IOException {
    ClientProxyMembershipID requestingClient = servConn == null ? null : servConn.getProxyID();
    for (Object key : keySet) {
      VersionTagHolder versionHolder = createVersionTagHolder();

      Object value = region.get(key, null, true, true, true, requestingClient, versionHolder, true);

      updateValues(values, key, value, versionHolder.getVersionTag());

      if (values.size() == MAXIMUM_CHUNK_SIZE) {
        // Send the chunk and clear the list
        // values.setKeys(null); // Now we need to send keys too.
        sendNewRegisterInterestResponseChunk(region, riKeys != null ? riKeys : "ALL_KEYS", values,
            false, servConn);
        values.clear();
      }
    } // for
  }

  public static void appendNewRegisterInterestResponseChunk(LocalRegion region,
      VersionedObjectList values, Object riKeys, Set<Map.Entry> set, ServerConnection servConn)
      throws IOException {
    for (Entry entry : set) {
      if (entry instanceof Region.Entry) { // local entries
        VersionTag vt;
        Object key;
        Object value;
        if (entry instanceof EntrySnapshot) {
          vt = ((EntrySnapshot) entry).getVersionTag();
          key = ((EntrySnapshot) entry).getRegionEntry().getKey();
          value = ((EntrySnapshot) entry).getRegionEntry().getValue(null);
          updateValues(values, key, value, vt);
        } else {
          VersionStamp vs = ((NonTXEntry) entry).getRegionEntry().getVersionStamp();
          vt = vs == null ? null : vs.asVersionTag();
          key = entry.getKey();
          value = ((NonTXEntry) entry).getRegionEntry().getValueRetain(region, true);
          try {
            updateValues(values, key, value, vt);
          } finally {
            OffHeapHelper.release(value);
          }
        }
      } else { // Map.Entry (remote entries)
        List list = (List) entry.getValue();
        Object value = list.get(0);
        VersionTag tag = (VersionTag) list.get(1);
        updateValues(values, entry.getKey(), value, tag);
      }
      if (values.size() == MAXIMUM_CHUNK_SIZE) {
        // Send the chunk and clear the list
        sendNewRegisterInterestResponseChunk(region, riKeys != null ? riKeys : "ALL_KEYS", values,
            false, servConn);
        values.clear();
      }
    } // for
  }

  public static void sendNewRegisterInterestResponseChunk(LocalRegion region, Object riKey,
      VersionedObjectList list, boolean lastChunk, ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getRegisterInterestResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, false);
    String regionName = region == null ? " null " : region.getFullPath();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Sending{}register interest response chunk for region: {} for keys: {} chunk=<{}>",
          servConn.getName(), lastChunk ? " last " : " ", regionName, riKey, chunkedResponseMsg);
    }
    chunkedResponseMsg.sendChunk(servConn);
  }

  /**
   * Process an interest request of type {@link InterestType#REGULAR_EXPRESSION}
   */
  private static void handleRegEx(LocalRegion region, String regex, InterestResultPolicy policy,
      ServerConnection servConn) throws IOException {
    if (region instanceof PartitionedRegion) {
      // too bad java doesn't provide another way to do this...
      handleRegExPR((PartitionedRegion) region, regex, policy, servConn);
      return;
    }
    List keyList = new ArrayList(MAXIMUM_CHUNK_SIZE);
    // Handle the regex pattern
    if (region != null) {
      Pattern keyPattern = Pattern.compile(regex);
      for (Object entryKey : region.keySet(sendTombstonesInRIResults(servConn, policy))) {
        if (!(entryKey instanceof String)) {
          // key is not a String, cannot apply regex to this entry
          continue;
        }
        if (!keyPattern.matcher((String) entryKey).matches()) {
          // key does not match the regex, this entry should not be returned.
          continue;
        }

        appendInterestResponseKey(region, regex, entryKey, keyList, servConn);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, regex, keyList, true, servConn);
  }

  /**
   * Process an interest request of type {@link InterestType#REGULAR_EXPRESSION}
   */
  private static void handleRegExPR(final PartitionedRegion region, final String regex,
      final InterestResultPolicy policy, final ServerConnection servConn) throws IOException {
    final List keyList = new ArrayList(MAXIMUM_CHUNK_SIZE);
    region.getKeysWithRegEx(regex, sendTombstonesInRIResults(servConn, policy),
        new PartitionedRegion.SetCollector() {
          @Override
          public void receiveSet(Set theSet) throws IOException {
            appendInterestResponseKeys(region, regex, theSet, keyList, servConn);
          }
        });
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, regex, keyList, true, servConn);
  }

  /**
   * Process an interest request involving a list of keys
   */
  private static void handleListPR(final PartitionedRegion region, final List keyList,
      final InterestResultPolicy policy, final ServerConnection servConn) throws IOException {
    final List newKeyList = new ArrayList(MAXIMUM_CHUNK_SIZE);
    region.getKeysWithList(keyList, sendTombstonesInRIResults(servConn, policy),
        new PartitionedRegion.SetCollector() {
          @Override
          public void receiveSet(Set theSet) throws IOException {
            appendInterestResponseKeys(region, keyList, theSet, newKeyList, servConn);
          }
        });
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, keyList, newKeyList, true, servConn);
  }

  private static void handleKVList(final LocalRegion region, final List keyList,
      boolean serializeValues, final ServerConnection servConn) throws IOException {

    if (region instanceof PartitionedRegion) {
      handleKVKeysPR((PartitionedRegion) region, keyList, serializeValues, servConn);
      return;
    }
    VersionedObjectList values = new VersionedObjectList(MAXIMUM_CHUNK_SIZE, true,
        region == null || region.getAttributes().getConcurrencyChecksEnabled(), serializeValues);

    // Handle list of keys
    if (region != null) {

      for (Object key : keyList) {
        if (region.containsKey(key) || region.containsTombstone(key)) {
          VersionTagHolder versionHolder = createVersionTagHolder();

          ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
          Object data = region.get(key, null, true, true, true, id, versionHolder, true);
          VersionTag versionTag = versionHolder.getVersionTag();
          updateValues(values, key, data, versionTag);

          if (values.size() == MAXIMUM_CHUNK_SIZE) {
            // Send the chunk and clear the list
            // values.setKeys(null); // Now we need to send keys too.
            sendNewRegisterInterestResponseChunk(region, keyList, values, false, servConn);
            values.clear();
          }
        }
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendNewRegisterInterestResponseChunk(region, keyList, values, true, servConn);
  }

  private static VersionTagHolder createVersionTagHolder() {
    VersionTagHolder versionHolder = new VersionTagHolder();
    versionHolder.setOperation(Operation.GET_FOR_REGISTER_INTEREST);
    return versionHolder;
  }

  /**
   * Append an interest response
   *
   * @param region the region (for debugging)
   * @param riKey the registerInterest "key" (what the client is interested in)
   * @param entryKey key we're responding to
   * @param list list to append to
   */
  private static void appendInterestResponseKey(LocalRegion region, Object riKey, Object entryKey,
      List list, ServerConnection servConn) throws IOException {
    list.add(entryKey);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: appendInterestResponseKey <{}>; list size was {}; region: {}",
          servConn.getName(), entryKey, list.size(), region.getFullPath());
    }
    if (list.size() == MAXIMUM_CHUNK_SIZE) {
      // Send the chunk and clear the list
      sendRegisterInterestResponseChunk(region, riKey, list, false, servConn);
      list.clear();
    }
  }

  private static void appendInterestResponseKeys(LocalRegion region, Object riKey,
      Collection entryKeys, List collector, ServerConnection servConn) throws IOException {
    for (final Object entryKey : entryKeys) {
      appendInterestResponseKey(region, riKey, entryKey, collector, servConn);
    }
  }
}
