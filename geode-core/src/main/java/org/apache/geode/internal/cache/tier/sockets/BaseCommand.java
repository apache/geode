/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package org.apache.geode.internal.cache.tier.sockets;

import static com.sun.corba.se.impl.util.RepositoryId.cache;

import org.apache.geode.*;
import org.apache.geode.cache.*;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.LocalRegion.NonTXEntry;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.security.GemFireSecurityException;

import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

public abstract class BaseCommand implements Command {
  protected static final Logger logger = LogService.getLogger();

  /**
   * Whether zipped values are being passed to/from the client. Can be modified
   * using the system property Message.ZIP_VALUES ? This does not appear to
   * happen anywhere
   */
  protected static final boolean zipValues = false;

  protected static final boolean APPLY_RETRIES = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "gateway.ApplyRetries");

  public static final byte[] OK_BYTES = new byte[]{0};  

  public static final int maximumChunkSize = Integer.getInteger(
      "BridgeServer.MAXIMUM_CHUNK_SIZE", 100).intValue();

  /** Maximum number of entries in each chunked response chunk */

  /** Whether to suppress logging of IOExceptions */
  private static boolean suppressIOExceptionLogging = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "bridge.suppressIOExceptionLogging");

  /**
   * Maximum number of concurrent incoming client message bytes that a bridge
   * server will allow. Once a server is working on this number additional
   * incoming client messages will wait until one of them completes or fails.
   * The bytes are computed based in the size sent in the incoming msg header.
   */
  private static final int MAX_INCOMING_DATA = Integer.getInteger(
      "BridgeServer.MAX_INCOMING_DATA", -1).intValue();

  /**
   * Maximum number of concurrent incoming client messages that a bridge server
   * will allow. Once a server is working on this number additional incoming
   * client messages will wait until one of them completes or fails.
   */
  private static final int MAX_INCOMING_MSGS = Integer.getInteger(
      "BridgeServer.MAX_INCOMING_MSGS", -1).intValue();

  private static final Semaphore incomingDataLimiter;

  private static final Semaphore incomingMsgLimiter;
  static {
    Semaphore tmp;
    if (MAX_INCOMING_DATA > 0) {
      // backport requires that this is fair since we inc by values > 1
      tmp = new Semaphore(MAX_INCOMING_DATA, true);
    }
    else {
      tmp = null;
    }
    incomingDataLimiter = tmp;
    if (MAX_INCOMING_MSGS > 0) {
      tmp = new Semaphore(MAX_INCOMING_MSGS, false); // unfair for best
      // performance
    }
    else {
      tmp = null;
    }
    incomingMsgLimiter = tmp;

  }

  protected SecurityService securityService = IntegratedSecurityService.getSecurityService();

  final public void execute(Message msg, ServerConnection servConn) {
    // Read the request and update the statistics
    long start = DistributionStats.getStatTime();
    //servConn.resetTransientData();
    if(EntryLogger.isEnabled() && servConn  != null) {
      EntryLogger.setSource(servConn.getMembershipID(), "c2s");
    }
    boolean shouldMasquerade = shouldMasqueradeForTx(msg, servConn);
    try {
      if (shouldMasquerade) {
        GemFireCacheImpl  cache = (GemFireCacheImpl)servConn.getCache();
        InternalDistributedMember member = (InternalDistributedMember)servConn.getProxyID().getDistributedMember();
        TXManagerImpl txMgr = cache.getTxManager();
        TXStateProxy tx = null;
        try {
          tx = txMgr.masqueradeAs(msg, member, false);
          cmdExecute(msg, servConn, start);
          tx.updateProxyServer(txMgr.getMemberId());
        } finally {
          txMgr.unmasquerade(tx);
        }
      } else {
        cmdExecute(msg, servConn, start);
      }
      
    }   
    catch (TransactionException
        | CopyException
        | SerializationException
        | CacheWriterException
        | CacheLoaderException
        | GemFireSecurityException
        | PartitionOfflineException
        | MessageTooLargeException e) {
      handleExceptionNoDisconnect(msg, servConn, e);
    }
    catch (EOFException eof) {
      BaseCommand.handleEOFException(msg, servConn, eof);
    }
    catch (InterruptedIOException e) { // Solaris only
      BaseCommand.handleInterruptedIOException(msg, servConn, e);
    }
    catch (IOException e) {
      BaseCommand.handleIOException(msg, servConn, e);
    }
    catch (DistributedSystemDisconnectedException e) {
      BaseCommand.handleShutdownException(msg, servConn, e);
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable e) {
      BaseCommand.handleThrowable(msg, servConn, e);
    } finally {
      EntryLogger.clearSource();
    }
  }

  /**
   * checks to see if this thread needs to masquerade as a transactional thread.
   * clients after GFE_66 should be able to start a transaction.
   * @param msg
   * @param servConn
   * @return true if thread should masquerade as a transactional thread.
   */
  protected boolean shouldMasqueradeForTx(Message msg, ServerConnection servConn) {
    if (servConn.getClientVersion().compareTo(Version.GFE_66) >= 0
        && msg.getTransactionId() > TXManagerImpl.NOTX) {
      return true;
    }
    return false;
  }
  
  /**
   * If an operation is retried then some server may have seen it already.
   * We cannot apply this operation to the cache without knowing whether a
   * version tag has already been created for it.  Otherwise caches that have
   * seen the event already will reject it but others will not, but will have
   * no version tag with which to perform concurrency checks.
   * <p>The client event should have the event identifier from the client and
   * the region affected by the operation.
   * @param clientEvent
   */
  public boolean recoverVersionTagForRetriedOperation(EntryEventImpl clientEvent) {
    LocalRegion r = clientEvent.getRegion();
    VersionTag tag =  null;
    if ((clientEvent.getVersionTag() != null) && (clientEvent.getVersionTag().isGatewayTag())) {
      tag = r.findVersionTagForGatewayEvent(clientEvent.getEventId());
    }
    else {
      tag = r.findVersionTagForClientEvent(clientEvent.getEventId());
    }
    if (tag == null) {
      if (r instanceof DistributedRegion || r instanceof PartitionedRegion) {
        // TODO this could be optimized for partitioned regions by sending the key
        // so that the PR could look at an individual bucket for the event
        tag = FindVersionTagOperation.findVersionTag(r, clientEvent.getEventId(), false);
      }
    }
    if (tag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("recovered version tag {} for replayed operation {}", tag, clientEvent.getEventId());
      }
      clientEvent.setVersionTag(tag);
    }
    return (tag != null);
  }
  
  /**
   * If an operation is retried then some server may have seen it already.
   * We cannot apply this operation to the cache without knowing whether a
   * version tag has already been created for it.  Otherwise caches that have
   * seen the event already will reject it but others will not, but will have
   * no version tag with which to perform concurrency checks.
   * <p>The client event should have the event identifier from the client and
   * the region affected by the operation.
   */
  protected VersionTag findVersionTagsForRetriedBulkOp(LocalRegion r, EventID eventID) {
    VersionTag tag = r.findVersionTagForClientBulkOp(eventID);
    if(tag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("recovered version tag {} for replayed bulk operation {}", tag, eventID);
      }
      return tag;
    }
    if (r instanceof DistributedRegion || r instanceof PartitionedRegion) {
      // TODO this could be optimized for partitioned regions by sending the key
      // so that the PR could look at an individual bucket for the event
      tag = FindVersionTagOperation.findVersionTag(r, eventID, true);
    }
    if (tag != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("recovered version tag {} for replayed bulk operation {}", tag, eventID);
      }
    }
    return tag;
  }

  abstract public void cmdExecute(Message msg, ServerConnection servConn,
      long start) throws IOException, ClassNotFoundException, InterruptedException;

  protected void writeReply(Message origMsg, ServerConnection servConn)
      throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(OK_BYTES);
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr, byte nwHop) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion(), nwHop});
    replyMsg.send(servConn);
    pr.getPrStats().incPRMetaDataSentCount();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADAT tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }

  private static void handleEOFException(Message msg,
      ServerConnection servConn, Exception eof) {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    boolean potentialModification = servConn.getPotentialModification();
    if (!crHelper.isShutdown()) {
      if (potentialModification) {
        stats.incAbandonedWriteRequests();
      }
      else {
        stats.incAbandonedReadRequests();
      }
      if (!suppressIOExceptionLogging) {
        if (potentialModification) {
          int transId = (msg != null) ? msg.getTransactionId()
              : Integer.MIN_VALUE;
          logger.warn(LocalizedMessage.create(
            LocalizedStrings.BaseCommand_0_EOFEXCEPTION_DURING_A_WRITE_OPERATION_ON_REGION__1_KEY_2_MESSAGEID_3,
            new Object[] {servConn.getName(), servConn.getModRegion(), servConn.getModKey(), Integer.valueOf(transId)}));
        }
        else {
          logger.debug("EOF exception", eof);
          logger.info(LocalizedMessage.create(
            LocalizedStrings.BaseCommand_0_CONNECTION_DISCONNECT_DETECTED_BY_EOF,
            servConn.getName()));
        }
      }
    }
    servConn.setFlagProcessMessagesAsFalse();
  }

  private static void handleInterruptedIOException(Message msg,
      ServerConnection servConn, Exception e) {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    if (!crHelper.isShutdown() && servConn.isOpen()) {
      if (!suppressIOExceptionLogging) {
        if (logger.isDebugEnabled())
          logger.debug("Aborted message due to interrupt: {}", e.getMessage(), e);
      }
    }
    servConn.setFlagProcessMessagesAsFalse();
  }

  private static void handleIOException(Message msg, ServerConnection servConn,
      Exception e) {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    boolean potentialModification = servConn.getPotentialModification();

    if (!crHelper.isShutdown() && servConn.isOpen()) {
      if (!suppressIOExceptionLogging) {
        if (potentialModification) {
          int transId = (msg != null) ? msg.getTransactionId()
              : Integer.MIN_VALUE;
          logger.warn(LocalizedMessage.create(
            LocalizedStrings.BaseCommand_0_UNEXPECTED_IOEXCEPTION_DURING_OPERATION_FOR_REGION_1_KEY_2_MESSID_3,
            new Object[] {servConn.getName(), servConn.getModRegion(), servConn.getModKey(), Integer.valueOf(transId)}), e);
        }
        else {
          logger.warn(LocalizedMessage.create(
            LocalizedStrings.BaseCommand_0_UNEXPECTED_IOEXCEPTION,
            servConn.getName()), e);
        }
      }
    }
    servConn.setFlagProcessMessagesAsFalse();
  }

  private static void handleShutdownException(Message msg,
      ServerConnection servConn, Exception e) {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    boolean potentialModification = servConn.getPotentialModification();

    if (!crHelper.isShutdown()) {
      if (potentialModification) {
        int transId = (msg != null) ? msg.getTransactionId()
            : Integer.MIN_VALUE;
        logger.warn(LocalizedMessage.create(
          LocalizedStrings.BaseCommand_0_UNEXPECTED_SHUTDOWNEXCEPTION_DURING_OPERATION_ON_REGION_1_KEY_2_MESSAGEID_3,
          new Object[] {servConn.getName(), servConn.getModRegion(), servConn.getModKey(), Integer.valueOf(transId)}), e);
      }
      else {
        logger.warn(LocalizedMessage.create(
          LocalizedStrings.BaseCommand_0_UNEXPECTED_SHUTDOWNEXCEPTION,
          servConn.getName()),e);
        }
    }
    servConn.setFlagProcessMessagesAsFalse();
  }

  // Handle GemfireSecurityExceptions separately since the connection should not
  // be terminated (by setting processMessages to false) unlike in
  // handleThrowable. Fixes bugs #38384 and #39392.
//  private static void handleGemfireSecurityException(Message msg,
//      ServerConnection servConn, GemFireSecurityException e) {
//
//    boolean requiresResponse = servConn.getTransientFlag(REQUIRES_RESPONSE);
//    boolean responded = servConn.getTransientFlag(RESPONDED);
//    boolean requiresChunkedResponse = servConn
//        .getTransientFlag(REQUIRES_CHUNKED_RESPONSE);
//    boolean potentialModification = servConn.getPotentialModification();
//
//    try {
//      try {
//        if (requiresResponse && !responded) {
//          if (requiresChunkedResponse) {
//            writeChunkedException(msg, e, false, servConn);
//          }
//          else {
//            writeException(msg, e, false, servConn);
//          }
//          servConn.setAsTrue(RESPONDED);
//        }
//      }
//      finally { // inner try-finally to ensure proper ordering of logging
//        if (potentialModification) {
//          int transId = (msg != null) ? msg.getTransactionId()
//              : Integer.MIN_VALUE;
//        }
//      }
//    }
//    catch (IOException ioe) {
//      if (logger.isDebugEnabled()) {
//        logger.fine(servConn.getName()
//            + ": Unexpected IOException writing security exception: ", ioe);
//      }
//    }
//  }

  private static void handleExceptionNoDisconnect(Message msg,
      ServerConnection servConn, Exception e) {
    boolean requiresResponse = servConn.getTransientFlag(REQUIRES_RESPONSE);
    boolean responded = servConn.getTransientFlag(RESPONDED);
    boolean requiresChunkedResponse = servConn
        .getTransientFlag(REQUIRES_CHUNKED_RESPONSE);
    boolean potentialModification = servConn.getPotentialModification();
    boolean wroteExceptionResponse = false;

    try {
      try {
        if (requiresResponse && !responded) {
          if (requiresChunkedResponse) {
            writeChunkedException(msg, e, false, servConn);
          }
          else {
            writeException(msg, e, false, servConn);
          }
          wroteExceptionResponse = true;
          servConn.setAsTrue(RESPONDED);
        }
      }
      finally { // inner try-finally to ensure proper ordering of logging
        if (potentialModification) {
          int transId = (msg != null) ? msg.getTransactionId()
              : Integer.MIN_VALUE;
          if (!wroteExceptionResponse) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION_DURING_OPERATION_ON_REGION_1_KEY_2_MESSAGEID_3,
                new Object[] {servConn.getName(),servConn.getModRegion(), servConn.getModKey(), Integer.valueOf(transId)}), e);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Exception during operation on region: {} key: {} messageId: {}", servConn.getName(),
                  servConn.getModRegion(), servConn.getModKey(), transId, e);
            }
          }
        }
        else {
          if (!wroteExceptionResponse) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION,
                servConn.getName()), e);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Exception: {}", servConn.getName(), e.getMessage(), e);
            }
          }
        }
      }
    }
    catch (IOException ioe) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Unexpected IOException writing exception: {}", servConn.getName(), ioe.getMessage(), ioe);
      }
    }
  }

  private static void handleThrowable(Message msg, ServerConnection servConn,
      Throwable th) {
    boolean requiresResponse = servConn.getTransientFlag(REQUIRES_RESPONSE);
    boolean responded = servConn.getTransientFlag(RESPONDED);
    boolean requiresChunkedResponse = servConn
        .getTransientFlag(REQUIRES_CHUNKED_RESPONSE);
    boolean potentialModification = servConn.getPotentialModification();

    try {
      try {
        if (th instanceof Error) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.BaseCommand_0_UNEXPECTED_ERROR_ON_SERVER,
              servConn.getName()), th);
        }
        if (requiresResponse && !responded) {
          if (requiresChunkedResponse) {
            writeChunkedException(msg, th, false, servConn);
          }
          else {
            writeException(msg, th, false, servConn);
          }
          servConn.setAsTrue(RESPONDED);
        }
      }
      finally { // inner try-finally to ensure proper ordering of logging
        if (th instanceof Error) {
          // log nothing
        } else if (th instanceof CancelException) {
          // log nothing
        } else {
          if (potentialModification) {
            int transId = (msg != null) ? msg.getTransactionId()
                : Integer.MIN_VALUE;
            logger.warn(LocalizedMessage.create(
              LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION_DURING_OPERATION_ON_REGION_1_KEY_2_MESSAGEID_3,
              new Object[] {servConn.getName(),servConn.getModRegion(), servConn.getModKey(), Integer.valueOf(transId)}), th);
          }
          else {
            logger.warn(LocalizedMessage.create(
              LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION,
              servConn.getName()), th);
          }
        }
      }
    } catch (IOException ioe) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Unexpected IOException writing exception: {}", servConn.getName(), ioe.getMessage(), ioe);
      }
    } finally {
      servConn.setFlagProcessMessagesAsFalse();
    }
  }
  
  protected static void writeChunkedException(Message origMsg, Throwable e,
      boolean isSevere, ServerConnection servConn) throws IOException {
    writeChunkedException(origMsg, e, isSevere, servConn, servConn.getChunkedResponseMessage());
  }

  protected static void writeChunkedException(Message origMsg, Throwable e,
      boolean isSevere, ServerConnection servConn, ChunkedMessage originalReponse) throws IOException {
    writeChunkedException(origMsg, e, isSevere, servConn, originalReponse, 2);
  }

  protected static void writeChunkedException(Message origMsg, Throwable exception,
      boolean isSevere, ServerConnection servConn, ChunkedMessage originalReponse, int numOfParts) throws IOException {
    Throwable e = getClientException(servConn, exception);
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setServerConnection(servConn);
    if (originalReponse.headerHasBeenSent()) {
      //chunkedResponseMsg = originalReponse;
      // fix for bug 35442
      chunkedResponseMsg.setNumberOfParts(numOfParts);
      chunkedResponseMsg.setLastChunkAndNumParts(true, numOfParts);
      chunkedResponseMsg.addObjPart(e); 
      if (numOfParts == 2) {
        chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: {}", servConn.getName(), e.getMessage(), e);
      }
    }
    else {
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
        logger.debug("{}: Sending exception chunk: {}", servConn.getName(), e.getMessage(), e);
      }
    }
    chunkedResponseMsg.sendChunk(servConn);
  }

  // Get the exception stacktrace for native clients
  public static String getExceptionTrace(Throwable ex) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    pw.close();
    return sw.toString();
  }

  protected static void writeException(Message origMsg, Throwable e,
      boolean isSevere, ServerConnection servConn) throws IOException {
    writeException(origMsg, MessageType.EXCEPTION, e, isSevere, servConn);
  }
  
  private static Throwable getClientException(ServerConnection servConn, Throwable e) {
    if (cache instanceof InternalCache) {
      InternalCache cache = (InternalCache) servConn.getCache();
      OldClientSupportService svc = cache.getService(OldClientSupportService.class);
      if (svc != null) {
        return svc.getThrowable(e, servConn.getClientVersion());
      }
    }
    return e;
  }

  protected static void writeException(Message origMsg, int msgType, Throwable e,
      boolean isSevere, ServerConnection servConn) throws IOException {
    Throwable theException = getClientException(servConn, e);
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(msgType);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    if (isSevere) {
      String msg = theException.getMessage();
      if (msg == null) {
        msg = theException.toString();
      }
      logger.fatal(LocalizedMessage.create(LocalizedStrings.BaseCommand_SEVERE_CACHE_EXCEPTION_0, msg));
    }
    errorMsg.addObjPart(theException);
    errorMsg.addStringPart(getExceptionTrace(theException));
    errorMsg.send(servConn);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Wrote exception: {}", servConn.getName(), e.getMessage(), e);
    }
    if (e instanceof MessageTooLargeException) {
      throw (IOException)e;
    }
  }

  protected static void writeErrorResponse(Message origMsg, int messageType,
      ServerConnection servConn) throws IOException {
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(messageType);
    errorMsg.setNumberOfParts(1);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    errorMsg
        .addStringPart(LocalizedStrings.BaseCommand_INVALID_DATA_RECEIVED_PLEASE_SEE_THE_CACHE_SERVER_LOG_FILE_FOR_ADDITIONAL_DETAILS.toLocalizedString());
    errorMsg.send(servConn);
  }

  protected static void writeErrorResponse(Message origMsg, int messageType,
      String msg, ServerConnection servConn) throws IOException {
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(messageType);
    errorMsg.setNumberOfParts(1);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    errorMsg.addStringPart(msg);
    errorMsg.send(servConn);
  }

  protected static void writeRegionDestroyedEx(Message msg, String regionName,
      String title, ServerConnection servConn) throws IOException {
    String reason = servConn.getName() + ": Region named " + regionName + title;
    RegionDestroyedException ex = new RegionDestroyedException(reason,
        regionName);
    if (servConn.getTransientFlag(REQUIRES_CHUNKED_RESPONSE)) {
      writeChunkedException(msg, ex, false, servConn);
    }
    else {
      writeException(msg, ex, false, servConn);
    }
  }

  protected static void writeResponse(Object data, Object callbackArg,
      Message origMsg, boolean isObject, ServerConnection servConn)
      throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    
    if (callbackArg == null) {
      responseMsg.setNumberOfParts(1);
    }
    else {
      responseMsg.setNumberOfParts(2);
    }
    if (data instanceof byte[]) {
      responseMsg.addRawPart((byte[])data, isObject);
    }
    else {
      Assert.assertTrue(isObject,
          "isObject should be true when value is not a byte[]");
      responseMsg.addObjPart(data, zipValues);
    }
    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.clearParts();
  }
  
  protected static void writeResponseWithRefreshMetadata(Object data,
      Object callbackArg, Message origMsg, boolean isObject,
      ServerConnection servConn, PartitionedRegion pr, byte nwHop) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    if (callbackArg == null) {
      responseMsg.setNumberOfParts(2);
    }
    else {
      responseMsg.setNumberOfParts(3);
    }

    if (data instanceof byte[]) {
      responseMsg.addRawPart((byte[])data, isObject);
    }
    else {
      Assert.assertTrue(isObject,
          "isObject should be true when value is not a byte[]");
      responseMsg.addObjPart(data, zipValues);
    }
    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    responseMsg.addBytesPart(new byte[]{pr.getMetadataVersion(),nwHop});
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.clearParts();
  }

  protected static void writeResponseWithFunctionAttribute(byte[] data,
      Message origMsg, ServerConnection servConn) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.setNumberOfParts(1);
    responseMsg.addBytesPart(data);
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.clearParts();
  }
  
  static protected void checkForInterrupt(ServerConnection servConn, Exception e) 
      throws InterruptedException, InterruptedIOException {
    servConn.getCachedRegionHelper().checkCancelInProgress(e);
    if (e instanceof InterruptedException) {
      throw (InterruptedException)e;
    }
    if (e instanceof InterruptedIOException) {
      throw (InterruptedIOException)e;
    }
  }

  protected static void writeQueryResponseChunk(Object queryResponseChunk,
      CollectionType collectionType, boolean lastChunk,
      ServerConnection servConn) throws IOException {
    ChunkedMessage queryResponseMsg = servConn.getQueryResponseMessage();
    queryResponseMsg.setNumberOfParts(2);
    queryResponseMsg.setLastChunk(lastChunk);
    queryResponseMsg.addObjPart(collectionType, zipValues);
    queryResponseMsg.addObjPart(queryResponseChunk, zipValues);
    queryResponseMsg.sendChunk(servConn);
  }

  protected static void writeQueryResponseException(Message origMsg,
      Throwable exception, boolean isSevere, ServerConnection servConn)
      throws IOException {
    Throwable e = getClientException(servConn, exception);
    ChunkedMessage queryResponseMsg = servConn.getQueryResponseMessage();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    if (queryResponseMsg.headerHasBeenSent()) {
      // fix for bug 35442
      // This client is expecting 2 parts in this message so send 2 parts
      queryResponseMsg.setServerConnection(servConn);
      queryResponseMsg.setNumberOfParts(2);
      queryResponseMsg.setLastChunkAndNumParts(true, 2);
      queryResponseMsg.addObjPart(e);
      queryResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: {}", servConn.getName(), e.getMessage(), e);
      }
      queryResponseMsg.sendChunk(servConn);
    }
    else {
      chunkedResponseMsg.setServerConnection(servConn);
      chunkedResponseMsg.setMessageType(MessageType.EXCEPTION);
      chunkedResponseMsg.setNumberOfParts(2);
      chunkedResponseMsg.setLastChunkAndNumParts(true, 2);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.addObjPart(e);
      chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk: {}", servConn.getName(), e.getMessage(), e);
      }
      chunkedResponseMsg.sendChunk(servConn);
    }
  }

  protected static void writeChunkedErrorResponse(Message origMsg,
      int messageType, String message, ServerConnection servConn)
      throws IOException {
    // Send chunked response header identifying error message
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    if (logger.isDebugEnabled()) {
      logger.debug(servConn.getName() + ": Sending error message header type: "
          + messageType + " transaction: " + origMsg.getTransactionId());
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
  
  protected static void writeFunctionResponseException(Message origMsg,
      int messageType, String message, ServerConnection servConn, Throwable exception)
      throws IOException {
    Throwable e = getClientException(servConn, exception);
    ChunkedMessage functionResponseMsg = servConn.getFunctionResponseMessage();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    if (functionResponseMsg.headerHasBeenSent()) {
      functionResponseMsg.setServerConnection(servConn);
      functionResponseMsg.setNumberOfParts(2);
      functionResponseMsg.setLastChunkAndNumParts(true,2);
      functionResponseMsg.addObjPart(e);
      functionResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: {}", servConn.getName(), e.getMessage(), e);
      }
      functionResponseMsg.sendChunk(servConn);
    }
    else {
      chunkedResponseMsg.setServerConnection(servConn);
      chunkedResponseMsg.setMessageType(messageType);
      chunkedResponseMsg.setNumberOfParts(2);
      chunkedResponseMsg.setLastChunkAndNumParts(true,2);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      chunkedResponseMsg.addObjPart(e);
      chunkedResponseMsg.addStringPart(getExceptionTrace(e));
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk: {}", servConn.getName(), e.getMessage(), e);
      }
      chunkedResponseMsg.sendChunk(servConn);
    }
  }
  
  protected static void writeFunctionResponseError(Message origMsg,
      int messageType, String message, ServerConnection servConn)
      throws IOException {
    ChunkedMessage functionResponseMsg = servConn.getFunctionResponseMessage();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    if (functionResponseMsg.headerHasBeenSent()) {
      functionResponseMsg.setNumberOfParts(1);
      functionResponseMsg.setLastChunk(true);
      functionResponseMsg.addStringPart(message);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending Error chunk while reply in progress: {}", servConn.getName(), message);
      }
      functionResponseMsg.sendChunk(servConn);
    }
    else {
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

  protected static void writeKeySetErrorResponse(Message origMsg,
      int messageType, String message, ServerConnection servConn)
      throws IOException {
    // Send chunked response header identifying error message
    ChunkedMessage chunkedResponseMsg = servConn.getKeySetResponseMessage();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending error message header type: {} transaction: {}",
          servConn.getName(), messageType, origMsg.getTransactionId());
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
      requestMsg.recv(servConn, MAX_INCOMING_DATA, incomingDataLimiter,
          incomingMsgLimiter);
      return requestMsg;
    }
    catch (EOFException eof) {
      handleEOFException(null, servConn, eof);
      // TODO:Asif: Check if there is any need for explicitly returning

    }
    catch (InterruptedIOException e) { // Solaris only
      handleInterruptedIOException(null, servConn, e);

    }
    catch (IOException e) {
      handleIOException(null, servConn, e);

    }
    catch (DistributedSystemDisconnectedException e) {
      handleShutdownException(null, servConn, e);

    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable e) {
      SystemFailure.checkFailure();
      handleThrowable(null, servConn, e);
    }
    return requestMsg;
  }

  protected static void fillAndSendRegisterInterestResponseChunks(
      LocalRegion region, Object riKey, int interestType,
      InterestResultPolicy policy, ServerConnection servConn)
      throws IOException {
    fillAndSendRegisterInterestResponseChunks(region, riKey, interestType,
        false, policy, servConn);
  }

  /*
   * serializeValues is unused for clients < GFE_80
   */
  protected static void fillAndSendRegisterInterestResponseChunks(
      LocalRegion region, Object riKey, int interestType, boolean serializeValues,
      InterestResultPolicy policy, ServerConnection servConn)
      throws IOException {
    // Client is not interested.
    if (policy.isNone()) {
      sendRegisterInterestResponseChunk(region, riKey, new ArrayList(), true,
          servConn);
      return;
    }
    if (policy.isKeysValues()
        && servConn.getClientVersion().compareTo(Version.GFE_80) >= 0) {
        handleKeysValuesPolicy(region, riKey, interestType, serializeValues, servConn);
        return;
    }
    if (riKey instanceof List) {
      handleList(region, (List)riKey, policy, servConn);
      return;
    }
    if (!(riKey instanceof String)) {
      handleSingleton(region, riKey, policy, servConn);
      return;
    }

    switch (interestType) {
    case InterestType.OQL_QUERY:
      // Not supported yet
      throw new InternalGemFireError(LocalizedStrings.BaseCommand_NOT_YET_SUPPORTED.toLocalizedString());
    case InterestType.FILTER_CLASS:
      throw new InternalGemFireError(LocalizedStrings.BaseCommand_NOT_YET_SUPPORTED.toLocalizedString());
      // handleFilter(region, (String)riKey, policy);
      // break;
    case InterestType.REGULAR_EXPRESSION: {
      String regEx = (String)riKey;
      if (regEx.equals(".*")) {
        handleAllKeys(region, policy, servConn);
      }
      else {
        handleRegEx(region, regEx, policy, servConn);
      }
    }
      break;
    case InterestType.KEY:
      if (riKey.equals("ALL_KEYS")) {
        handleAllKeys(region, policy, servConn);
      }
      else {
        handleSingleton(region, riKey, policy, servConn);
      }
      break;
    default:
      throw new InternalGemFireError(LocalizedStrings.BaseCommand_UNKNOWN_INTEREST_TYPE.toLocalizedString());
    }
  }

  @SuppressWarnings("rawtypes")
  private static void handleKeysValuesPolicy(LocalRegion region, Object riKey,
      int interestType, boolean serializeValues, ServerConnection servConn)
      throws IOException {
    if (riKey instanceof List) {
      handleKVList(region, (List)riKey, serializeValues, servConn);
      return;
    }
    if (!(riKey instanceof String)) {
      handleKVSingleton(region, riKey, serializeValues, servConn);
      return;
    }

    switch (interestType) {
    case InterestType.OQL_QUERY:
      throw new InternalGemFireError(LocalizedStrings.BaseCommand_NOT_YET_SUPPORTED.toLocalizedString());
    case InterestType.FILTER_CLASS:
      throw new InternalGemFireError(LocalizedStrings.BaseCommand_NOT_YET_SUPPORTED.toLocalizedString());
    case InterestType.REGULAR_EXPRESSION:
      String regEx = (String)riKey;
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
      throw new InternalGemFireError(LocalizedStrings.BaseCommand_UNKNOWN_INTEREST_TYPE.toLocalizedString());
    }
  }

  /**
   * @param list
   *                is a List of entry keys
   */
  protected static void sendRegisterInterestResponseChunk(Region region,
      Object riKey, ArrayList list, boolean lastChunk, ServerConnection servConn)
      throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getRegisterInterestResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);
    String regionName = (region == null) ? " null " : region.getFullPath();
    if (logger.isDebugEnabled()) {
      String str = servConn.getName() + ": Sending"
          + (lastChunk ? " last " : " ")
          + "register interest response chunk for region: " + regionName
          + " for keys: " + riKey + " chunk=<" + chunkedResponseMsg + ">";
      logger.debug(str);
    }

    chunkedResponseMsg.sendChunk(servConn);
  }
  
  /**
   * Determines whether keys for destroyed entries (tombstones) should be sent
   * to clients in register-interest results.
   * 
   * @param servConn
   * @param policy
   * @return true if tombstones should be sent to the client
   */
  private static boolean sendTombstonesInRIResults(ServerConnection servConn, InterestResultPolicy policy) {
    return (policy == InterestResultPolicy.KEYS_VALUES)
         && (servConn.getClientVersion().compareTo(Version.GFE_80) >= 0);
  }

  /**
   * Process an interest request involving a list of keys
   *
   * @param region
   *                the region
   * @param keyList
   *                the list of keys
   * @param policy
   *                the policy
   * @throws IOException
   */
  private static void handleList(LocalRegion region, List keyList,
      InterestResultPolicy policy, ServerConnection servConn)
      throws IOException {
    if (region instanceof PartitionedRegion) {
      // too bad java doesn't provide another way to do this...
      handleListPR((PartitionedRegion)region, keyList, policy, servConn);
      return;
    }
    ArrayList newKeyList = new ArrayList(maximumChunkSize);
    // Handle list of keys
    if (region != null) {
      for (Iterator it = keyList.iterator(); it.hasNext();) {
        Object entryKey = it.next();
        if (region.containsKey(entryKey)
            || (sendTombstonesInRIResults(servConn, policy) && region.containsTombstone(entryKey))) {
          
          appendInterestResponseKey(region, keyList, entryKey, newKeyList,
              "list", servConn);
        }
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, keyList, newKeyList, true,
        servConn);
  }

  /**
   * Handles both RR and PR cases
   */
  @SuppressWarnings("rawtypes")
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_PARAM_DEREF", justification="Null value handled in sendNewRegisterInterestResponseChunk()")
  private static void handleKVSingleton(LocalRegion region, Object entryKey,
      boolean serializeValues, ServerConnection servConn)
      throws IOException {
    VersionedObjectList values = new VersionedObjectList(maximumChunkSize,
        true, region == null ? true : region.getAttributes()
            .getConcurrencyChecksEnabled(), serializeValues);

    if (region != null) {
      if (region.containsKey(entryKey) || region.containsTombstone(entryKey)) {
        VersionTagHolder versionHolder = new VersionTagHolder();
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
   * @param region
   *                the region
   * @param entryKey
   *                the key
   * @param policy
   *                the policy
   * @throws IOException
   */
  private static void handleSingleton(LocalRegion region, Object entryKey,
      InterestResultPolicy policy, ServerConnection servConn)
      throws IOException {
    ArrayList keyList = new ArrayList(1);
    if (region != null) {
      if (region.containsKey(entryKey) ||
          (sendTombstonesInRIResults(servConn, policy) && region.containsTombstone(entryKey))) {
        appendInterestResponseKey(region, entryKey, entryKey, keyList,
            "individual", servConn);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, entryKey, keyList, true, servConn);
  }

  /**
   * Process an interest request of type ALL_KEYS
   *
   * @param region
   *                the region
   * @param policy
   *                the policy
   * @throws IOException
   */
  private static void handleAllKeys(LocalRegion region,
      InterestResultPolicy policy, ServerConnection servConn)
      throws IOException {
    ArrayList keyList = new ArrayList(maximumChunkSize);
    if (region != null) {
      for (Iterator it = region.keySet(sendTombstonesInRIResults(servConn, policy)).iterator(); it.hasNext();) {
        appendInterestResponseKey(region, "ALL_KEYS", it.next(), keyList,
            "ALL_KEYS", servConn);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, "ALL_KEYS", keyList, true,
        servConn);
  }

  /**
   * @param region
   * @param regex
   * @param serializeValues
   * @param servConn
   * @throws IOException
   */
  private static void handleKVAllKeys(LocalRegion region, String regex,
      boolean serializeValues, ServerConnection servConn) throws IOException {

    if (region != null && region instanceof PartitionedRegion) {
      handleKVKeysPR((PartitionedRegion) region, regex, serializeValues, servConn);
      return;
    }

    VersionedObjectList values = new VersionedObjectList(maximumChunkSize,
        true, region == null ? true : region.getAttributes()
            .getConcurrencyChecksEnabled(), serializeValues);

    if (region != null) {

      VersionTag versionTag = null;
      Object data = null;

      Pattern keyPattern = null;
      if (regex != null) {
        keyPattern = Pattern.compile(regex);
      }

      for (Object key : region.keySet(true)) {
        VersionTagHolder versionHolder = new VersionTagHolder();
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
        data = region.get(key, null, true, true, true, id, versionHolder, true);
        versionTag = versionHolder.getVersionTag();
        updateValues(values, key, data, versionTag);

        if (values.size() == maximumChunkSize) {
          sendNewRegisterInterestResponseChunk(region, regex != null ? regex : "ALL_KEYS", values, false, servConn);
          values.clear();
        }
      } // for
    } // if

    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendNewRegisterInterestResponseChunk(region, regex != null ? regex : "ALL_KEYS", values, true, servConn);
  }

  private static void handleKVKeysPR(PartitionedRegion region, Object keyInfo,
      boolean serializeValues, ServerConnection servConn) throws IOException {
    int id = 0;
    HashMap<Integer, HashSet> bucketKeys = null;

    VersionedObjectList values = new VersionedObjectList(maximumChunkSize,
        true, region.getConcurrencyChecksEnabled(), serializeValues);

    if (keyInfo != null && keyInfo instanceof List) {
      bucketKeys = new HashMap<Integer, HashSet>();
      for (Object key : (List) keyInfo) {
        id = PartitionedRegionHelper.getHashKey(region, null, key, null, null);
        if (bucketKeys.containsKey(id)) {
          bucketKeys.get(id).add(key);
        } else {
          HashSet<Object> keys = new HashSet<Object>();
          keys.add(key);
          bucketKeys.put(id, keys);
        }
      }
      region.fetchEntries(bucketKeys, values, servConn);
    } else { // keyInfo is a String
      region.fetchEntries((String)keyInfo, values, servConn);
    }

    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendNewRegisterInterestResponseChunk(region, keyInfo != null ? keyInfo : "ALL_KEYS", values, true, servConn);
  }

  /**
   * Copied from Get70.getValueAndIsObject(), except a minor change. (Make the
   * method static instead of copying it here?)
   * 
   * @param value
   */
  private static void updateValues(VersionedObjectList values, Object key, Object value, VersionTag versionTag) {
    boolean isObject = true;

    // If the value in the VM is a CachedDeserializable,
    // get its value. If it is Token.REMOVED, Token.DESTROYED,
    // Token.INVALID, or Token.LOCAL_INVALID
    // set it to null. If it is NOT_AVAILABLE, get the value from
    // disk. If it is already a byte[], set isObject to false.
    boolean wasInvalid = false;
    if (value instanceof CachedDeserializable) {
      value = ((CachedDeserializable)value).getValue();
    }
    else if (value == Token.REMOVED_PHASE1 || value == Token.REMOVED_PHASE2 || value == Token.DESTROYED || value == Token.TOMBSTONE) {
      value = null;
    }
    else if (value == Token.INVALID || value == Token.LOCAL_INVALID) {
      value = null; // fix for bug 35884
      wasInvalid = true;
    }
    else if (value instanceof byte[]) {
      isObject = false;
    }
    boolean keyNotPresent = !wasInvalid && (value == null || value == Token.TOMBSTONE);

    if (keyNotPresent) {
      values.addObjectPartForAbsentKey(key, value, versionTag);
    } else {
      values.addObjectPart(key, value, isObject, versionTag);
    }
  }

  public static void appendNewRegisterInterestResponseChunkFromLocal(LocalRegion region,
      VersionedObjectList values, Object riKeys, Set keySet, ServerConnection servConn)
      throws IOException {
    Object key = null;
    VersionTagHolder versionHolder = null;
    ClientProxyMembershipID requestingClient = servConn == null ? null : servConn.getProxyID();
    for (Iterator it = keySet.iterator(); it.hasNext();) {
      key = it.next();
      versionHolder = new VersionTagHolder();

      Object value = region.get(key, null, true, true, true, requestingClient, versionHolder, true);
      
      updateValues(values, key, value, versionHolder.getVersionTag());

      if (values.size() == maximumChunkSize) {
        // Send the chunk and clear the list
        // values.setKeys(null); // Now we need to send keys too.
        sendNewRegisterInterestResponseChunk(region, riKeys != null ? riKeys : "ALL_KEYS", values, false, servConn);
        values.clear();
      }
    } // for
  }

  /**
   * 
   * @param region
   * @param values {@link VersionedObjectList}
   * @param riKeys
   * @param set set of entries
   * @param servConn
   * @throws IOException
   */
  public static void appendNewRegisterInterestResponseChunk(LocalRegion region,
      VersionedObjectList values, Object riKeys, Set set, ServerConnection servConn)
      throws IOException {
    for (Iterator<Map.Entry> it = set.iterator(); it.hasNext();) {
      Map.Entry entry = it.next(); // Region.Entry or Map.Entry
      if (entry instanceof Region.Entry) { // local entries
        VersionTag vt = null;
        Object key = null;
        Object value = null;
        if (entry instanceof EntrySnapshot) {
          vt = ((EntrySnapshot) entry).getVersionTag();
          key = ((EntrySnapshot) entry).getRegionEntry().getKey();
          value = ((EntrySnapshot) entry).getRegionEntry().getValue(null);
          updateValues(values, key, value, vt);
        } else {
          VersionStamp vs = ((NonTXEntry)entry).getRegionEntry().getVersionStamp();
          vt = vs == null ? null : vs.asVersionTag();
          key = entry.getKey();
          value = ((NonTXEntry)entry).getRegionEntry()._getValueRetain(region, true);
          try {
            updateValues(values, key, value, vt);
          } finally {
            OffHeapHelper.release(value);
          }
        }
      } else { // Map.Entry (remote entries)
        ArrayList list = (ArrayList)entry.getValue();
        Object value = list.get(0);
        VersionTag tag = (VersionTag)list.get(1);
        updateValues(values, entry.getKey(), value, tag);
      }
      if (values.size() == maximumChunkSize) {
        // Send the chunk and clear the list
        // values.setKeys(null); // Now we need to send keys too.
        sendNewRegisterInterestResponseChunk(region, riKeys != null ? riKeys : "ALL_KEYS", values, false, servConn);
        values.clear();
      }
    } // for
  }

  public static void sendNewRegisterInterestResponseChunk(LocalRegion region,
      Object riKey, VersionedObjectList list, boolean lastChunk, ServerConnection servConn)
      throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getRegisterInterestResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);
    String regionName = (region == null) ? " null " : region.getFullPath();
    if (logger.isDebugEnabled()) {
      String str = servConn.getName() + ": Sending"
          + (lastChunk ? " last " : " ")
          + "register interest response chunk for region: " + regionName
          + " for keys: " + riKey + " chunk=<" + chunkedResponseMsg + ">";
      logger.debug(str);
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

  /**
   * Process an interest request of type {@link InterestType#REGULAR_EXPRESSION}
   *
   * @param region
   *                the region
   * @param regex
   *                the regex
   * @param policy
   *                the policy
   * @throws IOException
   */
  private static void handleRegEx(LocalRegion region, String regex,
      InterestResultPolicy policy, ServerConnection servConn)
      throws IOException {
    if (region instanceof PartitionedRegion) {
      // too bad java doesn't provide another way to do this...
      handleRegExPR((PartitionedRegion)region, regex, policy, servConn);
      return;
    }
    ArrayList keyList = new ArrayList(maximumChunkSize);
    // Handle the regex pattern
    Pattern keyPattern = Pattern.compile(regex);
    if (region != null) {
      for (Iterator it = region.keySet(sendTombstonesInRIResults(servConn, policy)).iterator(); it.hasNext();) {
        Object entryKey = it.next();
        if (!(entryKey instanceof String)) {
          // key is not a String, cannot apply regex to this entry
          continue;
        }
        if (!keyPattern.matcher((String)entryKey).matches()) {
          // key does not match the regex, this entry should not be returned.
          continue;
        }

        appendInterestResponseKey(region, regex, entryKey, keyList, "regex",
            servConn);
      }
    }
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, regex, keyList, true, servConn);
  }

  /**
   * Process an interest request of type {@link InterestType#REGULAR_EXPRESSION}
   *
   * @param region
   *                the region
   * @param regex
   *                the regex
   * @param policy
   *                the policy
   * @throws IOException
   */
  private static void handleRegExPR(final PartitionedRegion region,
      final String regex, final InterestResultPolicy policy,
      final ServerConnection servConn) throws IOException {
    final ArrayList keyList = new ArrayList(maximumChunkSize);
    region.getKeysWithRegEx(regex, sendTombstonesInRIResults(servConn, policy), new PartitionedRegion.SetCollector() {
      public void receiveSet(Set theSet) throws IOException {
        appendInterestResponseKeys(region, regex, theSet, keyList, "regex",
            servConn);
      }
    });
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, regex, keyList, true, servConn);
  }

  /**
   * Process an interest request involving a list of keys
   *
   * @param region
   *                the region
   * @param keyList
   *                the list of keys
   * @param policy
   *                the policy
   * @throws IOException
   */
  private static void handleListPR(final PartitionedRegion region,
      final List keyList, final InterestResultPolicy policy,
      final ServerConnection servConn) throws IOException {
    final ArrayList newKeyList = new ArrayList(maximumChunkSize);
    region.getKeysWithList(keyList, sendTombstonesInRIResults(servConn, policy), new PartitionedRegion.SetCollector() {
      public void receiveSet(Set theSet) throws IOException {
        appendInterestResponseKeys(region, keyList, theSet, newKeyList, "list",
            servConn);
      }
    });
    // Send the last chunk (the only chunk for individual and list keys)
    // always send it back, even if the list is of zero size.
    sendRegisterInterestResponseChunk(region, keyList, newKeyList, true,
        servConn);
  }

  @SuppressWarnings("rawtypes")
  private static void handleKVList(final LocalRegion region,
      final List keyList, boolean serializeValues,
      final ServerConnection servConn) throws IOException {

    if (region != null && region instanceof PartitionedRegion) {
      handleKVKeysPR((PartitionedRegion)region, keyList, serializeValues, servConn);
      return;
    }
    VersionedObjectList values = new VersionedObjectList(maximumChunkSize,
        true, region == null ? true : region.getAttributes()
            .getConcurrencyChecksEnabled(), serializeValues);

    // Handle list of keys
    if (region != null) {
      VersionTag versionTag = null;
      Object data = null;

      for (Iterator it = keyList.iterator(); it.hasNext();) {
        Object key = it.next();
        if (region.containsKey(key) || region.containsTombstone(key)) {
          VersionTagHolder versionHolder = new VersionTagHolder();

          ClientProxyMembershipID id = servConn == null ? null : servConn
              .getProxyID();
          data = region.get(key, null, true, true, true, id, versionHolder,
              true);
          versionTag = versionHolder.getVersionTag();
          updateValues(values, key, data, versionTag);

          if (values.size() == maximumChunkSize) {
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

  /**
   * Append an interest response
   *
   * @param region
   *                the region (for debugging)
   * @param riKey
   *                the registerInterest "key" (what the client is interested
   *                in)
   * @param entryKey
   *                key we're responding to
   * @param list
   *                list to append to
   * @param kind
   *                for debugging
   */
  private static void appendInterestResponseKey(LocalRegion region,
      Object riKey, Object entryKey, ArrayList list, String kind,
      ServerConnection servConn) throws IOException {
    list.add(entryKey);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: appendInterestResponseKey <{}>; list size was {}; region: {}",
          servConn.getName(), entryKey, list.size(), region.getFullPath());
    }
    if (list.size() == maximumChunkSize) {
      // Send the chunk and clear the list
      sendRegisterInterestResponseChunk(region, riKey, list, false, servConn);
      list.clear();
    }
  }

  protected static void appendInterestResponseKeys(LocalRegion region,
      Object riKey, Collection entryKeys, ArrayList collector, String riDescr,
      ServerConnection servConn) throws IOException {
    for (Iterator it = entryKeys.iterator(); it.hasNext();) {
      appendInterestResponseKey(region, riKey, it.next(), collector, riDescr,
          servConn);
    }
  }
}
